//! One collaborative session as the relay sees it: the authoritative document, the revision
//! counter, a short log of accepted operations and the people present.
//!
//! This is the *sequencer*. Operations arrive in some order; each is accepted or refused whole:
//!
//! - it is accepted when the active lock policy permits the change and no key it touches was
//!   changed after the revision it was based on (`base_rev`);
//! - otherwise it is refused and its author restores or merges the newer authoritative values.
//!   Nothing a client sends can overwrite a newer change unseen or bypass a resource lock.
//!
//! Deleted keys keep their last revision (a tombstone) so a late write to one is still caught.
//! The `Room` is plain data with no I/O, so the whole ordering policy is unit-tested here.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde_json::Value;

use crate::proto::{
    AppliedOp, Changes, Doc, Key, MAX_DOC_KEYS, MAX_KEYS_PER_OP, MAX_OBJECT_LOCKS_PER_USER,
    MAX_USERS, ObjectLock, UserInfo, clean_name, color_for, object_lock_ids_for_changes,
    secrets_equal, valid_key, valid_object_lock_id,
};

/// Accepted operations kept for clients that reconnect.
pub const LOG_LIMIT: usize = 4096;

/// Why an operation was refused.
#[derive(Clone, Debug, PartialEq)]
pub enum Reject {
    /// Somebody changed these keys after the operation's base revision.
    Stale(Vec<Key>),
    /// The operation itself is not acceptable.
    Invalid(String),
    /// Timeline-wide locking is on and this client does not own the timeline.
    TimelineLocked(Option<u64>),
    /// These objects are not reserved by the author.
    Locked(Vec<String>),
}

/// Why a lock request could not be granted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LockError {
    pub resource: String,
    pub owner: Option<u64>,
    pub message: String,
}

pub struct Room {
    token: Option<String>,
    rev: u64,
    state: Doc,
    key_rev: BTreeMap<Key, u64>,
    log: VecDeque<AppliedOp>,
    users: BTreeMap<u64, UserInfo>,
    next_client: u64,
    host_id: Option<u64>,
    timeline_locking: bool,
    timeline_lock_owner: Option<u64>,
    object_locks: BTreeMap<String, u64>,
}

impl Room {
    /// A session that starts from `state`, which is revision 1.
    pub fn new(state: Doc, token: Option<String>) -> Self {
        let key_rev = state.keys().map(|k| (k.clone(), 1)).collect();
        Self {
            token,
            rev: 1,
            state,
            key_rev,
            log: VecDeque::new(),
            users: BTreeMap::new(),
            next_client: 1,
            host_id: None,
            timeline_locking: true,
            timeline_lock_owner: None,
            object_locks: BTreeMap::new(),
        }
    }

    /// Check a state offered by a client that wants to start a session.
    pub fn validate_initial(state: &Doc) -> Result<(), String> {
        if state.len() > MAX_DOC_KEYS {
            return Err(format!(
                "the project has {} parts, more than the {MAX_DOC_KEYS} allowed",
                state.len()
            ));
        }
        if let Some(bad) = state.keys().find(|k| !valid_key(k)) {
            return Err(format!("invalid key {bad:?}"));
        }
        Ok(())
    }

    pub fn rev(&self) -> u64 {
        self.rev
    }
    pub fn state(&self) -> &Doc {
        &self.state
    }
    pub fn users(&self) -> Vec<UserInfo> {
        self.users.values().cloned().collect()
    }

    pub fn lock_state(&self) -> (Option<u64>, bool, Option<u64>, Vec<ObjectLock>) {
        let locks = self
            .object_locks
            .iter()
            .map(|(object_id, client_id)| ObjectLock {
                object_id: object_id.clone(),
                client_id: *client_id,
            })
            .collect();
        (
            self.host_id,
            self.timeline_locking,
            self.timeline_lock_owner,
            locks,
        )
    }

    fn known_user(&self, client_id: u64) -> bool {
        self.users.contains_key(&client_id)
    }

    /// Change policy. The first participant is host; when the host leaves, the oldest remaining
    /// participant becomes host. Switching modes clears reservations from the old mode.
    pub fn set_timeline_locking(&mut self, client_id: u64, enabled: bool) -> Result<(), LockError> {
        if self.host_id != Some(client_id) {
            return Err(LockError {
                resource: "timeline-mode".into(),
                owner: self.host_id,
                message: "only the session host can change the locking mode".into(),
            });
        }
        if self.timeline_locking != enabled {
            self.timeline_locking = enabled;
            self.timeline_lock_owner = None;
            self.object_locks.clear();
        }
        Ok(())
    }

    /// First request processed by the relay receives the timeline lock.
    pub fn acquire_timeline_lock(&mut self, client_id: u64) -> Result<(), LockError> {
        if !self.known_user(client_id) {
            return Err(LockError {
                resource: "timeline".into(),
                owner: None,
                message: "you are not in this session".into(),
            });
        }
        if !self.timeline_locking {
            return Err(LockError {
                resource: "timeline".into(),
                owner: None,
                message: "timeline locking is disabled".into(),
            });
        }
        match self.timeline_lock_owner {
            None => {
                self.timeline_lock_owner = Some(client_id);
                Ok(())
            }
            Some(owner) if owner == client_id => Ok(()),
            Some(owner) => Err(LockError {
                resource: "timeline".into(),
                owner: Some(owner),
                message: "the timeline is already locked by another collaborator".into(),
            }),
        }
    }

    pub fn release_timeline_lock(&mut self, client_id: u64) {
        if self.timeline_lock_owner == Some(client_id) {
            self.timeline_lock_owner = None;
        }
    }

    /// Replace a client's object reservations. Reservations no longer requested are released
    /// even when a newly requested object belongs to another collaborator; existing reservations
    /// that remain selected are kept. New reservations are granted only when all are available.
    pub fn set_object_locks(
        &mut self,
        client_id: u64,
        object_ids: Vec<String>,
    ) -> Result<(), LockError> {
        if !self.known_user(client_id) {
            return Err(LockError {
                resource: "objects".into(),
                owner: None,
                message: "you are not in this session".into(),
            });
        }
        if self.timeline_locking {
            return Err(LockError {
                resource: "objects".into(),
                owner: None,
                message: "object locking is disabled while timeline locking is on".into(),
            });
        }
        if object_ids.len() > MAX_OBJECT_LOCKS_PER_USER {
            return Err(LockError {
                resource: "objects".into(),
                owner: None,
                message: format!(
                    "at most {MAX_OBJECT_LOCKS_PER_USER} objects can be reserved at once"
                ),
            });
        }
        let mut requested = BTreeSet::new();
        for object_id in object_ids {
            if !valid_object_lock_id(&object_id) {
                return Err(LockError {
                    resource: object_id,
                    owner: None,
                    message: "invalid object lock identifier".into(),
                });
            }
            requested.insert(object_id);
        }
        // Drop locks for objects that are no longer selected before attempting new reservations.
        self.object_locks
            .retain(|id, owner| *owner != client_id || requested.contains(id));
        for object_id in &requested {
            if let Some(owner) = self.object_locks.get(object_id).copied()
                && owner != client_id
            {
                return Err(LockError {
                    resource: object_id.clone(),
                    owner: Some(owner),
                    message: "this object is being edited by another collaborator".into(),
                });
            }
        }
        for object_id in requested {
            self.object_locks.insert(object_id, client_id);
        }
        Ok(())
    }

    /// Validate a write against the active lock mode before it reaches the authoritative document.
    pub fn apply_locked(
        &mut self,
        client_id: u64,
        op_id: u64,
        base_rev: u64,
        changes: Changes,
    ) -> Result<AppliedOp, Reject> {
        // A stale full-track value may not contain objects added since `base_rev`. Report the
        // stale key first so the client can merge against the authoritative value; otherwise the
        // lock check would mistake another collaborator's new object for this client's deletion.
        self.validate_operation(base_rev, &changes)?;

        let touches_timeline = changes
            .keys()
            .any(|key| key.starts_with("track/") || key.starts_with("seq/"));
        if self.timeline_locking && touches_timeline && self.timeline_lock_owner != Some(client_id)
        {
            return Err(Reject::TimelineLocked(self.timeline_lock_owner));
        }
        if !self.timeline_locking {
            let missing: Vec<String> = object_lock_ids_for_changes(&self.state, &changes)
                .into_iter()
                .filter(|id| self.object_locks.get(id) != Some(&client_id))
                .collect();
            if !missing.is_empty() {
                return Err(Reject::Locked(missing));
            }
        }
        Ok(self.commit(client_id, op_id, changes))
    }

    /// Whether a client presenting `offered` may enter.
    pub fn token_ok(&self, offered: Option<&str>) -> bool {
        match (&self.token, offered) {
            (None, _) => true,
            (Some(want), Some(got)) => secrets_equal(want, got),
            (Some(_), None) => false,
        }
    }

    /// Admit a user: a new client id and colour, or `None` when the session is full.
    pub fn add_user(&mut self, name: &str) -> Option<UserInfo> {
        if self.users.len() >= MAX_USERS {
            return None;
        }
        let client_id = self.next_client;
        self.next_client += 1;
        let user = UserInfo {
            client_id,
            name: clean_name(name),
            color: color_for(client_id),
            presence: Value::Null,
        };
        if self.host_id.is_none() {
            self.host_id = Some(client_id);
        }
        self.users.insert(client_id, user.clone());
        Some(user)
    }

    pub fn remove_user(&mut self, client_id: u64) -> bool {
        let removed = self.users.remove(&client_id).is_some();
        if removed {
            self.release_timeline_lock(client_id);
            self.object_locks.retain(|_, owner| *owner != client_id);
            if self.host_id == Some(client_id) {
                self.host_id = self.users.keys().next().copied();
            }
        }
        removed
    }

    pub fn set_presence(&mut self, client_id: u64, data: Value) -> bool {
        match self.users.get_mut(&client_id) {
            Some(u) => {
                u.presence = data;
                true
            }
            None => false,
        }
    }

    fn validate_operation(&self, base_rev: u64, changes: &Changes) -> Result<(), Reject> {
        if changes.is_empty() {
            return Err(Reject::Invalid("empty operation".into()));
        }
        if changes.len() > MAX_KEYS_PER_OP {
            return Err(Reject::Invalid(format!(
                "operation touches {} keys, more than {MAX_KEYS_PER_OP}",
                changes.len()
            )));
        }
        if base_rev > self.rev {
            return Err(Reject::Invalid(format!(
                "operation is based on revision {base_rev}, which does not exist yet"
            )));
        }
        for (key, value) in changes {
            if !valid_key(key) {
                return Err(Reject::Invalid(format!("invalid key {key:?}")));
            }
            if matches!(value, Some(Value::Null)) {
                return Err(Reject::Invalid(format!("key {key:?} holds null")));
            }
        }
        let stale: Vec<Key> = changes
            .keys()
            .filter(|k| self.key_rev.get(*k).is_some_and(|r| *r > base_rev))
            .cloned()
            .collect();
        if !stale.is_empty() {
            return Err(Reject::Stale(stale));
        }
        let adds = changes
            .iter()
            .filter(|(k, v)| v.is_some() && !self.state.contains_key(*k))
            .count();
        if self.state.len() + adds > MAX_DOC_KEYS {
            return Err(Reject::Invalid(format!(
                "the project would exceed {MAX_DOC_KEYS} parts"
            )));
        }
        Ok(())
    }

    fn commit(&mut self, client_id: u64, op_id: u64, changes: Changes) -> AppliedOp {
        self.rev += 1;
        for (key, value) in &changes {
            match value {
                Some(v) => {
                    self.state.insert(key.clone(), v.clone());
                }
                None => {
                    self.state.remove(key);
                }
            }
            self.key_rev.insert(key.clone(), self.rev);
        }
        let applied = AppliedOp {
            rev: self.rev,
            client_id,
            op_id,
            changes,
        };
        self.log.push_back(applied.clone());
        while self.log.len() > LOG_LIMIT {
            self.log.pop_front();
        }
        applied
    }

    /// Decide on an operation. On success the document and revision have moved on and the returned
    /// [`AppliedOp`] is what every client must receive, in this order.
    pub fn apply(
        &mut self,
        client_id: u64,
        op_id: u64,
        base_rev: u64,
        changes: Changes,
    ) -> Result<AppliedOp, Reject> {
        self.validate_operation(base_rev, &changes)?;
        Ok(self.commit(client_id, op_id, changes))
    }

    /// The operations after revision `since`, if the log still reaches back that far.
    pub fn catchup(&self, since: u64) -> Option<Vec<AppliedOp>> {
        if since > self.rev {
            return None;
        }
        if since == self.rev {
            return Some(Vec::new());
        }
        let first = self.log.front()?.rev;
        if first > since + 1 {
            return None;
        }
        Some(
            self.log
                .iter()
                .filter(|op| op.rev > since)
                .cloned()
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{Reject, Room};
    use crate::proto::{Changes, Doc};

    fn room() -> Room {
        let mut doc = Doc::new();
        doc.insert("a".into(), json!({"v": 1}));
        doc.insert("b".into(), json!({"v": 1}));
        Room::new(doc, None)
    }

    fn set(key: &str, v: i64) -> Changes {
        [(key.to_string(), Some(json!({"v": v})))].into()
    }

    #[test]
    fn accepted_operations_get_consecutive_revisions() {
        let mut r = room();
        assert_eq!(r.apply(1, 1, 1, set("a", 2)).unwrap().rev, 2);
        assert_eq!(r.apply(2, 1, 2, set("a", 3)).unwrap().rev, 3);
        assert_eq!(r.state().get("a"), Some(&json!({"v": 3})));
    }

    #[test]
    fn a_write_based_on_an_old_revision_to_a_changed_key_is_refused() {
        let mut r = room();
        r.apply(1, 1, 1, set("a", 2)).unwrap();
        let err = r.apply(2, 1, 1, set("a", 9)).unwrap_err();
        assert_eq!(err, Reject::Stale(vec!["a".to_string()]));
        // nothing moved
        assert_eq!(r.rev(), 2);
        assert_eq!(r.state().get("a"), Some(&json!({"v": 2})));
    }

    #[test]
    fn different_keys_from_the_same_base_both_succeed() {
        let mut r = room();
        r.apply(1, 1, 1, set("a", 2)).unwrap();
        assert!(r.apply(2, 1, 1, set("b", 5)).is_ok());
    }

    #[test]
    fn one_stale_key_refuses_the_whole_operation() {
        let mut r = room();
        r.apply(1, 1, 1, set("a", 2)).unwrap();
        let mut both = set("a", 7);
        both.extend(set("b", 7));
        assert!(matches!(r.apply(2, 1, 1, both), Err(Reject::Stale(_))));
        assert_eq!(r.state().get("b"), Some(&json!({"v": 1})));
    }

    #[test]
    fn deleted_keys_are_still_protected() {
        let mut r = room();
        r.apply(1, 1, 1, [("a".to_string(), None)].into()).unwrap();
        assert!(r.state().get("a").is_none());
        assert!(matches!(
            r.apply(2, 1, 1, set("a", 4)),
            Err(Reject::Stale(_))
        ));
        assert!(r.apply(2, 2, 2, set("a", 4)).is_ok());
    }

    #[test]
    fn hostile_operations_are_invalid() {
        let mut r = room();
        assert!(matches!(
            r.apply(1, 1, 1, Changes::new()),
            Err(Reject::Invalid(_))
        ));
        assert!(matches!(
            r.apply(1, 1, 99, set("a", 1)),
            Err(Reject::Invalid(_))
        ));
        assert!(matches!(
            r.apply(1, 1, 1, set("bad\nkey", 1)),
            Err(Reject::Invalid(_))
        ));
        assert!(matches!(
            r.apply(
                1,
                1,
                1,
                [("a".to_string(), Some(serde_json::Value::Null))].into()
            ),
            Err(Reject::Invalid(_))
        ));
        assert_eq!(r.rev(), 1);
    }

    #[test]
    fn timeline_lock_is_first_come_first_served_and_guards_timeline_writes() {
        let mut doc = Doc::new();
        doc.insert(
            "track/1/v/2".into(),
            json!({"items": [{"id": 10, "start": 0}], "transitions": []}),
        );
        let mut r = Room::new(doc, None);
        let (a, b) = (r.add_user("A").unwrap(), r.add_user("B").unwrap());
        let change: Changes = [(
            "track/1/v/2".to_string(),
            Some(json!({"items": [{"id": 10, "start": 1}], "transitions": []})),
        )]
        .into();
        assert!(matches!(
            r.apply_locked(a.client_id, 1, 1, change.clone()),
            Err(Reject::TimelineLocked(None))
        ));
        r.acquire_timeline_lock(a.client_id).unwrap();
        assert_eq!(
            r.acquire_timeline_lock(b.client_id).unwrap_err().owner,
            Some(a.client_id)
        );
        assert!(
            matches!(r.apply_locked(b.client_id, 2, 1, change.clone()), Err(Reject::TimelineLocked(Some(owner))) if owner == a.client_id)
        );
        assert!(r.apply_locked(a.client_id, 3, 1, change).is_ok());
    }

    #[test]
    fn stale_object_edit_is_merged_before_lock_comparison() {
        let mut doc = Doc::new();
        doc.insert(
            "track/1/v/1".into(),
            json!({"items": [{"id": 10, "start": 0}], "transitions": []}),
        );
        let mut r = Room::new(doc, None);
        let (a, b) = (r.add_user("A").unwrap(), r.add_user("B").unwrap());
        r.set_timeline_locking(a.client_id, false).unwrap();
        r.set_object_locks(a.client_id, vec!["clip:1:12".into()])
            .unwrap();
        r.set_object_locks(b.client_id, vec!["clip:1:10".into()])
            .unwrap();

        let add_clip = [(
            "track/1/v/1".to_string(),
            Some(json!({"items": [
            {"id": 10, "start": 0}, {"id": 12, "start": 90}
        ], "transitions": []})),
        )]
        .into();
        r.apply_locked(a.client_id, 1, 1, add_clip).unwrap();

        // B's full-track edit comes from revision 1, so it does not contain A's new clip 12.
        // It must be rejected as stale before current-state lock comparison, not as an attempt
        // to remove A's newly-created clip (which B does not own).
        let stale_b_edit = [(
            "track/1/v/1".to_string(),
            Some(json!({"items": [
            {"id": 10, "start": 5}
        ], "transitions": []})),
        )]
        .into();
        assert!(
            matches!(r.apply_locked(b.client_id, 2, 1, stale_b_edit), Err(Reject::Stale(keys)) if keys == vec!["track/1/v/1".to_string()])
        );
    }

    #[test]
    fn object_lock_mode_allows_different_objects_and_blocks_the_same_object() {
        let mut doc = Doc::new();
        doc.insert(
            "track/1/v/2".into(),
            json!({"items": [
            {"id": 10, "start": 0}, {"id": 11, "start": 20}
        ], "transitions": []}),
        );
        let mut r = Room::new(doc, None);
        let (a, b) = (r.add_user("A").unwrap(), r.add_user("B").unwrap());
        r.set_timeline_locking(a.client_id, false).unwrap();
        r.set_object_locks(a.client_id, vec!["clip:1:10".into()])
            .unwrap();
        r.set_object_locks(b.client_id, vec!["clip:1:11".into()])
            .unwrap();
        let edit_a = [(
            "track/1/v/2".to_string(),
            Some(json!({"items": [
            {"id": 10, "start": 5}, {"id": 11, "start": 20}
        ], "transitions": []})),
        )]
        .into();
        assert!(r.apply_locked(a.client_id, 1, 1, edit_a).is_ok());
        let edit_b_same = [(
            "track/1/v/2".to_string(),
            Some(json!({"items": [
            {"id": 10, "start": 9}, {"id": 11, "start": 20}
        ], "transitions": []})),
        )]
        .into();
        assert!(
            matches!(r.apply_locked(b.client_id, 2, 2, edit_b_same), Err(Reject::Locked(ids)) if ids == vec!["clip:1:10".to_string()])
        );
        assert!(
            r.set_object_locks(b.client_id, vec!["clip:1:10".into()])
                .is_err()
        );
        let locks = r.lock_state().3;
        assert!(
            !locks
                .iter()
                .any(|lock| lock.client_id == b.client_id && lock.object_id == "clip:1:11"),
            "deselected objects are released even when a new reservation is denied"
        );
    }

    #[test]
    fn leaving_releases_locks_and_transfers_host() {
        let mut r = Room::new(Doc::new(), None);
        let (a, b) = (r.add_user("A").unwrap(), r.add_user("B").unwrap());
        r.acquire_timeline_lock(a.client_id).unwrap();
        r.remove_user(a.client_id);
        let (host, enabled, owner, _) = r.lock_state();
        assert_eq!(host, Some(b.client_id));
        assert!(enabled);
        assert_eq!(owner, None);
        assert!(r.set_timeline_locking(b.client_id, false).is_ok());
    }

    #[test]
    fn leaving_releases_object_reservations() {
        let mut doc = Doc::new();
        doc.insert(
            "track/1/v/1".into(),
            serde_json::json!({"items": [{"id": 10}], "transitions": []}),
        );
        let mut r = Room::new(doc, None);
        let (a, b) = (r.add_user("A").unwrap(), r.add_user("B").unwrap());
        r.set_timeline_locking(a.client_id, false).unwrap();
        r.set_object_locks(a.client_id, vec!["clip:1:10".into()])
            .unwrap();
        assert_eq!(r.lock_state().3.len(), 1);
        assert!(r.remove_user(a.client_id));
        let (host, _, _, locks) = r.lock_state();
        assert_eq!(host, Some(b.client_id));
        assert!(locks.is_empty());
    }

    #[test]
    fn catchup_returns_what_a_client_missed() {
        let mut r = room();
        r.apply(1, 1, 1, set("a", 2)).unwrap();
        r.apply(1, 2, 2, set("b", 2)).unwrap();
        assert_eq!(r.catchup(3).unwrap().len(), 0);
        assert_eq!(r.catchup(1).unwrap().len(), 2);
        assert_eq!(r.catchup(2).unwrap()[0].rev, 3);
        assert!(r.catchup(50).is_none());
    }

    #[test]
    fn users_get_distinct_ids_colours_and_the_token_is_checked() {
        let mut r = Room::new(Doc::new(), Some("secret".into()));
        assert!(r.token_ok(Some("secret")));
        assert!(!r.token_ok(Some("nope")));
        assert!(!r.token_ok(None));
        let (a, b) = (r.add_user("Ann").unwrap(), r.add_user("Bob").unwrap());
        assert_ne!(a.client_id, b.client_id);
        assert_ne!(a.color, b.color);
        assert!(r.remove_user(a.client_id));
        assert_eq!(r.users().len(), 1);
    }
}
