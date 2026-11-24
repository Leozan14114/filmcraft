//! The client side of the protocol, as a state machine with no I/O.
//!
//! The client keeps three things:
//!
//! - `mirror`: the document exactly as the relay has it, at revision `rev`. Only messages from the
//!   relay change it.
//! - `local`: the document as the editor has it, including edits the relay has not seen yet.
//!   The editor reports each changed key with [`SyncClient::local_set`].
//! - `dirty`: the keys where `local` differs from `mirror`. These are what is still to be sent.
//!
//! At most one operation is in flight. When it is acknowledged, the next batch of dirty keys goes.
//! When another client's operation arrives, every key it touches is merged into `local` with a
//! three-way merge (`base` = the mirror before, `mine` = `local`, `theirs` = the new value), so the
//! two people's work is combined, and the result is handed back to the editor as
//! [`Remote::updates`]. When our own operation is refused because someone got there first, nothing
//! is rolled back: the newer values were already merged in, the dirty keys are recomputed against
//! the newer mirror, and the next send carries only what is still different.
//!
//! Operations are assignments of whole values, so sending the same thing twice, or after a
//! reconnect, is harmless: a value the relay already has is simply not dirty.

use std::collections::BTreeSet;

use serde_json::Value;

use crate::merge::merge3;
use crate::proto::{AppliedOp, Changes, ClientMsg, Doc, Key, ServerMsg};

/// What a remote operation did to the local document.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Remote {
    /// The relay revision this brings the mirror to.
    pub rev: u64,
    /// Keys whose *local* value changed and must be installed in the editor (the merged value, or
    /// `None` when the key is gone).
    pub updates: Changes,
    /// Every key the remote operation touched, changed locally or not.
    pub remote_keys: Vec<Key>,
    /// Leaves both sides had changed differently; theirs were kept.
    pub conflicts: usize,
}

impl Remote {
    fn absorb(&mut self, other: Remote) {
        self.rev = other.rev;
        self.updates.extend(other.updates);
        self.remote_keys.extend(other.remote_keys);
        self.conflicts += other.conflicts;
    }
}

/// What a relay message meant for the client.
#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    /// Our operation was accepted.
    Acked,
    /// Our operation was refused; the next [`SyncClient::poll_send`] sends what is still different.
    Rejected { reason: String },
    /// Another client's operation was merged in.
    Remote(Remote),
    /// Nothing for the editor (an old duplicate, presence…).
    Ignored,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SyncError {
    /// A revision was skipped; the client must ask for the whole state.
    Gap { expected: u64, got: u64 },
}

impl std::fmt::Display for SyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncError::Gap { expected, got } => write!(f, "missed revisions (expected {expected}, got {got})"),
        }
    }
}

#[derive(Clone, Debug)]
struct Inflight {
    op_id: u64,
    sent: Changes,
}

pub struct SyncClient {
    client_id: u64,
    rev: u64,
    mirror: Doc,
    local: Doc,
    dirty: BTreeSet<Key>,
    inflight: Option<Inflight>,
    next_op: u64,
    /// Leaves merged to the other side's value since this client started.
    pub total_conflicts: u64,
    /// Operations the relay refused since this client started.
    pub rejected_ops: u64,
}

fn put(doc: &mut Doc, key: &str, value: Option<Value>) {
    match value {
        Some(v) => {
            doc.insert(key.to_string(), v);
        }
        None => {
            doc.remove(key);
        }
    }
}

impl SyncClient {
    /// A client that holds exactly the relay's document at `rev`.
    pub fn new(client_id: u64, rev: u64, state: Doc) -> Self {
        Self { client_id, rev, local: state.clone(), mirror: state, dirty: BTreeSet::new(), inflight: None, next_op: 1, total_conflicts: 0, rejected_ops: 0 }
    }

    pub fn client_id(&self) -> u64 {
        self.client_id
    }
    pub fn rev(&self) -> u64 {
        self.rev
    }
    /// A reconnect gives the client a new id. What was in flight is forgotten: if the relay did
    /// apply it, the catch-up shows the mirror already holds it and nothing is resent.
    pub fn reconnected(&mut self, client_id: u64) {
        self.client_id = client_id;
        self.inflight = None;
    }
    pub fn local(&self) -> &Doc {
        &self.local
    }
    pub fn mirror(&self) -> &Doc {
        &self.mirror
    }
    /// Whether the relay has everything this client has.
    pub fn is_idle(&self) -> bool {
        self.inflight.is_none() && self.dirty.is_empty()
    }
    /// Keys still to be sent (including those in flight).
    pub fn pending_keys(&self) -> usize {
        self.dirty.len()
    }

    fn refresh_dirty(&mut self, key: &str) {
        if self.local.get(key) == self.mirror.get(key) {
            self.dirty.remove(key);
        } else {
            self.dirty.insert(key.to_string());
        }
    }

    /// The editor's value for `key` is now `value` (`None`: the key is gone).
    pub fn local_set(&mut self, key: &str, value: Option<Value>) {
        if self.local.get(key) == value.as_ref() {
            return;
        }
        put(&mut self.local, key, value);
        self.refresh_dirty(key);
    }

    /// The next operation to send, if none is in flight and something differs from the relay.
    pub fn poll_send(&mut self) -> Option<ClientMsg> {
        if self.inflight.is_some() || self.dirty.is_empty() {
            return None;
        }
        let changes: Changes = self.dirty.iter().map(|k| (k.clone(), self.local.get(k).cloned())).collect();
        let op_id = self.next_op;
        self.next_op += 1;
        self.inflight = Some(Inflight { op_id, sent: changes.clone() });
        Some(ClientMsg::Op { op_id, base_rev: self.rev, changes })
    }

    /// Handle `Applied` and `Rejected`. Other messages are [`Event::Ignored`]; the connection layer
    /// handles `Welcome` and `Catchup` through [`SyncClient::on_snapshot`] and
    /// [`SyncClient::on_catchup`].
    pub fn on_message(&mut self, msg: &ServerMsg) -> Result<Event, SyncError> {
        match msg {
            ServerMsg::Applied { rev, client_id, op_id, changes } => self.on_applied(*rev, *client_id, *op_id, changes),
            ServerMsg::Rejected { op_id, reason, .. } => {
                if self.inflight.as_ref().is_some_and(|f| f.op_id == *op_id) {
                    self.inflight = None;
                    self.rejected_ops += 1;
                    Ok(Event::Rejected { reason: reason.clone() })
                } else {
                    Ok(Event::Ignored)
                }
            }
            _ => Ok(Event::Ignored),
        }
    }

    fn on_applied(&mut self, rev: u64, client_id: u64, op_id: u64, changes: &Changes) -> Result<Event, SyncError> {
        if rev <= self.rev {
            return Ok(Event::Ignored);
        }
        if rev != self.rev + 1 {
            return Err(SyncError::Gap { expected: self.rev + 1, got: rev });
        }
        let ours = client_id == self.client_id && self.inflight.as_ref().is_some_and(|f| f.op_id == op_id);
        if ours {
            if let Some(f) = self.inflight.take() {
                for (k, v) in f.sent {
                    put(&mut self.mirror, &k, v);
                    self.refresh_dirty(&k);
                }
            }
            self.rev = rev;
            return Ok(Event::Acked);
        }
        Ok(Event::Remote(self.apply_remote(changes, rev)))
    }

    fn apply_remote(&mut self, changes: &Changes, rev: u64) -> Remote {
        let mut out = Remote { rev, ..Remote::default() };
        for (key, theirs) in changes {
            let base = self.mirror.get(key).cloned();
            let mine = self.local.get(key).cloned();
            let new_local = if mine == base {
                theirs.clone()
            } else {
                let merged = merge3(base.as_ref(), mine.as_ref(), theirs.as_ref());
                out.conflicts += merged.conflicts;
                merged.value
            };
            if new_local != mine {
                out.updates.insert(key.clone(), new_local.clone());
            }
            put(&mut self.local, key, new_local);
            put(&mut self.mirror, key, theirs.clone());
            self.refresh_dirty(key);
            out.remote_keys.push(key.clone());
        }
        self.rev = rev;
        self.total_conflicts += out.conflicts as u64;
        out
    }

    /// Adopt the relay's whole document (a join, or a resync). Whatever differs from the mirror is
    /// treated as a remote change and merged with the local edits, so nothing local is lost.
    pub fn on_snapshot(&mut self, rev: u64, state: Doc) -> Remote {
        self.inflight = None;
        let mut changes = Changes::new();
        for (k, v) in &state {
            if self.mirror.get(k) != Some(v) {
                changes.insert(k.clone(), Some(v.clone()));
            }
        }
        for k in self.mirror.keys() {
            if !state.contains_key(k) {
                changes.insert(k.clone(), None);
            }
        }
        self.apply_remote(&changes, rev)
    }

    /// Adopt the operations a reconnecting client missed.
    pub fn on_catchup(&mut self, ops: &[AppliedOp]) -> Result<Remote, SyncError> {
        let mut out = Remote { rev: self.rev, ..Remote::default() };
        for op in ops {
            if op.rev <= self.rev {
                continue;
            }
            if op.rev != self.rev + 1 {
                return Err(SyncError::Gap { expected: self.rev + 1, got: op.rev });
            }
            out.absorb(self.apply_remote(&op.changes, op.rev));
        }
        Ok(out)
    }

    /// Cancel an operation rejected by the relay's lock policy and restore its keys from the
    /// authoritative mirror. The returned changes are ready for the editor to install.
    pub fn reject_locked(&mut self, op_id: u64, keys: &[Key]) -> Option<Changes> {
        let inflight = self.inflight.take()?;
        if inflight.op_id != op_id {
            self.inflight = Some(inflight);
            return None;
        }
        self.rejected_ops += 1;
        let keys: Vec<Key> = if keys.is_empty() { inflight.sent.keys().cloned().collect() } else { keys.to_vec() };
        let mut restore = Changes::new();
        for key in keys {
            let value = self.mirror.get(&key).cloned();
            put(&mut self.local, &key, value.clone());
            self.refresh_dirty(&key);
            restore.insert(key, value);
        }
        Some(restore)
    }

    /// The editor could not accept the merged value of `key` (it broke a rule of the project): drop
    /// the local value in favour of the relay's. Returns that value for the editor to install.
    pub fn force_theirs(&mut self, key: &str) -> Option<Value> {
        let theirs = self.mirror.get(key).cloned();
        put(&mut self.local, key, theirs.clone());
        self.refresh_dirty(key);
        theirs
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{Event, SyncClient, SyncError};
    use crate::proto::{ClientMsg, Doc, ServerMsg};
    use crate::room::{Reject, Room};

    fn doc() -> Doc {
        let mut d = Doc::new();
        d.insert("t1".into(), json!({"id": 1, "items": [{"id": 10, "start": 0}]}));
        d.insert("t2".into(), json!({"id": 2, "items": []}));
        d
    }

    /// A relay and two clients, with explicit delivery so tests choose the interleaving.
    struct Net {
        room: Room,
        a: SyncClient,
        b: SyncClient,
        to_a: Vec<ServerMsg>,
        to_b: Vec<ServerMsg>,
    }

    impl Net {
        fn new() -> Net {
            let mut room = Room::new(doc(), None);
            let (ua, ub) = (room.add_user("A").unwrap(), room.add_user("B").unwrap());
            Net { a: SyncClient::new(ua.client_id, 1, doc()), b: SyncClient::new(ub.client_id, 1, doc()), room, to_a: vec![], to_b: vec![] }
        }
        /// The relay processes a client's next operation and queues the replies.
        fn send(&mut self, from_a: bool) {
            let msg = if from_a { self.a.poll_send() } else { self.b.poll_send() };
            let Some(ClientMsg::Op { op_id, base_rev, changes }) = msg else { return };
            let client = if from_a { self.a.client_id() } else { self.b.client_id() };
            match self.room.apply(client, op_id, base_rev, changes) {
                Ok(op) => {
                    let m = ServerMsg::Applied { rev: op.rev, client_id: op.client_id, op_id: op.op_id, changes: op.changes };
                    self.to_a.push(m.clone());
                    self.to_b.push(m);
                }
                Err(Reject::Stale(keys)) => {
                    let m = ServerMsg::Rejected { op_id, rev: self.room.rev(), reason: "stale".into(), keys };
                    if from_a { self.to_a.push(m) } else { self.to_b.push(m) }
                }
                Err(Reject::Invalid(reason)) => panic!("{reason}"),
                Err(Reject::TimelineLocked(_)) | Err(Reject::Locked(_)) => panic!("lock check should not run in this test harness"),
            }
        }
        fn deliver(&mut self, to_a: bool) -> Event {
            let q = if to_a { &mut self.to_a } else { &mut self.to_b };
            let m = q.remove(0);
            if to_a { self.a.on_message(&m).unwrap() } else { self.b.on_message(&m).unwrap() }
        }
        fn settle(&mut self) {
            for _ in 0..50 {
                self.send(true);
                self.send(false);
                while !self.to_a.is_empty() {
                    self.deliver(true);
                }
                while !self.to_b.is_empty() {
                    self.deliver(false);
                }
            }
        }
    }

    fn clips(d: &Doc, key: &str) -> Vec<(i64, i64)> {
        let items = d.get(key).and_then(|v| v.get("items")).and_then(Value::as_array).cloned().unwrap_or_default();
        let mut v: Vec<(i64, i64)> = items.iter().map(|c| (c["id"].as_i64().unwrap(), c["start"].as_i64().unwrap())).collect();
        v.sort();
        v
    }

    #[test]
    fn a_lock_rejected_edit_restores_the_authoritative_value() {
        let original = doc();
        let mut client = SyncClient::new(1, 1, original.clone());
        let mut edited = client.local().get("t1").cloned().unwrap();
        edited["items"][0]["start"] = json!(99);
        client.local_set("t1", Some(edited));
        let Some(ClientMsg::Op { op_id, .. }) = client.poll_send() else { panic!("expected an operation") };

        let restored = client.reject_locked(op_id, &["t1".to_string()]).unwrap();
        assert_eq!(restored.get("t1").cloned(), Some(Some(original["t1"].clone())));
        assert_eq!(client.local().get("t1"), Some(&original["t1"]));
        assert_eq!(client.pending_keys(), 0);
        assert!(client.poll_send().is_none());
        assert_eq!(client.rejected_ops, 1);
    }

    #[test]
    fn an_edit_reaches_the_other_client_and_nothing_loops() {
        let mut n = Net::new();
        let mut t = n.a.local().get("t2").cloned().unwrap();
        t["items"] = json!([{"id": 20, "start": 5}]);
        n.a.local_set("t2", Some(t));
        n.send(true);
        assert!(matches!(n.deliver(true), Event::Acked));
        let Event::Remote(r) = n.deliver(false) else { panic!("expected a remote change") };
        assert_eq!(r.remote_keys, vec!["t2".to_string()]);
        assert_eq!(r.updates.len(), 1);
        // B now holds the same thing locally, so it has nothing to send back
        assert!(n.b.poll_send().is_none());
        assert!(n.a.poll_send().is_none());
        assert_eq!(n.a.local(), n.b.local());
    }

    #[test]
    fn simultaneous_edits_to_different_clips_of_one_track_both_survive() {
        let mut n = Net::new();
        let mut ta = n.a.local().get("t1").cloned().unwrap();
        ta["items"].as_array_mut().unwrap().push(json!({"id": 11, "start": 50}));
        let mut tb = n.b.local().get("t1").cloned().unwrap();
        tb["items"][0]["start"] = json!(7);
        n.a.local_set("t1", Some(ta));
        n.b.local_set("t1", Some(tb));
        // both send from revision 1; the relay accepts A and refuses B
        n.send(true);
        n.send(false);
        assert!(matches!(n.to_b[1], ServerMsg::Rejected { .. }));
        n.settle();
        assert_eq!(n.a.local(), n.b.local());
        assert_eq!(n.a.local(), n.room.state());
        assert_eq!(clips(n.room.state(), "t1"), vec![(10, 7), (11, 50)]);
        assert_eq!(n.b.rejected_ops, 1);
        assert_eq!(n.a.total_conflicts + n.b.total_conflicts, 0);
    }

    #[test]
    fn a_clash_on_the_same_field_is_reported_not_silent() {
        let mut n = Net::new();
        let mut ta = n.a.local().get("t1").cloned().unwrap();
        ta["items"][0]["start"] = json!(1);
        let mut tb = n.b.local().get("t1").cloned().unwrap();
        tb["items"][0]["start"] = json!(2);
        n.a.local_set("t1", Some(ta));
        n.b.local_set("t1", Some(tb));
        n.send(true);
        n.send(false);
        n.settle();
        assert_eq!(n.a.local(), n.b.local());
        assert_eq!(clips(n.room.state(), "t1"), vec![(10, 1)]);
        assert_eq!(n.b.total_conflicts, 1);
    }

    #[test]
    fn edits_made_while_an_operation_is_in_flight_are_sent_next() {
        let mut n = Net::new();
        n.a.local_set("t2", Some(json!({"id": 2, "items": [{"id": 30, "start": 1}]})));
        n.send(true);
        n.a.local_set("t2", Some(json!({"id": 2, "items": [{"id": 30, "start": 1}, {"id": 31, "start": 9}]})));
        assert!(n.a.poll_send().is_none(), "one operation at a time");
        n.settle();
        assert_eq!(clips(n.room.state(), "t2"), vec![(30, 1), (31, 9)]);
        assert!(n.a.is_idle() && n.b.is_idle());
    }

    #[test]
    fn a_skipped_revision_is_a_gap() {
        let mut c = SyncClient::new(1, 1, doc());
        let m = ServerMsg::Applied { rev: 3, client_id: 2, op_id: 1, changes: [("t2".to_string(), Some(json!({"id": 2})))].into() };
        assert_eq!(c.on_message(&m), Err(SyncError::Gap { expected: 2, got: 3 }));
    }

    #[test]
    fn old_duplicates_are_ignored() {
        let mut c = SyncClient::new(1, 5, doc());
        let m = ServerMsg::Applied { rev: 4, client_id: 2, op_id: 1, changes: [("t2".to_string(), None)].into() };
        assert_eq!(c.on_message(&m), Ok(Event::Ignored));
        assert!(c.local().contains_key("t2"));
    }

    #[test]
    fn a_snapshot_merges_with_unsent_local_edits() {
        let mut c = SyncClient::new(1, 1, doc());
        c.local_set("t1", Some(json!({"id": 1, "items": [{"id": 10, "start": 0}, {"id": 12, "start": 80}]})));
        let mut theirs = doc();
        theirs.insert("t2".into(), json!({"id": 2, "items": [{"id": 40, "start": 3}]}));
        let r = c.on_snapshot(9, theirs);
        assert_eq!(c.rev(), 9);
        assert_eq!(r.updates.len(), 1);
        assert_eq!(clips(c.local(), "t1"), vec![(10, 0), (12, 80)]);
        assert_eq!(clips(c.local(), "t2"), vec![(40, 3)]);
        assert_eq!(c.pending_keys(), 1, "the local edit to t1 is still to be sent");
    }

    #[test]
    fn catchup_applies_missed_operations_in_order() {
        let mut room = Room::new(doc(), None);
        room.apply(7, 1, 1, [("t2".to_string(), Some(json!({"id": 2, "items": [{"id": 5, "start": 1}]})))].into()).unwrap();
        room.apply(7, 2, 2, [("t2".to_string(), None)].into()).unwrap();
        let mut c = SyncClient::new(1, 1, doc());
        let r = c.on_catchup(&room.catchup(1).unwrap()).unwrap();
        assert_eq!((r.rev, c.rev()), (3, 3));
        assert!(!c.local().contains_key("t2"));
        assert_eq!(c.local(), room.state());
    }

    #[test]
    fn force_theirs_drops_the_local_value() {
        let mut c = SyncClient::new(1, 1, doc());
        c.local_set("t2", Some(json!({"id": 2, "items": [{"id": 1, "start": 1}]})));
        assert_eq!(c.pending_keys(), 1);
        let v = c.force_theirs("t2");
        assert_eq!(v, doc().get("t2").cloned());
        assert!(c.is_idle());
    }
}
