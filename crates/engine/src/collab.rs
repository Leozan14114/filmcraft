//! Real-time collaboration, engine side.
//!
//! A [`Project`] is mapped onto the shared document of `filmcraft-collab`: a map of keys to JSON.
//!
//! | key | holds |
//! |---|---|
//! | `meta` | the project without its items and `next_id` (name, bins, settings, LUTs, transcripts, notes…) |
//! | `item/<id>` | one project item that is not a sequence (media, subclip, adjustment layer, graphic) |
//! | `seq/<id>` | a sequence item without its tracks, and the order of its tracks |
//! | `track/<seq>/<v|a|s>/<id>` | one video, audio or submix track, with its clips and transitions |
//! | `track/<seq>/c/<id>` | one caption track |
//!
//! Edits to distinct tracks or distinct locked objects can coexist; the relay reserves each selected
//! clip/caption/transition (or the whole timeline when that mode is active). The three-way merge in
//! `filmcraft_collab::merge3` still combines compatible changes to the same document key.
//!
//! The session does **not** replicate commands. Commands depend on each machine's selection and
//! active sequence; what they *did* is in the project. After every command the project is compared
//! with what was last reported ([`CollabState::seen`]) and only the keys that really changed are
//! reported. Selection, zoom, playhead, panels and the undo history live outside the project, so
//! they never travel.
//!
//! Undo and redo are *selective* while a session is active: they put back only the parts the undone
//! step changed, and never a part somebody else has changed since.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::Arc;

use filmcraft_collab::{
    Changes, ClientMsg, Doc, Event, Key, Remote, ServerMsg, SyncClient, SyncError, merge3,
};
use filmcraft_project::caption::CaptionTrack;
use filmcraft_project::{ItemId, ItemKind, Project, ProjectItem, Sequence, Track, TrackId};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::Session;

const META: &str = "meta";
/// Each client allocates new ids from its own range, so two people adding clips never collide.
const ID_RANGE: u64 = 1 << 40;
/// Largest client id whose id range fits under `Project::MAX_NEXT_ID`.
const MAX_CLIENT_ID: u64 = (1 << 22) - 1;
/// Remote operations remembered for undo (which keys others changed, and when).
const REMOTE_LOG_LIMIT: usize = 20_000;
/// How many times a failing merged key is replaced by the relay's value before giving up.
const MAX_REPAIRS: usize = 8;

// ---- keys ----

fn item_key(id: ItemId) -> Key {
    format!("item/{}", id.0)
}
fn seq_key(id: ItemId) -> Key {
    format!("seq/{}", id.0)
}
fn track_key(seq: ItemId, kind: char, id: TrackId) -> Key {
    format!("track/{}/{}/{}", seq.0, kind, id.0)
}

enum KeyKind {
    Meta,
    Item(ItemId),
    Seq(ItemId),
    Track(ItemId, char, u64),
}

fn parse_key(key: &str) -> Option<KeyKind> {
    if key == META {
        return Some(KeyKind::Meta);
    }
    let mut parts = key.split('/');
    match (
        parts.next()?,
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) {
        ("item", Some(id), None, None, None) => Some(KeyKind::Item(ItemId(id.parse().ok()?))),
        ("seq", Some(id), None, None, None) => Some(KeyKind::Seq(ItemId(id.parse().ok()?))),
        ("track", Some(seq), Some(kind), Some(id), None) => {
            let kind = kind
                .chars()
                .next()
                .filter(|c| matches!(*c, 'v' | 'a' | 's' | 'c') && kind.len() == 1)?;
            Some(KeyKind::Track(
                ItemId(seq.parse().ok()?),
                kind,
                id.parse().ok()?,
            ))
        }
        _ => None,
    }
}

// ---- serialization ----

fn to_value<T: Serialize>(v: &T) -> Result<Value, String> {
    serde_json::to_value(v).map_err(|e| e.to_string())
}

fn from_value<T: DeserializeOwned>(v: &Value) -> Result<T, String> {
    serde_json::from_value(v.clone()).map_err(|e| e.to_string())
}

/// The project without its items and id counter.
fn meta_project(p: &Project) -> Project {
    Project {
        name: p.name.clone(),
        custom_colors: p.custom_colors.clone(),
        custom_color_names: p.custom_color_names.clone(),
        settings: p.settings.clone(),
        root: p.root.clone(),
        items: BTreeMap::new(),
        next_id: 0,
        luts: p.luts.clone(),
        transcripts: p.transcripts.clone(),
        search_bins: p.search_bins.clone(),
        source_graphics: p.source_graphics.clone(),
        narrations: p.narrations.clone(),
        notes: p.notes.clone(),
    }
}

fn meta_equal(a: &Project, b: &Project) -> bool {
    a.name == b.name
        && a.custom_colors == b.custom_colors
        && a.custom_color_names == b.custom_color_names
        && a.settings == b.settings
        && a.root == b.root
        && a.luts == b.luts
        && a.transcripts == b.transcripts
        && a.search_bins == b.search_bins
        && a.source_graphics == b.source_graphics
        && a.narrations == b.narrations
        && a.notes == b.notes
}

fn apply_meta(p: &mut Project, v: &Value) -> Result<(), String> {
    let m: Project = from_value(v)?;
    p.name = m.name;
    p.custom_colors = m.custom_colors;
    p.custom_color_names = m.custom_color_names;
    p.settings = m.settings;
    p.root = m.root;
    p.luts = m.luts;
    // the same bound a project file gets on load
    p.luts
        .retain(|l| l.text.len() <= filmcraft_project::MAX_LUT_TEXT_BYTES);
    p.transcripts = m.transcripts;
    p.search_bins = m.search_bins;
    p.source_graphics = m.source_graphics;
    p.narrations = m.narrations;
    p.notes = m.notes;
    Ok(())
}

/// A sequence without tracks (they are separate keys).
fn bare_sequence(s: &Sequence) -> Sequence {
    Sequence {
        settings: s.settings.clone(),
        video_tracks: Vec::new(),
        audio_tracks: Vec::new(),
        markers: s.markers.clone(),
        mark_in: s.mark_in,
        mark_out: s.mark_out,
        work_area: s.work_area.clone(),
        start_timecode: s.start_timecode,
        master_volume_db: s.master_volume_db,
        master_effects: s.master_effects.clone(),
        master_mixer: s.master_mixer.clone(),
        submix_tracks: Vec::new(),
        caption_tracks: Vec::new(),
        multicam: s.multicam.clone(),
        merged: s.merged.clone(),
        split: s.split,
    }
}

/// The track ids of a sequence, in order (a list of `{"id": n}` so concurrent additions and
/// removals merge by id).
#[derive(Clone, Debug, Default, PartialEq)]
struct Order {
    v: Vec<u64>,
    a: Vec<u64>,
    s: Vec<u64>,
    c: Vec<u64>,
}

fn order_of(s: &Sequence) -> Order {
    Order {
        v: s.video_tracks.iter().map(|t| t.id.0).collect(),
        a: s.audio_tracks.iter().map(|t| t.id.0).collect(),
        s: s.submix_tracks.iter().map(|t| t.id.0).collect(),
        c: s.caption_tracks.iter().map(|t| t.id.0).collect(),
    }
}

fn ids_value(ids: &[u64]) -> Value {
    Value::Array(ids.iter().map(|i| json!({ "id": i })).collect())
}

fn ids_from(v: Option<&Value>) -> Result<Vec<u64>, String> {
    let arr = v
        .and_then(Value::as_array)
        .ok_or("track order is missing")?;
    arr.iter()
        .map(|e| {
            e.get("id")
                .and_then(Value::as_u64)
                .ok_or_else(|| "bad track id".to_string())
        })
        .collect()
}

fn seq_header(item: &ProjectItem, seq: &Sequence) -> Result<Value, String> {
    let mut bare = item.clone();
    bare.kind = ItemKind::Sequence(Arc::new(bare_sequence(seq)));
    let o = order_of(seq);
    Ok(
        json!({ "item": to_value(&bare)?, "order": { "v": ids_value(&o.v), "a": ids_value(&o.a), "s": ids_value(&o.s), "c": ids_value(&o.c) } }),
    )
}

fn parse_header(v: &Value) -> Result<(ProjectItem, Order), String> {
    let item: ProjectItem = from_value(v.get("item").ok_or("sequence header has no item")?)?;
    if !matches!(item.kind, ItemKind::Sequence(_)) {
        return Err("sequence header holds a different kind of item".into());
    }
    let o = v.get("order").ok_or("sequence header has no track order")?;
    let order = Order {
        v: ids_from(o.get("v"))?,
        a: ids_from(o.get("a"))?,
        s: ids_from(o.get("s"))?,
        c: ids_from(o.get("c"))?,
    };
    Ok((item, order))
}

/// Everything a sequence item contributes to the document.
fn sequence_entries(item: &ProjectItem, seq: &Sequence, out: &mut Changes) -> Result<(), String> {
    out.insert(seq_key(item.id), Some(seq_header(item, seq)?));
    diff_group(item.id, 'v', &[], &seq.video_tracks, |t: &Track| t.id, out)?;
    diff_group(item.id, 'a', &[], &seq.audio_tracks, |t: &Track| t.id, out)?;
    diff_group(item.id, 's', &[], &seq.submix_tracks, |t: &Track| t.id, out)?;
    diff_group(
        item.id,
        'c',
        &[],
        &seq.caption_tracks,
        |t: &CaptionTrack| t.id,
        out,
    )
}

fn item_entries(item: &ProjectItem, out: &mut Changes) -> Result<(), String> {
    match &item.kind {
        ItemKind::Sequence(seq) => sequence_entries(item, seq, out),
        _ => {
            out.insert(item_key(item.id), Some(to_value(item)?));
            Ok(())
        }
    }
}

fn removed_item_entries(item: &ProjectItem, out: &mut Changes) {
    match &item.kind {
        ItemKind::Sequence(seq) => {
            out.insert(seq_key(item.id), None);
            for t in &seq.video_tracks {
                out.insert(track_key(item.id, 'v', t.id), None);
            }
            for t in &seq.audio_tracks {
                out.insert(track_key(item.id, 'a', t.id), None);
            }
            for t in &seq.submix_tracks {
                out.insert(track_key(item.id, 's', t.id), None);
            }
            for t in &seq.caption_tracks {
                out.insert(track_key(item.id, 'c', t.id), None);
            }
        }
        _ => {
            out.insert(item_key(item.id), None);
        }
    }
}

/// The whole project as a shared document.
pub fn project_to_doc(p: &Project) -> Result<Doc, String> {
    let mut changes = Changes::new();
    changes.insert(META.to_string(), Some(to_value(&meta_project(p))?));
    for item in p.items.values() {
        item_entries(item, &mut changes)?;
    }
    Ok(changes
        .into_iter()
        .filter_map(|(k, v)| v.map(|v| (k, v)))
        .collect())
}

/// The value of one key in `p` (`None`: the key does not exist there).
fn value_of(p: &Project, key: &str) -> Result<Option<Value>, String> {
    match parse_key(key) {
        Some(KeyKind::Meta) => Ok(Some(to_value(&meta_project(p))?)),
        Some(KeyKind::Item(id)) => match p.items.get(&id) {
            Some(item) if !matches!(item.kind, ItemKind::Sequence(_)) => Ok(Some(to_value(item)?)),
            _ => Ok(None),
        },
        Some(KeyKind::Seq(id)) => match p.items.get(&id) {
            Some(item) => match item.as_sequence() {
                Some(seq) => Ok(Some(seq_header(item, seq)?)),
                None => Ok(None),
            },
            None => Ok(None),
        },
        Some(KeyKind::Track(seq, kind, tid)) => {
            let Some(s) = p.sequence(seq) else {
                return Ok(None);
            };
            let found = match kind {
                'v' => s.video_tracks.iter().find(|t| t.id.0 == tid).map(to_value),
                'a' => s.audio_tracks.iter().find(|t| t.id.0 == tid).map(to_value),
                's' => s.submix_tracks.iter().find(|t| t.id.0 == tid).map(to_value),
                _ => s
                    .caption_tracks
                    .iter()
                    .find(|t| t.id.0 == tid)
                    .map(to_value),
            };
            found.transpose()
        }
        None => Ok(None),
    }
}

fn items_equal(a: &ProjectItem, b: &ProjectItem) -> bool {
    match (&a.kind, &b.kind) {
        (ItemKind::Sequence(x), ItemKind::Sequence(y)) if Arc::ptr_eq(x, y) => {
            a.id == b.id
                && a.name == b.name
                && a.label == b.label
                && a.custom_label_color == b.custom_label_color
                && a.metadata == b.metadata
                && a.created == b.created
                && a.split == b.split
        }
        _ => a == b,
    }
}

fn diff_group<T: Serialize + PartialEq>(
    seq: ItemId,
    kind: char,
    old: &[T],
    new: &[T],
    id_of: fn(&T) -> TrackId,
    out: &mut Changes,
) -> Result<(), String> {
    let old_by: BTreeMap<u64, &T> = old.iter().map(|t| (id_of(t).0, t)).collect();
    let mut present = BTreeSet::new();
    for t in new {
        let id = id_of(t);
        present.insert(id.0);
        if old_by.get(&id.0).is_none_or(|o| *o != t) {
            out.insert(track_key(seq, kind, id), Some(to_value(t)?));
        }
    }
    for id in old_by.keys() {
        if !present.contains(id) {
            out.insert(track_key(seq, kind, TrackId(*id)), None);
        }
    }
    Ok(())
}

fn diff_sequence(
    old_item: &ProjectItem,
    old: &Sequence,
    new_item: &ProjectItem,
    new: &Sequence,
    out: &mut Changes,
) -> Result<(), String> {
    let (header_old, header_new) = (seq_header(old_item, old)?, seq_header(new_item, new)?);
    if header_old != header_new {
        out.insert(seq_key(new_item.id), Some(header_new));
    }
    let id = new_item.id;
    diff_group(
        id,
        'v',
        &old.video_tracks,
        &new.video_tracks,
        |t: &Track| t.id,
        out,
    )?;
    diff_group(
        id,
        'a',
        &old.audio_tracks,
        &new.audio_tracks,
        |t: &Track| t.id,
        out,
    )?;
    diff_group(
        id,
        's',
        &old.submix_tracks,
        &new.submix_tracks,
        |t: &Track| t.id,
        out,
    )?;
    diff_group(
        id,
        'c',
        &old.caption_tracks,
        &new.caption_tracks,
        |t: &CaptionTrack| t.id,
        out,
    )
}

/// The keys whose value differs between `old` and `new`, with their new value (`None`: removed).
/// Parts that are the same `Arc` or equal are not even serialized.
pub fn changed_keys(old: &Project, new: &Project) -> Result<Changes, String> {
    let mut out = Changes::new();
    if !meta_equal(old, new) {
        out.insert(META.to_string(), Some(to_value(&meta_project(new))?));
    }
    for (id, item) in &new.items {
        match old.items.get(id) {
            Some(o) if items_equal(o, item) => {}
            Some(o) => match (&o.kind, &item.kind) {
                (ItemKind::Sequence(os), ItemKind::Sequence(ns)) => {
                    diff_sequence(o, os, item, ns, &mut out)?
                }
                _ => {
                    removed_item_entries(o, &mut out);
                    item_entries(item, &mut out)?;
                }
            },
            None => item_entries(item, &mut out)?,
        }
    }
    for (id, o) in &old.items {
        if !new.items.contains_key(id) {
            removed_item_entries(o, &mut out);
        }
    }
    Ok(out)
}

// ---- applying changes ----

/// A failure applying changes: the key at fault when there is one, and why.
pub type ApplyError = (Option<Key>, String);

fn parse_track<T: DeserializeOwned>(
    key: &str,
    v: &Value,
    id: u64,
    id_of: fn(&T) -> TrackId,
) -> Result<T, ApplyError> {
    let t: T = from_value(v).map_err(|m| (Some(key.to_string()), m))?;
    if id_of(&t).0 != id {
        return Err((
            Some(key.to_string()),
            "track id does not match its key".into(),
        ));
    }
    Ok(t)
}

fn take_tracks(
    pool: &mut BTreeMap<(char, u64), Track>,
    kind: char,
    ids: &[u64],
    sid: ItemId,
) -> Result<Vec<Track>, ApplyError> {
    ids.iter()
        .map(|id| {
            pool.remove(&(kind, *id)).ok_or_else(|| {
                (
                    Some(track_key(sid, kind, TrackId(*id))),
                    "track listed by the sequence is missing".to_string(),
                )
            })
        })
        .collect()
}

/// Rebuild sequence `sid` from its current tracks plus the header and track changes in `updates`.
fn assemble_sequence(
    p: &mut Project,
    sid: ItemId,
    updates: &Changes,
    strict: bool,
) -> Result<(), ApplyError> {
    let hkey = seq_key(sid);
    let header_update = updates.get(&hkey);
    if let Some(None) = header_update {
        p.items.remove(&sid);
        return Ok(());
    }
    // take the current tracks out of the sequence
    let mut pool: BTreeMap<(char, u64), Track> = BTreeMap::new();
    let mut cpool: BTreeMap<u64, CaptionTrack> = BTreeMap::new();
    let mut old_order = Order::default();
    if let Some(seq) = p.items.get_mut(&sid).and_then(ProjectItem::as_sequence_mut) {
        old_order = order_of(seq);
        for (kind, tracks) in [
            ('v', std::mem::take(&mut seq.video_tracks)),
            ('a', std::mem::take(&mut seq.audio_tracks)),
            ('s', std::mem::take(&mut seq.submix_tracks)),
        ] {
            for t in tracks {
                pool.insert((kind, t.id.0), t);
            }
        }
        for c in std::mem::take(&mut seq.caption_tracks) {
            cpool.insert(c.id.0, c);
        }
    }
    // the track changes
    for (key, value) in updates {
        let Some(KeyKind::Track(seq, kind, tid)) = parse_key(key) else {
            continue;
        };
        if seq != sid {
            continue;
        }
        match (value, kind) {
            (None, 'c') => {
                cpool.remove(&tid);
            }
            (None, _) => {
                pool.remove(&(kind, tid));
            }
            (Some(v), 'c') => {
                let mut t: CaptionTrack = parse_track(key, v, tid, |t: &CaptionTrack| t.id)?;
                t.sort();
                let verdict = if strict { t.check() } else { t.check_bounds() };
                verdict.map_err(|m| (Some(key.clone()), m))?;
                cpool.insert(tid, t);
            }
            (Some(v), _) => {
                let mut t: Track = parse_track(key, v, tid, |t: &Track| t.id)?;
                t.sort();
                let verdict = if strict { t.check() } else { t.check_bounds() };
                verdict.map_err(|m| (Some(key.clone()), m))?;
                pool.insert((kind, tid), t);
            }
        }
    }
    // the header
    let (mut item, order) = match header_update {
        Some(Some(v)) => parse_header(v).map_err(|m| (Some(hkey.clone()), m))?,
        _ => match p.items.get(&sid) {
            Some(it) if it.as_sequence().is_some() => (it.clone(), old_order),
            // tracks of a sequence this document does not have
            _ => return Ok(()),
        },
    };
    if item.id != sid {
        return Err((
            Some(hkey),
            "sequence header id does not match its key".into(),
        ));
    }
    let video = take_tracks(&mut pool, 'v', &order.v, sid)?;
    let audio = take_tracks(&mut pool, 'a', &order.a, sid)?;
    let submix = take_tracks(&mut pool, 's', &order.s, sid)?;
    let captions: Vec<CaptionTrack> = order
        .c
        .iter()
        .map(|id| {
            cpool.remove(id).ok_or_else(|| {
                (
                    Some(track_key(sid, 'c', TrackId(*id))),
                    "caption track listed by the sequence is missing".to_string(),
                )
            })
        })
        .collect::<Result<_, _>>()?;
    let Some(seq) = item.as_sequence_mut() else {
        return Err((Some(hkey), "not a sequence".into()));
    };
    seq.video_tracks = video;
    seq.audio_tracks = audio;
    seq.submix_tracks = submix;
    seq.caption_tracks = captions;
    seq.check_bounds().map_err(|m| (Some(hkey.clone()), m))?;
    p.items.insert(sid, item);
    Ok(())
}

/// `base` with `updates` applied. `strict` also requires the structural rules (no overlapping
/// clips); without it only the value bounds are required, which is what an opened file must meet.
pub fn apply_updates(
    base: &Project,
    updates: &Changes,
    strict: bool,
) -> Result<Project, ApplyError> {
    let mut p = base.clone();
    let mut sequences: BTreeSet<ItemId> = BTreeSet::new();
    for (key, value) in updates {
        let at = |m: String| (Some(key.clone()), m);
        match parse_key(key) {
            Some(KeyKind::Meta) => {
                let v = value
                    .as_ref()
                    .ok_or_else(|| at("the project settings cannot be deleted".into()))?;
                apply_meta(&mut p, v).map_err(at)?;
            }
            Some(KeyKind::Item(id)) => match value {
                Some(v) => {
                    let item: ProjectItem = from_value(v).map_err(at)?;
                    if item.id != id || matches!(item.kind, ItemKind::Sequence(_)) {
                        return Err(at("item does not match its key".into()));
                    }
                    p.items.insert(id, item);
                }
                None => {
                    p.items.remove(&id);
                }
            },
            Some(KeyKind::Seq(id)) | Some(KeyKind::Track(id, _, _)) => {
                sequences.insert(id);
            }
            None => log::warn!("collaboration: ignoring unknown key {key:?}"),
        }
    }
    for sid in sequences {
        assemble_sequence(&mut p, sid, updates, strict)?;
    }
    Ok(p)
}

/// A project from a whole shared document.
pub fn doc_to_project(doc: &Doc) -> Result<Project, String> {
    let updates: Changes = doc
        .iter()
        .map(|(k, v)| (k.clone(), Some(v.clone())))
        .collect();
    let mut p = apply_updates(&Project::new("Untitled"), &updates, false).map_err(
        |(key, why)| match key {
            Some(k) => format!("{k}: {why}"),
            None => why,
        },
    )?;
    p.next_id = p.max_id().saturating_add(1);
    Ok(p)
}

// ---- the session's side ----

/// What a session keeps while it takes part in a shared project.
pub struct CollabState {
    pub sync: SyncClient,
    pub session_id: String,
    /// The project as last reported to `sync`.
    seen: Arc<Project>,
    /// `Session::document_epoch` when the session began; another document ends it.
    epoch: u64,
    /// Remote changes not yet installed (a dynamic trim is running and keeps rebuilding the project).
    pending_remote: Changes,
    remote_seq: u64,
    remote_log: VecDeque<(u64, Vec<Key>)>,
    /// History steps (by the address of their snapshot) and `remote_seq` when each was first seen.
    tags: HashMap<usize, u64>,
    /// Leaves where somebody else's value replaced ours, since the session began.
    pub clashes: u64,
}

impl CollabState {
    fn new(sync: SyncClient, session_id: &str, seen: Arc<Project>, epoch: u64) -> Self {
        Self {
            sync,
            session_id: session_id.to_string(),
            seen,
            epoch,
            pending_remote: Changes::new(),
            remote_seq: 0,
            remote_log: VecDeque::new(),
            tags: HashMap::new(),
            clashes: 0,
        }
    }

    /// Keys changed by others after `tag` (`None`: the log no longer reaches back that far).
    fn remote_since(&self, tag: u64) -> Option<BTreeSet<Key>> {
        if tag
            < self
                .remote_log
                .front()
                .map_or(self.remote_seq, |(s, _)| s.saturating_sub(1))
        {
            return None;
        }
        Some(
            self.remote_log
                .iter()
                .filter(|(s, _)| *s > tag)
                .flat_map(|(_, keys)| keys.iter().cloned())
                .collect(),
        )
    }
}

fn describe((key, why): ApplyError) -> String {
    match key {
        Some(k) => format!("{k}: {why}"),
        None => why,
    }
}

impl Session {
    /// Whether this session takes part in a shared project.
    pub fn collab_active(&self) -> bool {
        self.collab.is_some()
    }

    pub fn collab_state(&self) -> Option<&CollabState> {
        self.collab.as_deref()
    }

    /// The open project as a shared document, with the snapshot it was made from (hand that
    /// snapshot to [`Session::collab_attach`] so edits made meanwhile are not lost).
    pub fn collab_export(&self) -> Result<(Doc, Arc<Project>), String> {
        Ok((project_to_doc(&self.project)?, self.project.clone()))
    }

    /// Give new ids from this client's own range.
    fn claim_ids(&mut self, client_id: u64) -> Result<(), String> {
        if client_id == 0 || client_id > MAX_CLIENT_ID {
            return Err(
                "this relay session has handed out too many client ids; start a new session".into(),
            );
        }
        let base = client_id * ID_RANGE;
        let max = self.project.max_id();
        let next = if max >= base && max < base + ID_RANGE {
            max + 1
        } else {
            self.project.next_id.max(base)
        };
        if self.project.next_id != next {
            Arc::make_mut(&mut self.project).next_id = next;
        }
        Ok(())
    }

    /// Start taking part with the project that is already open (this client shared it).
    /// `shared` is the snapshot [`Session::collab_export`] returned.
    pub fn collab_attach(
        &mut self,
        session_id: &str,
        client_id: u64,
        rev: u64,
        state: Doc,
        shared: Arc<Project>,
    ) -> Result<(), String> {
        self.claim_ids(client_id)?;
        let sync = SyncClient::new(client_id, rev, state);
        self.collab = Some(Box::new(CollabState::new(
            sync,
            session_id,
            shared,
            self.document_epoch,
        )));
        Ok(())
    }

    /// Replace the open project with the shared one (this client joined). The caller has made sure
    /// nothing unsaved is lost.
    pub fn collab_join(
        &mut self,
        session_id: &str,
        client_id: u64,
        rev: u64,
        state: Doc,
    ) -> Result<(), String> {
        let project = doc_to_project(&state)?;
        crate::masks::discard_jobs(self);
        crate::commands::install_project(self, project, None, false);
        self.claim_ids(client_id)?;
        let sync = SyncClient::new(client_id, rev, state);
        self.collab = Some(Box::new(CollabState::new(
            sync,
            session_id,
            self.project.clone(),
            self.document_epoch,
        )));
        Ok(())
    }

    /// Stop taking part. The project stays as it is, as a normal local project.
    pub fn collab_detach(&mut self) {
        self.collab = None;
    }

    /// Another document replaced the shared one (open, new, close…): the session ends. True if it did.
    pub fn collab_check_epoch(&mut self) -> bool {
        if self
            .collab
            .as_ref()
            .is_some_and(|c| c.epoch != self.document_epoch)
        {
            self.collab = None;
            return true;
        }
        false
    }

    /// Report what changed in the project since the last call, and tag new undo steps.
    fn collab_observe(&mut self) {
        // a dynamic trim rebuilds the project from a snapshot every tick: wait until it ends
        if self.trim_play.dynamic.is_some() {
            return;
        }
        let Some(st) = self.collab.as_mut() else {
            return;
        };
        if !Arc::ptr_eq(&self.project, &st.seen) {
            match changed_keys(&st.seen, &self.project) {
                Ok(changes) => {
                    for (key, value) in changes {
                        st.sync.local_set(&key, value);
                    }
                    st.seen = self.project.clone();
                }
                Err(e) => log::error!("collaboration: cannot read the project: {e}"),
            }
        }
        let seq = st.remote_seq;
        let live: BTreeSet<usize> = self
            .history
            .undo
            .iter()
            .chain(self.history.redo.iter())
            .map(|e| Arc::as_ptr(&e.1) as usize)
            .collect();
        st.tags.retain(|ptr, _| live.contains(ptr));
        for ptr in live {
            st.tags.entry(ptr).or_insert(seq);
        }
    }

    /// Call once per frame: install deferred remote changes, report local ones, and return what to
    /// send to the relay.
    pub fn collab_pump(&mut self) -> Vec<ClientMsg> {
        self.collab_flush_pending();
        self.collab_observe();
        let mut out = Vec::new();
        if let Some(st) = self.collab.as_mut()
            && let Some(msg) = st.sync.poll_send()
        {
            out.push(msg);
        }
        out
    }

    /// Handle a message from the relay; returns what to send back.
    pub fn collab_on_message(&mut self, msg: ServerMsg) -> Vec<ClientMsg> {
        self.collab_observe();
        if let ServerMsg::OpLockRejected {
            op_id,
            keys,
            resources,
            ..
        } = &msg
        {
            let restore = self
                .collab
                .as_mut()
                .and_then(|st| st.sync.reject_locked(*op_id, keys));
            if let Some(restore) = restore {
                self.collab_install(restore);
                self.error_toast(
                    "collaboration",
                    format!("Edit refused: lock required for {}", resources.join(", ")),
                );
            }
            return self.collab_pump();
        }
        let mut out = Vec::new();
        let mut remote: Option<Remote> = None;
        let new_id = match &msg {
            ServerMsg::Welcome { client_id, .. } | ServerMsg::Catchup { client_id, .. } => {
                Some(*client_id)
            }
            _ => None,
        };
        if let Some(id) = new_id
            && self
                .collab
                .as_ref()
                .is_some_and(|c| c.sync.client_id() != id)
            && let Err(e) = self.claim_ids(id)
        {
            self.error_toast("collaboration", e);
        }
        let Some(st) = self.collab.as_mut() else {
            return out;
        };
        match msg {
            ServerMsg::Welcome {
                client_id,
                rev,
                state,
                ..
            } => {
                st.sync.reconnected(client_id);
                remote = Some(st.sync.on_snapshot(rev, state));
            }
            ServerMsg::Catchup { client_id, ops, .. } => {
                st.sync.reconnected(client_id);
                match st.sync.on_catchup(&ops) {
                    Ok(r) => remote = Some(r),
                    Err(SyncError::Gap { .. }) => out.push(ClientMsg::RequestState),
                }
            }
            other => match st.sync.on_message(&other) {
                Ok(Event::Remote(r)) => remote = Some(r),
                Ok(Event::Rejected { reason }) => log::debug!(
                    "collaboration: operation refused ({reason}); merging and sending again"
                ),
                Ok(Event::Acked | Event::Ignored) => {}
                Err(SyncError::Gap { expected, got }) => {
                    log::warn!(
                        "collaboration: missed revisions (expected {expected}, got {got}); asking for the whole project"
                    );
                    out.push(ClientMsg::RequestState);
                }
            },
        }
        if let Some(r) = remote {
            self.collab_remote(r);
        }
        out.extend(self.collab_pump());
        out
    }

    /// Record and install what another client changed.
    fn collab_remote(&mut self, r: Remote) {
        let Some(st) = self.collab.as_mut() else {
            return;
        };
        if !r.remote_keys.is_empty() {
            st.remote_seq += 1;
            st.remote_log.push_back((st.remote_seq, r.remote_keys));
            while st.remote_log.len() > REMOTE_LOG_LIMIT {
                st.remote_log.pop_front();
            }
        }
        st.clashes += r.conflicts as u64;
        st.pending_remote.extend(r.updates);
        if r.conflicts > 0 {
            self.toast(format!(
                "Simultaneous edits to the same property: {} change(s) of the other user were kept",
                r.conflicts
            ));
        }
        self.collab_flush_pending();
    }

    /// Install the deferred remote changes into the project.
    fn collab_flush_pending(&mut self) {
        if self.trim_play.dynamic.is_some() {
            return;
        }
        let Some(st) = self.collab.as_mut() else {
            return;
        };
        if st.pending_remote.is_empty() {
            return;
        }
        let updates: Changes = std::mem::take(&mut st.pending_remote);
        // Edits the editor has made that were not reported yet are merged with the change, key by key.
        let mut final_updates = Changes::new();
        let mut clashes = 0;
        for (key, theirs) in updates {
            let (engine, seen) = match (value_of(&self.project, &key), value_of(&st.seen, &key)) {
                (Ok(e), Ok(s)) => (e, s),
                _ => {
                    final_updates.insert(key, theirs);
                    continue;
                }
            };
            if engine == seen {
                final_updates.insert(key, theirs);
            } else {
                let merged = merge3(seen.as_ref(), engine.as_ref(), theirs.as_ref());
                clashes += merged.conflicts;
                st.sync.local_set(&key, merged.value.clone());
                final_updates.insert(key, merged.value);
            }
        }
        st.clashes += clashes as u64;
        self.collab_install(final_updates);
    }

    /// Put `updates` into the project (and into `seen`). A key whose merged value breaks a rule of
    /// the project is replaced by the relay's value.
    fn collab_install(&mut self, mut updates: Changes) {
        let Some(st) = self.collab.as_mut() else {
            return;
        };
        let mut strict = true;
        let mut repairs = 0;
        let new_project = loop {
            match apply_updates(&self.project, &updates, strict) {
                Ok(p) => break Some(p),
                Err((Some(key), why)) if repairs < MAX_REPAIRS => {
                    repairs += 1;
                    log::warn!("collaboration: {key}: {why}; using the other user's version");
                    let theirs = st.sync.force_theirs(&key);
                    st.clashes += 1;
                    updates.insert(key, theirs);
                }
                Err(e) if strict => {
                    log::warn!(
                        "collaboration: {}; accepting the project as the relay has it",
                        describe(e)
                    );
                    strict = false;
                }
                Err(e) => {
                    let why = describe(e);
                    log::error!("collaboration: cannot install remote changes: {why}");
                    break None;
                }
            }
        };
        let Some(project) = new_project else {
            self.error_toast("collaboration", "Could not apply a change from another user; leave and rejoin the session to resynchronize");
            return;
        };
        let new_seen = apply_updates(&st.seen, &updates, false)
            .map(Arc::new)
            .unwrap_or_else(|_| Arc::new(project.clone()));
        let cycle = project.nest_cycle().is_some() && self.project.nest_cycle().is_none();
        self.project = Arc::new(project);
        st.seen = new_seen;
        self.media.add_stored(&self.project, self.path.as_deref());
        self.fix_state();
        self.bump();
        self.previews.live.publish_project(self.project.clone());
        self.sync_persistence();
        if cycle {
            self.error_toast("collaboration", "Simultaneous edits nested two sequences inside each other; remove one of the nests");
        }
    }

    /// Selective restore of `target` (an older snapshot of this project) onto the current project:
    /// only the parts that differ and that nobody else changed since `tag`. Returns the project and
    /// how many differing parts were left alone because somebody else changed them.
    fn collab_restore(
        &self,
        target: &Project,
        tag: Option<u64>,
    ) -> Result<(Project, usize), String> {
        let st = self.collab.as_ref().ok_or("not in a session")?;
        let differing = changed_keys(&self.project, target)?;
        let others: BTreeSet<Key> = match tag.and_then(|t| st.remote_since(t)) {
            Some(keys) => keys,
            None => differing.keys().cloned().collect(),
        };
        let total = differing.len();
        let mine: Changes = differing
            .into_iter()
            .filter(|(k, _)| !others.contains(k))
            .collect();
        let skipped = total - mine.len();
        let restored = apply_updates(&self.project, &mine, false).map_err(describe)?;
        Ok((restored, skipped))
    }

    fn collab_tag(&self, snapshot: &Arc<Project>) -> Option<u64> {
        self.collab
            .as_ref()
            .and_then(|c| c.tags.get(&(Arc::as_ptr(snapshot) as usize)).copied())
    }

    fn collab_restored_toast(&mut self, what: &str, skipped: usize) {
        if skipped > 0 {
            self.toast(format!("{what}: {skipped} part(s) were changed by another user since and were left as they are"));
        }
    }

    /// `Session::undo` while a session is active.
    pub(crate) fn collab_undo(&mut self) -> Option<String> {
        self.collab_observe();
        let (label, prev, origin) = self.history.undo.pop()?;
        let tag = self.collab_tag(&prev);
        match self.collab_restore(&prev, tag) {
            Ok((restored, skipped)) => {
                self.collab_restored_toast("Undo", skipped);
                let current = std::mem::replace(&mut self.project, Arc::new(restored));
                self.history.redo.push((label.clone(), current, origin));
                self.history.merge_key = None;
                self.fix_state();
                self.bump();
                Some(label)
            }
            Err(e) => {
                self.history.undo.push((label, prev, origin));
                self.error_toast("collaboration", format!("Cannot undo while sharing: {e}"));
                None
            }
        }
    }

    /// `Session::redo` while a session is active.
    pub(crate) fn collab_redo(&mut self) -> Option<String> {
        self.collab_observe();
        let (label, next, origin) = self.history.redo.pop()?;
        let tag = self.collab_tag(&next);
        match self.collab_restore(&next, tag) {
            Ok((restored, skipped)) => {
                self.collab_restored_toast("Redo", skipped);
                let current = std::mem::replace(&mut self.project, Arc::new(restored));
                self.history.undo.push((label.clone(), current, origin));
                self.history.merge_key = None;
                self.fix_state();
                self.bump();
                Some(label)
            }
            Err(e) => {
                self.history.redo.push((label, next, origin));
                self.error_toast("collaboration", format!("Cannot redo while sharing: {e}"));
                None
            }
        }
    }

    /// `Session::revert_to` while a session is active: `earliest` is the snapshot to go back to.
    pub(crate) fn collab_revert(&mut self, earliest: Arc<Project>) {
        let tag = self.collab_tag(&earliest);
        match self.collab_restore(&earliest, tag) {
            Ok((restored, skipped)) => {
                self.collab_restored_toast("Cancel", skipped);
                self.project = Arc::new(restored);
            }
            Err(e) => {
                self.error_toast("collaboration", format!("Cannot cancel while sharing: {e}"))
            }
        }
    }
}
