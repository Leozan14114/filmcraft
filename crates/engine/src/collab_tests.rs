//! Collaboration: the project ↔ shared document mapping, and two sessions converging through the
//! relay's sequencer (`filmcraft_collab::Room`), without any network.

use std::collections::VecDeque;

use filmcraft_collab::{ClientMsg, Reject, Room, ServerMsg};
use serde_json::json;

use crate::Session;
use crate::collab::{changed_keys, doc_to_project, project_to_doc};

fn demo() -> Session {
    let mut s = Session::default();
    s.execute("file.openDemoProject", json!({})).unwrap();
    s
}

fn rename_track(s: &mut Session, audio: bool, name: &str) {
    let id = s.state.active_sequence.unwrap();
    s.edit("rename track", |p, _| {
        let seq = p.sequence_mut(id).unwrap();
        let t = if audio {
            &mut seq.audio_tracks[0]
        } else {
            &mut seq.video_tracks[0]
        };
        t.name = name.to_string();
        Ok(())
    })
    .unwrap();
}

fn track_names(s: &Session) -> (String, String) {
    let q = s.active_sequence().unwrap();
    (
        q.video_tracks[0].name.clone(),
        q.audio_tracks[0].name.clone(),
    )
}

/// Two sessions that share the demo project through a `Room`.
struct Pair {
    room: Room,
    a: Session,
    b: Session,
    ids: [u64; 2],
}

impl Pair {
    fn new() -> Pair {
        let mut a = demo();
        let (doc, shared) = a.collab_export().unwrap();
        let mut room = Room::new(doc.clone(), None);
        let (ua, ub) = (room.add_user("A").unwrap(), room.add_user("B").unwrap());
        a.collab_attach("t", ua.client_id, 1, doc.clone(), shared)
            .unwrap();
        let mut b = Session::default();
        b.collab_join("t", ub.client_id, 1, doc).unwrap();
        Pair {
            room,
            a,
            b,
            ids: [ua.client_id, ub.client_id],
        }
    }

    fn session(&mut self, i: usize) -> &mut Session {
        if i == 0 { &mut self.a } else { &mut self.b }
    }

    /// Run the protocol until nothing is left to send.
    fn settle(&mut self) {
        let mut outbox: VecDeque<(usize, ClientMsg)> = VecDeque::new();
        for _ in 0..40 {
            for i in 0..2 {
                for m in self.session(i).collab_pump() {
                    outbox.push_back((i, m));
                }
            }
            if outbox.is_empty() {
                return;
            }
            while let Some((from, msg)) = outbox.pop_front() {
                let ClientMsg::Op {
                    op_id,
                    base_rev,
                    changes,
                } = msg
                else {
                    continue;
                };
                match self.room.apply(self.ids[from], op_id, base_rev, changes) {
                    Ok(op) => {
                        let applied = ServerMsg::Applied {
                            rev: op.rev,
                            client_id: op.client_id,
                            op_id: op.op_id,
                            changes: op.changes,
                        };
                        for i in 0..2 {
                            for m in self.session(i).collab_on_message(applied.clone()) {
                                outbox.push_back((i, m));
                            }
                        }
                    }
                    Err(Reject::Stale(keys)) => {
                        let rejected = ServerMsg::Rejected {
                            op_id,
                            rev: self.room.rev(),
                            reason: "stale".into(),
                            keys,
                        };
                        for m in self.session(from).collab_on_message(rejected) {
                            outbox.push_back((from, m));
                        }
                    }
                    Err(Reject::Invalid(why)) => panic!("{why}"),
                    Err(Reject::TimelineLocked(_)) | Err(Reject::Locked(_)) => {
                        panic!("lock check should not run in this test harness")
                    }
                }
            }
        }
        panic!("the sessions did not settle");
    }

    fn assert_converged(&self) {
        let (da, db) = (
            project_to_doc(&self.a.project).unwrap(),
            project_to_doc(&self.b.project).unwrap(),
        );
        assert_eq!(da, db, "both editors hold the same project");
        assert_eq!(&da, self.room.state(), "and it is what the relay holds");
    }
}

#[test]
fn the_demo_project_round_trips_through_the_shared_document() {
    let s = demo();
    let doc = project_to_doc(&s.project).unwrap();
    let back = doc_to_project(&doc).unwrap();
    assert_eq!(back.items, s.project.items);
    assert_eq!(project_to_doc(&back).unwrap(), doc);
}

#[test]
fn an_edit_reports_only_the_track_it_changed() {
    let mut s = demo();
    let before = s.project.clone();
    assert!(
        changed_keys(&before, &s.project).unwrap().is_empty(),
        "nothing changed, nothing to report"
    );
    rename_track(&mut s, false, "Picture");
    let changes = changed_keys(&before, &s.project).unwrap();
    assert_eq!(changes.len(), 1, "{:?}", changes.keys().collect::<Vec<_>>());
    assert!(changes.keys().next().unwrap().starts_with("track/"));
}

#[test]
fn selection_and_playhead_do_not_touch_the_project() {
    let mut s = demo();
    let before = s.project.clone();
    s.set_playhead(filmcraft_time::Tick(1234));
    s.state.selection = vec![];
    assert!(std::sync::Arc::ptr_eq(&before, &s.project));
}

#[test]
fn a_joined_session_gets_the_project_and_its_own_id_range() {
    let p = Pair::new();
    p.assert_converged();
    let (ia, ib) = (p.ids[0] << 40, p.ids[1] << 40);
    assert!(p.a.project.next_id >= ia && p.b.project.next_id >= ib);
    assert_ne!(p.a.project.next_id >> 40, p.b.project.next_id >> 40);
}

#[test]
fn edits_travel_in_both_directions_and_are_not_echoed() {
    let mut p = Pair::new();
    rename_track(&mut p.a, false, "Picture");
    p.settle();
    assert_eq!(track_names(&p.b).0, "Picture");
    rename_track(&mut p.b, true, "Dialogue");
    p.settle();
    assert_eq!(track_names(&p.a).1, "Dialogue");
    p.assert_converged();
    assert_eq!(
        p.room.rev(),
        3,
        "initial state plus exactly the two edits: remote changes were not sent back"
    );
    assert!(p.a.collab_pump().is_empty() && p.b.collab_pump().is_empty());
}

#[test]
fn simultaneous_edits_to_different_parts_both_survive() {
    let mut p = Pair::new();
    rename_track(&mut p.a, false, "Picture");
    rename_track(&mut p.b, true, "Dialogue");
    p.settle();
    p.assert_converged();
    assert_eq!(
        track_names(&p.a),
        ("Picture".to_string(), "Dialogue".to_string())
    );
}

#[test]
fn simultaneous_edits_to_the_same_property_converge_and_are_reported() {
    let mut p = Pair::new();
    rename_track(&mut p.a, false, "Mine");
    rename_track(&mut p.b, false, "Theirs");
    p.settle();
    p.assert_converged();
    assert_eq!(track_names(&p.a).0, track_names(&p.b).0);
    let clashes = p.a.collab_state().unwrap().clashes + p.b.collab_state().unwrap().clashes;
    assert_eq!(
        clashes, 1,
        "somebody's value replaced somebody's, and it was counted"
    );
}

#[test]
fn undo_takes_back_my_edit_but_not_somebody_elses() {
    let mut p = Pair::new();
    let original = track_names(&p.a);
    rename_track(&mut p.a, false, "Picture");
    rename_track(&mut p.b, true, "Dialogue");
    p.settle();
    assert_eq!(p.a.undo().as_deref(), Some("rename track"));
    p.settle();
    p.assert_converged();
    assert_eq!(
        track_names(&p.a),
        (original.0, "Dialogue".to_string()),
        "my rename is undone, B's stays"
    );
    assert_eq!(track_names(&p.b).1, "Dialogue");
    assert!(p.a.redo().is_some());
    p.settle();
    assert_eq!(
        track_names(&p.b),
        ("Picture".to_string(), "Dialogue".to_string())
    );
}

#[test]
fn undo_leaves_a_part_somebody_else_changed_afterwards() {
    let mut p = Pair::new();
    let original = track_names(&p.a).0;
    rename_track(&mut p.a, false, "Picture");
    p.settle();
    rename_track(&mut p.b, false, "Picture (B)");
    p.settle();
    p.a.undo();
    p.settle();
    p.assert_converged();
    assert_ne!(
        track_names(&p.a).0,
        original,
        "B built on my change; undoing it must not wipe B's"
    );
    assert_eq!(track_names(&p.a).0, "Picture (B)");
}

#[test]
fn leaving_ends_the_session_and_another_document_does_too() {
    let mut p = Pair::new();
    assert!(p.a.collab_active());
    p.a.execute("file.openDemoProject", json!({})).unwrap();
    assert!(p.a.collab_check_epoch());
    assert!(!p.a.collab_active());
    p.b.collab_detach();
    assert!(p.b.collab_pump().is_empty());
}
