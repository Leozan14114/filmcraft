//! Two independent clients against a real relay over TCP.
#![cfg(feature = "net")]

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use filmcraft_collab::conn::{Connection, NetEvent};
use filmcraft_collab::{ClientMsg, Doc, ObjectLock, PROTOCOL_VERSION, ServerMsg, SyncClient};
use serde_json::{Value, json};

fn start_relay() -> SocketAddr {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            tx.send(listener.local_addr().unwrap()).unwrap();
            let _ = filmcraft_collab::server::serve(listener).await;
        });
    });
    rx.recv().unwrap()
}

struct Peer {
    conn: Connection,
    sync: Option<SyncClient>,
    users: usize,
    last_error: Option<String>,
    host_id: Option<u64>,
    timeline_locking: bool,
    timeline_lock_owner: Option<u64>,
    object_locks: Vec<ObjectLock>,
    lock_denied: Option<String>,
    lock_rejected: Option<Vec<String>>,
    closed: bool,
}

impl Peer {
    fn open(addr: SocketAddr, hello: ClientMsg) -> Peer {
        let conn = Connection::open(&format!("ws://{addr}"), None).unwrap();
        conn.send(hello);
        Peer {
            conn, sync: None, users: 0, last_error: None,
            host_id: None, timeline_locking: true, timeline_lock_owner: None,
            object_locks: Vec::new(), lock_denied: None, lock_rejected: None, closed: false,
        }
    }

    /// Read what arrived and send what is due; true if anything happened.
    fn pump(&mut self) -> bool {
        let mut did = false;
        while let Some(ev) = self.conn.try_recv() {
            did = true;
            match ev {
                NetEvent::Connected => {}
                NetEvent::Closed(_) => self.closed = true,
                NetEvent::Message(ServerMsg::Welcome { client_id, rev, state, users, .. }) => {
                    self.users = users.len();
                    match &mut self.sync {
                        Some(s) => {
                            s.reconnected(client_id);
                            s.on_snapshot(rev, state);
                        }
                        None => self.sync = Some(SyncClient::new(client_id, rev, state)),
                    }
                }
                NetEvent::Message(ServerMsg::Catchup { client_id, ops, users, .. }) => {
                    self.users = users.len();
                    if let Some(s) = &mut self.sync {
                        s.reconnected(client_id);
                        s.on_catchup(&ops).unwrap();
                    }
                }
                NetEvent::Message(ServerMsg::UserJoin { .. }) => self.users += 1,
                NetEvent::Message(ServerMsg::UserLeave { .. }) => self.users = self.users.saturating_sub(1),
                NetEvent::Message(ServerMsg::Error { message, .. }) => self.last_error = Some(message),
                NetEvent::Message(ServerMsg::LockState { host_id, timeline_locking, timeline_owner, object_locks }) => {
                    self.host_id = host_id;
                    self.timeline_locking = timeline_locking;
                    self.timeline_lock_owner = timeline_owner;
                    self.object_locks = object_locks;
                }
                NetEvent::Message(ServerMsg::LockDenied { message, .. }) => self.lock_denied = Some(message),
                NetEvent::Message(ServerMsg::OpLockRejected { op_id, keys, resources, .. }) => {
                    self.lock_rejected = Some(resources);
                    if let Some(sync) = &mut self.sync {
                        let _ = sync.reject_locked(op_id, &keys);
                    }
                },
                NetEvent::Message(other) => {
                    if let Some(s) = &mut self.sync {
                        s.on_message(&other).unwrap();
                    }
                }
            }
        }
        if let Some(s) = &mut self.sync
            && let Some(op) = s.poll_send()
        {
            self.conn.send(op);
            did = true;
        }
        did
    }
}

fn wait_until(peers: &mut [&mut Peer], what: &str, mut done: impl FnMut(&[&mut Peer]) -> bool) {
    let start = Instant::now();
    loop {
        for p in peers.iter_mut() {
            p.pump();
        }
        if done(peers) {
            return;
        }
        assert!(start.elapsed() < Duration::from_secs(10), "timed out waiting for: {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn project() -> Doc {
    let mut d = Doc::new();
    d.insert("meta".into(), json!({"name": "Film"}));
    d.insert("track/1/v/1".into(), json!({"id": 1, "items": [{"id": 10, "start": 0}]}));
    d
}

fn create(session: &str) -> ClientMsg {
    ClientMsg::Create { protocol: PROTOCOL_VERSION, session: session.into(), name: "Ann".into(), token: None, state: project() }
}

fn join(session: &str, name: &str, token: Option<&str>, last_rev: Option<u64>) -> ClientMsg {
    ClientMsg::Join { protocol: PROTOCOL_VERSION, session: session.into(), name: name.into(), token: token.map(str::to_string), last_rev }
}

fn track(p: &Peer) -> Value {
    p.sync.as_ref().and_then(|s| s.local().get("track/1/v/1").cloned()).unwrap_or(Value::Null)
}

#[test]
fn two_clients_share_a_project_and_edit_in_both_directions() {
    let addr = start_relay();
    let mut a = Peer::open(addr, create("film"));
    wait_until(&mut [&mut a], "A welcomed", |p| p[0].sync.is_some());
    let mut b = Peer::open(addr, join("film", "Bob", None, None));
    wait_until(&mut [&mut a, &mut b], "B receives the initial state", |p| p[1].sync.is_some() && p[0].users == 2);
    assert_eq!(b.sync.as_ref().unwrap().local(), &project(), "initial sync is lossless");
    assert_ne!(a.sync.as_ref().unwrap().client_id(), b.sync.as_ref().unwrap().client_id());

    // Exercise the normal two-way workflow in per-object mode: A reserves the new clip,
    // while B reserves the existing clip. Both collaborators can then edit distinct objects.
    let a_id = a.sync.as_ref().unwrap().client_id();
    let b_id = b.sync.as_ref().unwrap().client_id();
    a.conn.send(ClientMsg::SetTimelineLocking { enabled: false });
    wait_until(&mut [&mut a, &mut b], "object locking enabled", |p| p.iter().all(|x| !x.timeline_locking));
    a.conn.send(ClientMsg::SetObjectLocks { object_ids: vec!["clip:1:11".into()] });
    b.conn.send(ClientMsg::SetObjectLocks { object_ids: vec!["clip:1:10".into()] });
    wait_until(&mut [&mut a, &mut b], "both objects reserved", |p| {
        p.iter().all(|x| x.object_locks.iter().any(|l| l.object_id == "clip:1:11" && l.client_id == a_id)
            && x.object_locks.iter().any(|l| l.object_id == "clip:1:10" && l.client_id == b_id))
    });

    // A -> B
    let mut t = track(&a);
    t["items"].as_array_mut().unwrap().push(json!({"id": 11, "start": 40}));
    a.sync.as_mut().unwrap().local_set("track/1/v/1", Some(t.clone()));
    wait_until(&mut [&mut a, &mut b], "B sees A's clip", |p| track(p[1]) == t);

    // B -> A
    let mut t2 = track(&b);
    t2["items"][0]["start"] = json!(3);
    b.sync.as_mut().unwrap().local_set("track/1/v/1", Some(t2.clone()));
    wait_until(&mut [&mut a, &mut b], "A sees B's move", |p| track(p[0]) == t2);

    // everything settles, and the remote changes were not sent back
    wait_until(&mut [&mut a, &mut b], "both idle", |p| p.iter().all(|x| x.sync.as_ref().is_some_and(SyncClient::is_idle)));
    let (ra, rb) = (a.sync.as_ref().unwrap().rev(), b.sync.as_ref().unwrap().rev());
    assert_eq!(ra, rb);
    assert_eq!(ra, 3, "initial state is revision 1; exactly two operations were accepted");
    std::thread::sleep(Duration::from_millis(200));
    a.pump();
    b.pump();
    assert_eq!(a.sync.as_ref().unwrap().rev(), 3, "no echo loop: no further operations appeared");
}

#[test]
fn simultaneous_edits_to_the_same_track_are_merged() {
    let addr = start_relay();
    let mut a = Peer::open(addr, create("merge"));
    wait_until(&mut [&mut a], "A welcomed", |p| p[0].sync.is_some());
    let mut b = Peer::open(addr, join("merge", "Bob", None, None));
    wait_until(&mut [&mut a, &mut b], "B joined", |p| p[1].sync.is_some());

    // Concurrent edits to different clip IDs in the same track are allowed in object mode.
    // This leaves the revision/merge path under test without bypassing the lock policy.
    let a_id = a.sync.as_ref().unwrap().client_id();
    let b_id = b.sync.as_ref().unwrap().client_id();
    a.conn.send(ClientMsg::SetTimelineLocking { enabled: false });
    wait_until(&mut [&mut a, &mut b], "object locking enabled", |p| p.iter().all(|x| !x.timeline_locking));
    a.conn.send(ClientMsg::SetObjectLocks { object_ids: vec!["clip:1:12".into()] });
    b.conn.send(ClientMsg::SetObjectLocks { object_ids: vec!["clip:1:10".into()] });
    wait_until(&mut [&mut a, &mut b], "both objects reserved", |p| {
        p.iter().all(|x| x.object_locks.iter().any(|l| l.object_id == "clip:1:12" && l.client_id == a_id)
            && x.object_locks.iter().any(|l| l.object_id == "clip:1:10" && l.client_id == b_id))
    });

    let mut ta = track(&a);
    ta["items"].as_array_mut().unwrap().push(json!({"id": 12, "start": 90}));
    let mut tb = track(&b);
    tb["items"][0]["start"] = json!(5);
    // both edit before either hears of the other
    a.sync.as_mut().unwrap().local_set("track/1/v/1", Some(ta));
    b.sync.as_mut().unwrap().local_set("track/1/v/1", Some(tb));
    wait_until(&mut [&mut a, &mut b], "converged", |p| {
        p.iter().all(|x| x.sync.as_ref().is_some_and(SyncClient::is_idle)) && track(p[0]) == track(p[1]) && track(p[0])["items"].as_array().is_some_and(|i| i.len() == 2)
    });
    let t = track(&a);
    assert_eq!(t["items"][0]["start"], json!(5), "B's move survived");
    assert_eq!(t["items"][1]["id"], json!(12), "A's new clip survived");
}

#[test]
fn a_wrong_password_and_an_unknown_session_are_refused() {
    let addr = start_relay();
    let hello = ClientMsg::Create { protocol: PROTOCOL_VERSION, session: "private".into(), name: "Ann".into(), token: Some("s3cret".into()), state: project() };
    let mut a = Peer::open(addr, hello);
    wait_until(&mut [&mut a], "A welcomed", |p| p[0].sync.is_some());
    let mut bad = Peer::open(addr, join("private", "Eve", Some("nope"), None));
    wait_until(&mut [&mut bad], "refused", |p| p[0].last_error.is_some());
    assert!(bad.last_error.as_ref().unwrap().contains("password"));
    assert!(bad.sync.is_none());
    let mut missing = Peer::open(addr, join("nothing-here", "Eve", None, None));
    wait_until(&mut [&mut missing], "unknown", |p| p[0].last_error.is_some());
    let mut dup = Peer::open(addr, ClientMsg::Create { protocol: PROTOCOL_VERSION, session: "private".into(), name: "X".into(), token: None, state: project() });
    wait_until(&mut [&mut dup], "exists", |p| p[0].last_error.is_some());
}

#[test]
fn a_reconnecting_client_catches_up_and_keeps_its_unsent_edit() {
    let addr = start_relay();
    let mut a = Peer::open(addr, create("recon"));
    wait_until(&mut [&mut a], "A welcomed", |p| p[0].sync.is_some());
    let mut b = Peer::open(addr, join("recon", "Bob", None, None));
    wait_until(&mut [&mut a, &mut b], "B joined", |p| p[1].sync.is_some());
    let b_rev = b.sync.as_ref().unwrap().rev();

    // A keeps the timeline lock while B is offline and makes the two authoritative edits.
    let a_id = a.sync.as_ref().unwrap().client_id();
    a.conn.send(ClientMsg::AcquireTimelineLock);
    wait_until(&mut [&mut a, &mut b], "A owns timeline lock", |p| p.iter().all(|x| x.timeline_lock_owner == Some(a_id)));

    // B goes away; meanwhile A edits twice
    b.conn.send(ClientMsg::Leave);
    wait_until(&mut [&mut a, &mut b], "B left", |p| p[1].closed);
    for start in [20, 30] {
        let mut t = track(&a);
        t["items"][0]["start"] = json!(start);
        a.sync.as_mut().unwrap().local_set("track/1/v/1", Some(t));
        wait_until(&mut [&mut a], "A idle", |p| p[0].sync.as_ref().is_some_and(SyncClient::is_idle));
    }
    // B made an edit to another part while offline
    b.sync.as_mut().unwrap().local_set("meta", Some(json!({"name": "Film (B's cut)"})));

    let mut b2 = Peer {
        conn: Connection::open(&format!("ws://{addr}"), None).unwrap(),
        sync: b.sync.take(), users: 0, last_error: None,
        host_id: None, timeline_locking: true, timeline_lock_owner: None,
        object_locks: Vec::new(), lock_denied: None, lock_rejected: None, closed: false,
    };
    b2.conn.send(join("recon", "Bob", None, Some(b_rev)));
    let bs_cut = json!({"name": "Film (B's cut)"});
    wait_until(&mut [&mut a, &mut b2], "caught up and synced", |p| {
        p.iter().all(|x| x.sync.as_ref().is_some_and(SyncClient::is_idle))
            && track(p[1])["items"][0]["start"] == json!(30)
            && p[0].sync.as_ref().and_then(|s| s.local().get("meta")) == Some(&bs_cut)
    });
    assert_eq!(a.sync.as_ref().unwrap().local().get("meta"), Some(&bs_cut), "B's offline edit reached A");
}


#[test]
fn relay_enforces_host_timeline_mode_and_first_come_locks() {
    let addr = start_relay();
    let mut a = Peer::open(addr, create("locks"));
    wait_until(&mut [&mut a], "host welcomed", |p| p[0].sync.is_some() && p[0].host_id.is_some());
    let a_id = a.sync.as_ref().unwrap().client_id();

    let mut b = Peer::open(addr, join("locks", "Bob", None, None));
    wait_until(&mut [&mut a, &mut b], "second user joined", |p| p[1].sync.is_some() && p[1].host_id == Some(a_id));
    let b_id = b.sync.as_ref().unwrap().client_id();

    // Only the first participant (the host) may switch from timeline to object locking.
    b.conn.send(ClientMsg::SetTimelineLocking { enabled: false });
    wait_until(&mut [&mut b], "non-host mode change refused", |p| p[0].lock_denied.as_deref().is_some_and(|m| m.contains("only the session host")));
    b.lock_denied = None;

    a.conn.send(ClientMsg::SetTimelineLocking { enabled: false });
    wait_until(&mut [&mut a, &mut b], "object mode enabled", |p| p.iter().all(|x| !x.timeline_locking));

    a.conn.send(ClientMsg::SetObjectLocks { object_ids: vec!["clip:1:10".into()] });
    wait_until(&mut [&mut a, &mut b], "first object reserved", |p| p.iter().all(|x| x.object_locks.iter().any(|l| l.object_id == "clip:1:10" && l.client_id == a_id)));

    b.conn.send(ClientMsg::SetObjectLocks { object_ids: vec!["clip:1:10".into()] });
    wait_until(&mut [&mut b], "duplicate object reservation refused", |p| p[0].lock_denied.is_some());
    b.conn.send(ClientMsg::Op {
        op_id: 77,
        base_rev: 1,
        changes: [("track/1/v/1".to_string(), Some(json!({"id": 1, "items": [{"id": 10, "start": 99}]})))].into(),
    });
    wait_until(&mut [&mut b], "edit without its object lock rejected", |p| p[0].lock_rejected.as_deref().is_some_and(|r| r.len() == 1 && r[0] == "clip:1:10"));

    // Switching back clears object reservations; every timeline edit now needs the one global lock.
    a.conn.send(ClientMsg::SetTimelineLocking { enabled: true });
    wait_until(&mut [&mut a, &mut b], "timeline mode restored", |p| p.iter().all(|x| x.timeline_locking && x.object_locks.is_empty()));
    b.conn.send(ClientMsg::AcquireTimelineLock);
    wait_until(&mut [&mut a, &mut b], "B acquires first", |p| p.iter().all(|x| x.timeline_lock_owner == Some(b_id)));
    a.conn.send(ClientMsg::AcquireTimelineLock);
    wait_until(&mut [&mut a], "A cannot take B's lock", |p| p[0].lock_denied.is_some());
    assert_eq!(a.timeline_lock_owner, Some(b_id));
}
