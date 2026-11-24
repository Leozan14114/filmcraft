//! The relay: a WebSocket server that hosts sessions ([`crate::room::Room`]) in memory.
//!
//! Every message that changes a room is handled under one lock, and each client has its own queue,
//! so every client sees the operations in the same order as the relay decided. A client whose queue
//! fills up (it cannot keep up) is disconnected; it reconnects and catches up.
//!
//! The relay keeps no files: a session lives until the relay restarts. Clients keep their own copy
//! of the project, and one of them can share it again.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::{accept_async, tungstenite::Message};

use crate::proto::{ClientMsg, PROTOCOL_VERSION, ServerMsg, UserInfo, valid_session_id};
use crate::room::{Reject, Room};

/// Messages queued for one client before it is dropped as too slow.
const CLIENT_QUEUE: usize = 4096;
/// How long a new connection has to say who it is.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
/// Most sessions the relay hosts at once.
const MAX_SESSIONS: usize = 256;
/// Largest presence payload relayed.
const MAX_PRESENCE_BYTES: usize = 4096;

struct Hosted {
    room: Room,
    clients: HashMap<u64, mpsc::Sender<ServerMsg>>,
}

impl Hosted {
    /// Queue `msg` for `to`; a client that cannot take it is removed (returns its id).
    fn send(&mut self, to: u64, msg: &ServerMsg) -> Option<u64> {
        let full = match self.clients.get(&to) {
            Some(tx) => tx.try_send(msg.clone()).is_err(),
            None => false,
        };
        if full {
            self.drop_client(to);
            return Some(to);
        }
        None
    }

    fn drop_client(&mut self, id: u64) {
        self.clients.remove(&id);
        self.room.remove_user(id);
    }

    fn broadcast_lock_state(&mut self) {
        let (host_id, timeline_locking, timeline_owner, object_locks) = self.room.lock_state();
        self.broadcast(&ServerMsg::LockState { host_id, timeline_locking, timeline_owner, object_locks }, None);
    }

    /// Queue `msg` for everyone (but `except`), then tell the rest about anybody who was dropped.
    fn broadcast(&mut self, msg: &ServerMsg, except: Option<u64>) {
        let ids: Vec<u64> = self.clients.keys().copied().filter(|id| Some(*id) != except).collect();
        let mut dropped = Vec::new();
        for id in ids {
            if let Some(d) = self.send(id, msg) {
                dropped.push(d);
            }
        }
        let had_dropped = !dropped.is_empty();
        for id in dropped {
            let leave = ServerMsg::UserLeave { client_id: id };
            let rest: Vec<u64> = self.clients.keys().copied().collect();
            for r in rest {
                let _ = self.send(r, &leave);
            }
        }
        if had_dropped {
            let (host_id, timeline_locking, timeline_owner, object_locks) = self.room.lock_state();
            let locks = ServerMsg::LockState { host_id, timeline_locking, timeline_owner, object_locks };
            let rest: Vec<u64> = self.clients.keys().copied().collect();
            for r in rest { let _ = self.send(r, &locks); }
        }
    }
}

type Hub = Arc<Mutex<HashMap<String, Hosted>>>;

fn lock(hub: &Hub) -> std::sync::MutexGuard<'_, HashMap<String, Hosted>> {
    hub.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Listen on `addr` until the process ends.
pub async fn run(addr: SocketAddr) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    serve(listener).await
}

/// Serve on an already bound listener (tests bind port 0 and read the address first).
pub async fn serve(listener: TcpListener) -> std::io::Result<()> {
    let hub: Hub = Arc::new(Mutex::new(HashMap::new()));
    loop {
        let (stream, remote) = listener.accept().await?;
        let hub = hub.clone();
        tokio::spawn(async move {
            if let Err(error) = handle(stream, hub).await {
                log::debug!("client {remote}: {error}");
            }
        });
    }
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

fn text(msg: &ServerMsg) -> Option<Message> {
    serde_json::to_string(msg).ok().map(|s| Message::Text(s.into()))
}

struct Joined {
    session: String,
    client_id: u64,
    rx: mpsc::Receiver<ServerMsg>,
}

/// Run the handshake under the lock. On success the client is registered and its first message
/// (`Welcome` or `Catchup`) is already queued.
fn admit(hub: &Hub, first: ClientMsg) -> Result<Joined, String> {
    let (create, session, name, token, last_rev, state) = match first {
        ClientMsg::Create { protocol, session, name, token, state } => (true, (protocol, session), name, token, None, Some(state)),
        ClientMsg::Join { protocol, session, name, token, last_rev } => (false, (protocol, session), name, token, last_rev, None),
        _ => return Err("the first message must be Create or Join".into()),
    };
    let (protocol, session) = session;
    if protocol != PROTOCOL_VERSION {
        return Err(format!("protocol version {protocol} is not supported (this relay speaks {PROTOCOL_VERSION})"));
    }
    if !valid_session_id(&session) {
        return Err("invalid session id (use letters, digits, '-', '_' and '.', up to 64 characters)".into());
    }
    let (tx, rx) = mpsc::channel::<ServerMsg>(CLIENT_QUEUE);
    let mut hosted = lock(hub);
    if create {
        if hosted.contains_key(&session) {
            return Err(format!("session {session:?} already exists; join it or choose another id"));
        }
        if hosted.len() >= MAX_SESSIONS {
            return Err("the relay hosts too many sessions".into());
        }
        let state = state.unwrap_or_default();
        Room::validate_initial(&state)?;
        let room_token = token.clone().filter(|t| !t.is_empty());
        hosted.insert(session.clone(), Hosted { room: Room::new(state, room_token), clients: HashMap::new() });
    }
    let Some(h) = hosted.get_mut(&session) else {
        return Err(format!("session {session:?} does not exist"));
    };
    if !h.room.token_ok(token.as_deref()) {
        return Err("wrong session password".into());
    }
    let Some(user) = h.room.add_user(&name) else {
        return Err("the session is full".into());
    };
    let users: Vec<UserInfo> = h.room.users();
    let rev = h.room.rev();
    let reply = match last_rev.and_then(|r| h.room.catchup(r)) {
        Some(ops) => ServerMsg::Catchup { session: session.clone(), client_id: user.client_id, rev, ops, users },
        None => ServerMsg::Welcome { session: session.clone(), client_id: user.client_id, rev, state: h.room.state().clone(), users },
    };
    // a fresh channel is empty, so this cannot fail
    let _ = tx.try_send(reply);
    h.clients.insert(user.client_id, tx);
    h.broadcast(&ServerMsg::UserJoin { user: user.clone() }, Some(user.client_id));
    h.broadcast_lock_state();
    Ok(Joined { session, client_id: user.client_id, rx })
}

fn leave(hub: &Hub, session: &str, client_id: u64) {
    let mut hosted = lock(hub);
    if let Some(h) = hosted.get_mut(session)
        && h.clients.contains_key(&client_id)
    {
        h.drop_client(client_id);
        h.broadcast(&ServerMsg::UserLeave { client_id }, None);
        h.broadcast_lock_state();
    }
}

/// Handle one message after the handshake. Returns false when the client asked to leave.
fn on_client_message(hub: &Hub, session: &str, client_id: u64, msg: ClientMsg) -> bool {
    let mut hosted = lock(hub);
    let Some(h) = hosted.get_mut(session) else { return false };
    if !h.clients.contains_key(&client_id) {
        return false;
    }
    match msg {
        ClientMsg::Op { op_id, base_rev, changes } => {
            let op_keys: Vec<String> = changes.keys().cloned().collect();
            match h.room.apply_locked(client_id, op_id, base_rev, changes) {
                Ok(op) => {
                    h.broadcast(
                        &ServerMsg::Applied {
                            rev: op.rev,
                            client_id: op.client_id,
                            op_id: op.op_id,
                            changes: op.changes,
                        },
                        None,
                    );
                }
                Err(Reject::TimelineLocked(owner)) => {
                    let resources = vec!["timeline".to_string()];
                    let rev = h.room.rev();
                    let _ = h.send(
                        client_id,
                        &ServerMsg::OpLockRejected { op_id, rev, keys: op_keys, resources },
                    );
                    let _ = h.send(
                        client_id,
                        &ServerMsg::LockDenied {
                            resource: "timeline".into(),
                            owner,
                            message: "acquire the timeline lock before editing".into(),
                        },
                    );
                }
                Err(Reject::Locked(resources)) => {
                    let rev = h.room.rev();
                    let _ = h.send(
                        client_id,
                        &ServerMsg::OpLockRejected {
                            op_id,
                            rev,
                            keys: op_keys,
                            resources: resources.clone(),
                        },
                    );
                    let first = resources.first().cloned().unwrap_or_else(|| "object".into());
                    let owner = h
                        .room
                        .lock_state()
                        .3
                        .iter()
                        .find(|lock| lock.object_id == first)
                        .map(|lock| lock.client_id);
                    let _ = h.send(
                        client_id,
                        &ServerMsg::LockDenied {
                            resource: first,
                            owner,
                            message: "reserve the selected object before editing".into(),
                        },
                    );
                }
                Err(refused) => {
                    let (reason, keys) = match refused {
                        Reject::Stale(keys) => (
                            "changed by someone else since your revision".to_string(),
                            keys,
                        ),
                        Reject::Invalid(why) => (why, Vec::new()),
                        Reject::TimelineLocked(_) => {
                            ("timeline lock required".to_string(), Vec::new())
                        }
                        Reject::Locked(resources) => (
                            format!("object locks required: {}", resources.join(", ")),
                            Vec::new(),
                        ),
                    };
                    let rev = h.room.rev();
                    let _ = h.send(
                        client_id,
                        &ServerMsg::Rejected { op_id, rev, reason, keys },
                    );
                }
            }
        }
        ClientMsg::RequestState => {
            let (rev, state, users) = (h.room.rev(), h.room.state().clone(), h.room.users());
            let _ = h.send(client_id, &ServerMsg::Welcome { session: session.to_string(), client_id, rev, state, users });
        }
        ClientMsg::Presence { data } => {
            // presence is small (a sequence id, a playhead…); refuse anything big
            if data.to_string().len() <= MAX_PRESENCE_BYTES && h.room.set_presence(client_id, data.clone()) {
                h.broadcast(&ServerMsg::Presence { client_id, data }, Some(client_id));
            }
        }
        ClientMsg::SetTimelineLocking { enabled } => {
            match h.room.set_timeline_locking(client_id, enabled) {
                Ok(()) => h.broadcast_lock_state(),
                Err(e) => { let _ = h.send(client_id, &ServerMsg::LockDenied { resource: e.resource, owner: e.owner, message: e.message }); }
            }
        }
        ClientMsg::AcquireTimelineLock => {
            match h.room.acquire_timeline_lock(client_id) {
                Ok(()) => h.broadcast_lock_state(),
                Err(e) => { let _ = h.send(client_id, &ServerMsg::LockDenied { resource: e.resource, owner: e.owner, message: e.message }); }
            }
        }
        ClientMsg::ReleaseTimelineLock => {
            h.room.release_timeline_lock(client_id);
            h.broadcast_lock_state();
        }
        ClientMsg::SetObjectLocks { object_ids } => {
            match h.room.set_object_locks(client_id, object_ids) {
                Ok(()) => h.broadcast_lock_state(),
                Err(e) => {
                    // A failed replacement may still have released objects the user deselected.
                    h.broadcast_lock_state();
                    let _ = h.send(client_id, &ServerMsg::LockDenied { resource: e.resource, owner: e.owner, message: e.message });
                }
            }
        }
        ClientMsg::Leave => return false,
        ClientMsg::Create { .. } | ClientMsg::Join { .. } => {
            let _ = h.send(client_id, &ServerMsg::Error { message: "already in a session".into(), fatal: false });
        }
    }
    true
}

async fn handle(stream: TcpStream, hub: Hub) -> Result<(), BoxError> {
    let ws = accept_async(stream).await?;
    let (mut sink, mut source) = ws.split();

    // The handshake: the first text frame must be Create or Join.
    let first = tokio::time::timeout(HANDSHAKE_TIMEOUT, async {
        while let Some(frame) = source.next().await {
            match frame? {
                Message::Text(t) => return Ok::<Option<ClientMsg>, BoxError>(serde_json::from_str::<ClientMsg>(t.as_str()).ok()),
                Message::Ping(p) => sink.send(Message::Pong(p)).await?,
                Message::Close(_) => return Ok(None),
                _ => {}
            }
        }
        Ok(None)
    })
    .await;
    let first = match first {
        Ok(Ok(Some(msg))) => msg,
        Ok(Ok(None)) | Err(_) => return Ok(()),
        Ok(Err(e)) => return Err(e),
    };
    let mut joined = match admit(&hub, first) {
        Ok(j) => j,
        Err(message) => {
            if let Some(m) = text(&ServerMsg::Error { message, fatal: true }) {
                let _ = sink.send(m).await;
            }
            let _ = sink.close().await;
            return Ok(());
        }
    };
    let (session, client_id) = (joined.session.clone(), joined.client_id);

    let result: Result<(), BoxError> = async {
        loop {
            tokio::select! {
                outgoing = joined.rx.recv() => match outgoing {
                    Some(msg) => {
                        if let Some(m) = text(&msg) {
                            sink.send(m).await?;
                        }
                    }
                    // dropped by the relay (too slow)
                    None => break,
                },
                incoming = source.next() => match incoming {
                    None | Some(Ok(Message::Close(_))) => break,
                    Some(Ok(Message::Text(t))) => match serde_json::from_str::<ClientMsg>(t.as_str()) {
                        Ok(msg) => {
                            if !on_client_message(&hub, &session, client_id, msg) {
                                break;
                            }
                        }
                        Err(_) => {
                            // malformed: ignore, the client may be newer than this relay
                        }
                    },
                    Some(Ok(Message::Ping(p))) => sink.send(Message::Pong(p)).await?,
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(Box::new(e) as BoxError),
                },
            }
        }
        Ok(())
    }
    .await;
    leave(&hub, &session, client_id);
    let _ = sink.close().await;
    result
}
