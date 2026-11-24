//! A client's connection to the relay.
//!
//! [`Connection::open`] starts a thread that owns a small tokio runtime and one WebSocket. The
//! editor never waits on the network: it hands messages over with [`Connection::send`] and picks up
//! what arrived with [`Connection::try_recv`], once per frame. When the link drops, the thread
//! reports [`NetEvent::Closed`] and ends; reconnecting (with a delay, and a `Join` that says which
//! revision the client holds) is the caller's decision.

use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::proto::{ClientMsg, ServerMsg};

/// A ping goes out this often when the link is quiet.
const PING_EVERY: Duration = Duration::from_secs(15);
/// The link is considered dead after this long without any frame from the relay.
const DEAD_AFTER: Duration = Duration::from_secs(50);
/// How long to wait for the TCP and WebSocket handshake.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// What the connection thread reports.
#[derive(Clone, Debug, PartialEq)]
pub enum NetEvent {
    /// The WebSocket is up; the caller sends its `Create` or `Join` now.
    Connected,
    Message(ServerMsg),
    /// The link ended (for any reason); the thread has stopped.
    Closed(String),
}

/// Called from the network thread whenever there is something to read (wake the UI).
pub type Waker = Arc<dyn Fn() + Send + Sync>;

pub struct Connection {
    to_net: mpsc::UnboundedSender<ClientMsg>,
    from_net: Mutex<std_mpsc::Receiver<NetEvent>>,
}

impl Connection {
    /// Start connecting to the relay at `url` (as people type it: `host`, `host:port` or `ws://host:port`; see [`crate::proto::relay_url`]). Returns at once; progress arrives as events.
    pub fn open(url: &str, waker: Option<Waker>) -> Result<Connection, String> {
        let url = crate::proto::relay_url(url)?;
        let (to_net, from_ui) = mpsc::unbounded_channel::<ClientMsg>();
        let (to_ui, from_net) = std_mpsc::channel::<NetEvent>();
        let thread_tx = to_ui.clone();
        let thread_waker = waker.clone();
        let spawned = std::thread::Builder::new()
            .name("filmcraft-collab-net".into())
            .spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run(url, from_ui, thread_tx.clone(), thread_waker.clone())
                }));
                let reason = match outcome {
                    Ok(reason) => reason,
                    Err(_) => "internal error in the network thread".to_string(),
                };
                let _ = thread_tx.send(NetEvent::Closed(reason));
                if let Some(w) = &thread_waker {
                    w();
                }
            });
        match spawned {
            Ok(_) => Ok(Connection {
                to_net,
                from_net: Mutex::new(from_net),
            }),
            Err(e) => Err(format!("cannot start the network thread: {e}")),
        }
    }

    /// Queue a message. False when the connection is already gone.
    pub fn send(&self, msg: ClientMsg) -> bool {
        self.to_net.send(msg).is_ok()
    }

    /// The next event, if any.
    pub fn try_recv(&self) -> Option<NetEvent> {
        self.from_net
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .try_recv()
            .ok()
    }
}

/// The thread body; the returned text says why the link ended.
fn run(
    url: String,
    from_ui: mpsc::UnboundedReceiver<ClientMsg>,
    to_ui: std_mpsc::Sender<NetEvent>,
    waker: Option<Waker>,
) -> String {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => return format!("cannot start the network runtime: {e}"),
    };
    runtime.block_on(link(url, from_ui, to_ui, waker))
}

async fn link(
    url: String,
    mut from_ui: mpsc::UnboundedReceiver<ClientMsg>,
    to_ui: std_mpsc::Sender<NetEvent>,
    waker: Option<Waker>,
) -> String {
    let notify = |event: NetEvent| -> bool {
        let sent = to_ui.send(event).is_ok();
        if let Some(w) = &waker {
            w();
        }
        sent
    };
    let connected = tokio::time::timeout(CONNECT_TIMEOUT, connect_async(url.as_str())).await;
    let ws = match connected {
        Ok(Ok((ws, _response))) => ws,
        Ok(Err(e)) => return format!("cannot connect to {url}: {e}"),
        Err(_) => return format!("timed out connecting to {url}"),
    };
    if !notify(NetEvent::Connected) {
        return "the editor closed the connection".into();
    }
    let (mut sink, mut source) = ws.split();
    let mut ping = tokio::time::interval(PING_EVERY);
    let mut last_heard = tokio::time::Instant::now();
    loop {
        tokio::select! {
            outgoing = from_ui.recv() => match outgoing {
                Some(msg) => {
                    let Ok(json) = serde_json::to_string(&msg) else { continue };
                    if let Err(e) = sink.send(Message::Text(json.into())).await {
                        return format!("send failed: {e}");
                    }
                    if matches!(msg, ClientMsg::Leave) {
                        let _ = sink.close().await;
                        return "left the session".into();
                    }
                }
                // the editor dropped its end
                None => {
                    let _ = sink.close().await;
                    return "the editor closed the connection".into();
                }
            },
            incoming = source.next() => {
                last_heard = tokio::time::Instant::now();
                match incoming {
                    None => return "the relay closed the connection".into(),
                    Some(Err(e)) => return format!("connection error: {e}"),
                    Some(Ok(Message::Close(_))) => return "the relay closed the connection".into(),
                    Some(Ok(Message::Text(t))) => {
                        if let Ok(msg) = serde_json::from_str::<ServerMsg>(t.as_str())
                            && !notify(NetEvent::Message(msg))
                        {
                            return "the editor closed the connection".into();
                        }
                    }
                    Some(Ok(Message::Ping(p))) => {
                        if sink.send(Message::Pong(p)).await.is_err() {
                            return "send failed".into();
                        }
                    }
                    Some(Ok(_)) => {}
                }
            },
            _ = ping.tick() => {
                if last_heard.elapsed() > DEAD_AFTER {
                    return "the relay stopped answering".into();
                }
                if sink.send(Message::Ping(Vec::<u8>::new().into())).await.is_err() {
                    return "send failed".into();
                }
            },
        }
    }
}
