//! Collaboration panel: share the open project, or join someone else's, through a FilmCraft relay.
//!
//! The network runs on its own thread ([`filmcraft_collab::conn::Connection`]); this module only
//! moves messages between that thread and the engine once per logic tick, so the editor never
//! waits on the network. The protocol and merge rules are in `filmcraft-collab`, the mapping of the
//! project onto the shared document is in `filmcraft_engine::collab`.
//!
//! Joining replaces the open project with the shared one, so it is refused while the open project
//! has unsaved changes (unless the box to discard them is ticked).

use std::sync::Arc;
use std::time::{Duration, Instant};

use egui::{Color32, Rect, RichText, vec2};
use filmcraft_collab::conn::{Connection, NetEvent};
use filmcraft_collab::{
    ClientMsg, Doc, ObjectLock, PROTOCOL_VERSION, ServerMsg, UserInfo, object_lock_ids_for_changes,
};
use filmcraft_project::{ItemId, Project};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::FilmcraftApp;

/// Where the last relay, user name and session are remembered (next to the other preferences).
const SETTINGS_FILE: &str = "collab.json";
/// Seconds to wait before each reconnect attempt (the last repeats).
const BACKOFF: [u64; 5] = [1, 2, 4, 8, 15];
/// Presence is sent at most this often.
const PRESENCE_EVERY: Duration = Duration::from_millis(250);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Link {
    Disconnected,
    /// The socket is opening, or the relay has not answered the Create/Join yet.
    Connecting,
    Online,
    /// The link dropped; the session is kept and the panel retries.
    Reconnecting,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Intent {
    Share,
    Join,
}

/// What is remembered between runs. The password is never stored.
#[derive(Default, Serialize, Deserialize)]
struct Remembered {
    #[serde(default)]
    relay: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    session: String,
}

pub struct CollabUi {
    /// The relay as typed: `host`, `host:port` (default port 9001) or `ws://host:port`.
    pub relay: String,
    pub name: String,
    pub session: String,
    pub password: String,
    /// Allow joining even though the open project has unsaved changes (they are replaced).
    pub discard_ok: bool,
    pub link: Link,
    pub status: String,
    pub users: Vec<UserInfo>,
    pub my_id: Option<u64>,
    pub host_id: Option<u64>,
    pub timeline_locking: bool,
    pub timeline_owner: Option<u64>,
    pub object_locks: Vec<ObjectLock>,
    pub lock_status: String,
    conn: Option<Connection>,
    intent: Intent,
    /// What `Create` carried: the document and the project snapshot it was made from.
    shared: Option<(Doc, Arc<Project>)>,
    reconnect_at: Option<Instant>,
    attempts: usize,
    last_presence: Option<(serde_json::Value, Instant)>,
    /// The last selection sent to the relay; `None` forces a refresh after reconnect.
    last_object_locks: Option<Vec<String>>,
    /// The remembered settings have been read.
    loaded: bool,
}

impl Default for CollabUi {
    fn default() -> Self {
        let name = std::env::var("USER")
            .or_else(|_| std::env::var("USERNAME"))
            .unwrap_or_default();
        Self {
            // empty until the user types one or a remembered one is loaded; FILMCRAFT_RELAY may preset it
            relay: std::env::var("FILMCRAFT_RELAY").unwrap_or_default(),
            name,
            session: String::new(),
            password: String::new(),
            discard_ok: false,
            link: Link::Disconnected,
            status: String::new(),
            users: Vec::new(),
            my_id: None,
            host_id: None,
            timeline_locking: true,
            timeline_owner: None,
            object_locks: Vec::new(),
            lock_status: String::new(),
            conn: None,
            intent: Intent::Share,
            shared: None,
            reconnect_at: None,
            attempts: 0,
            last_presence: None,
            last_object_locks: None,
            loaded: false,
        }
    }
}

fn settings_path(app: &FilmcraftApp) -> Option<std::path::PathBuf> {
    app.session
        .prefs_path
        .as_ref()?
        .parent()
        .map(|d| d.join(SETTINGS_FILE))
}

/// Read the remembered relay, name and session once.
fn ensure_loaded(app: &mut FilmcraftApp) {
    if app.collab.loaded {
        return;
    }
    app.collab.loaded = true;
    let Some(path) = settings_path(app) else {
        return;
    };
    let Ok(text) = std::fs::read_to_string(&path) else {
        return;
    };
    let Ok(saved) = serde_json::from_str::<Remembered>(&text) else {
        return;
    };
    if !saved.relay.is_empty() {
        app.collab.relay = saved.relay;
    }
    if !saved.name.is_empty() {
        app.collab.name = saved.name;
    }
    if !saved.session.is_empty() {
        app.collab.session = saved.session;
    }
}

/// Remember the relay, name and session that worked.
fn remember(app: &FilmcraftApp) {
    let Some(path) = settings_path(app) else {
        return;
    };
    let saved = Remembered {
        relay: app.collab.relay.trim().to_string(),
        name: app.collab.name.clone(),
        session: app.collab.session.trim().to_string(),
    };
    let Ok(text) = serde_json::to_string_pretty(&saved) else {
        return;
    };
    if let Some(dir) = path.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    if let Err(e) = std::fs::write(&path, text) {
        log::debug!("collaboration: cannot save settings: {e}");
    }
}

impl CollabUi {
    fn open_conn(&mut self, ctx: &egui::Context) -> bool {
        let ctx = ctx.clone();
        let waker: filmcraft_collab::conn::Waker = Arc::new(move || ctx.request_repaint());
        match Connection::open(&self.relay, Some(waker)) {
            Ok(c) => {
                self.conn = Some(c);
                true
            }
            Err(e) => {
                self.status = e;
                self.link = Link::Disconnected;
                false
            }
        }
    }

    fn send(&self, msg: ClientMsg) {
        if let Some(c) = &self.conn {
            c.send(msg);
        }
    }

    fn token(&self) -> Option<String> {
        Some(self.password.clone()).filter(|p| !p.is_empty())
    }

    fn hello(&self, last_rev: Option<u64>) -> ClientMsg {
        let (session, name, token) = (
            self.session.trim().to_string(),
            self.name.clone(),
            self.token(),
        );
        match (&self.shared, self.intent, last_rev) {
            (Some((doc, _)), Intent::Share, None) => ClientMsg::Create {
                protocol: PROTOCOL_VERSION,
                session,
                name,
                token,
                state: doc.clone(),
            },
            _ => ClientMsg::Join {
                protocol: PROTOCOL_VERSION,
                session,
                name,
                token,
                last_rev,
            },
        }
    }

    fn reset(&mut self) {
        self.conn = None;
        self.shared = None;
        self.users.clear();
        self.my_id = None;
        self.host_id = None;
        self.timeline_locking = true;
        self.timeline_owner = None;
        self.object_locks.clear();
        self.last_object_locks = None;
        self.reconnect_at = None;
        self.attempts = 0;
        self.last_presence = None;
        self.link = Link::Disconnected;
    }
}

/// Start a session with the open project, or join one.
fn connect(app: &mut FilmcraftApp, ctx: &egui::Context, intent: Intent) {
    let c = &mut app.collab;
    if !filmcraft_collab::proto::valid_session_id(c.session.trim()) {
        c.status =
            tl!("Enter a session id (letters, digits, '-', '_' and '.', up to 64 characters).")
                .to_string();
        return;
    }
    if app.session.collab_active() {
        return;
    }
    if let Err(e) = filmcraft_collab::proto::relay_url(&c.relay) {
        c.status = e;
        return;
    }
    if intent == Intent::Join && app.session.is_dirty() && !c.discard_ok {
        c.status = tl!("The open project has unsaved changes and joining replaces it. Save it first, or tick \"Discard unsaved changes\".").to_string();
        return;
    }
    c.shared = None;
    if intent == Intent::Share {
        match app.session.collab_export() {
            Ok(shared) => c.shared = Some(shared),
            Err(e) => {
                c.status = format!("{}: {e}", tl!("Cannot share the project"));
                return;
            }
        }
    }
    c.intent = intent;
    c.status = tl!("Connecting…").to_string();
    c.link = Link::Connecting;
    if !c.open_conn(ctx) {
        c.shared = None;
    }
}

/// Leave the session; the project stays open as an ordinary local project.
fn disconnect(app: &mut FilmcraftApp, status: &str) {
    app.collab.send(ClientMsg::Leave);
    app.session.collab_detach();
    app.collab.reset();
    app.collab.status = status.to_string();
}

fn lost(app: &mut FilmcraftApp, ctx: &egui::Context, reason: &str) {
    app.collab.conn = None;
    app.collab.last_object_locks = None;
    if app.session.collab_active() {
        app.collab.link = Link::Reconnecting;
        let wait = BACKOFF.get(app.collab.attempts).copied().unwrap_or(15);
        app.collab.reconnect_at = Some(Instant::now() + Duration::from_secs(wait));
        app.collab.status = format!(
            "{} ({reason}). {}",
            tl!("Connection lost"),
            tlf!("Retrying in {s} s…", s = wait)
        );
        ctx.request_repaint_after(Duration::from_secs(wait));
    } else {
        if app.collab.status.is_empty() || app.collab.status == tl!("Connecting…") {
            app.collab.status = reason.to_string();
        }
        app.collab.reset();
    }
}

fn on_server_message(app: &mut FilmcraftApp, ctx: &egui::Context, msg: ServerMsg) {
    // A running session being brought up to date (reconnect, or a resync it asked for).
    if app.session.collab_active()
        && let ServerMsg::Welcome {
            client_id, users, ..
        }
        | ServerMsg::Catchup {
            client_id, users, ..
        } = &msg
    {
        app.collab.users = users.clone();
        app.collab.my_id = Some(*client_id);
        app.collab.last_object_locks = None;
        app.collab.link = Link::Online;
        app.collab.attempts = 0;
        app.collab.status = tl!("Connected").to_string();
        for m in app.session.collab_on_message(msg) {
            app.collab.send(m);
        }
        ctx.request_repaint();
        return;
    }
    match msg {
        // (a Welcome reaching this arm starts a session; one for a running session is handled above)
        ServerMsg::Welcome {
            client_id,
            rev,
            state,
            users,
            ..
        } => {
            let session = app.collab.session.trim().to_string();
            let started = match (app.collab.intent, app.collab.shared.take()) {
                (Intent::Share, Some((_, shared))) => app
                    .session
                    .collab_attach(&session, client_id, rev, state, shared),
                _ => app.session.collab_join(&session, client_id, rev, state),
            };
            match started {
                Ok(()) => {
                    app.collab.users = users;
                    app.collab.my_id = Some(client_id);
                    app.collab.last_object_locks = None;
                    app.collab.link = Link::Online;
                    app.collab.attempts = 0;
                    app.collab.status = tl!("Connected").to_string();
                    remember(app);
                }
                Err(e) => disconnect(app, &format!("{}: {e}", tl!("Could not start the session"))),
            }
        }
        ServerMsg::UserJoin { user } => {
            app.collab.users.retain(|u| u.client_id != user.client_id);
            app.collab.users.push(user);
        }
        ServerMsg::UserLeave { client_id } => app.collab.users.retain(|u| u.client_id != client_id),
        ServerMsg::Presence { client_id, data } => {
            if let Some(u) = app
                .collab
                .users
                .iter_mut()
                .find(|u| u.client_id == client_id)
            {
                u.presence = data;
            }
        }
        ServerMsg::LockState {
            host_id,
            timeline_locking,
            timeline_owner,
            object_locks,
        } => {
            let mode_changed = app.collab.timeline_locking != timeline_locking;
            let previous_locks =
                std::mem::replace(&mut app.collab.object_locks, object_locks.clone());
            app.collab.host_id = host_id;
            app.collab.timeline_locking = timeline_locking;
            app.collab.timeline_owner = timeline_owner;

            // Do not resend a denied reservation on every LockState broadcast. Retry only when
            // an object that was previously held by someone becomes free while still selected.
            if mode_changed {
                app.collab.last_object_locks = None;
            } else if !timeline_locking {
                let selected = selected_object_locks(app);
                let newly_available = previous_locks.iter().any(|previous| {
                    selected.iter().any(|id| id == &previous.object_id)
                        && !object_locks
                            .iter()
                            .any(|current| current.object_id == previous.object_id)
                });
                if newly_available {
                    app.collab.last_object_locks = None;
                }
            }
            app.collab.lock_status.clear();
        }
        ServerMsg::LockDenied {
            resource,
            owner,
            message,
        } => {
            let owner_name = owner.and_then(|id| {
                app.collab
                    .users
                    .iter()
                    .find(|u| u.client_id == id)
                    .map(|u| u.name.clone())
            });
            app.collab.lock_status = match owner_name {
                Some(name) => format!("{message} (held by {name}; resource: {resource})"),
                None => format!("{message} (resource: {resource})"),
            };
        }
        locked @ ServerMsg::OpLockRejected { .. } => {
            if app.session.collab_active() {
                for m in app.session.collab_on_message(locked) {
                    app.collab.send(m);
                }
            }
        }
        ServerMsg::ResyncRequired => app.collab.send(ClientMsg::RequestState),
        ServerMsg::Error { message, fatal } => {
            if fatal {
                if app.session.collab_active() {
                    // e.g. the relay restarted and no longer has the session
                    disconnect(
                        app,
                        &format!(
                            "{message}. {}",
                            tl!("The project stays open here; share it again to continue.")
                        ),
                    );
                } else {
                    app.collab.reset();
                    app.collab.status = message;
                }
            } else {
                app.collab.status = message;
            }
        }
        other => {
            if app.session.collab_active() {
                for m in app.session.collab_on_message(other) {
                    app.collab.send(m);
                }
            }
        }
    }
    ctx.request_repaint();
}

fn presence(app: &mut FilmcraftApp) -> serde_json::Value {
    json!({ "sequence": app.session.state.active_sequence.map(|s| s.0), "playhead": app.session.playhead().0 })
}

/// Lock ids mirror the stable IDs stored in the shared document. Selected objects are
/// automatically reserved while timeline-wide locking is disabled.
fn selected_object_locks(app: &FilmcraftApp) -> Vec<String> {
    let Some(sequence) = app.session.state.active_sequence else {
        return Vec::new();
    };
    let mut ids = std::collections::BTreeSet::new();
    ids.extend(
        app.session
            .state
            .selection
            .iter()
            .map(|id| format!("clip:{}:{}", sequence.0, id.0)),
    );
    ids.extend(
        app.session
            .state
            .caption_selection
            .iter()
            .map(|id| format!("caption:{}:{}", sequence.0, id.0)),
    );
    ids.extend(
        app.session
            .state
            .transition_selection
            .iter()
            .map(|id| format!("transition:{}:{}", sequence.0, id.0)),
    );
    ids.into_iter().collect()
}

/// Called every logic tick: move messages between the network thread and the engine.
pub fn tick(app: &mut FilmcraftApp, ctx: &egui::Context) {
    ensure_loaded(app);
    if app.session.collab_check_epoch() {
        disconnect(
            app,
            tl!("The session ended because another project was opened."),
        );
        return;
    }
    // a scheduled reconnect
    if let Some(at) = app.collab.reconnect_at
        && Instant::now() >= at
        && app.collab.conn.is_none()
    {
        app.collab.reconnect_at = None;
        app.collab.attempts += 1;
        app.collab.link = Link::Reconnecting;
        if !app.collab.open_conn(ctx) {
            lost(app, ctx, tl!("cannot connect"));
        }
    } else if app.collab.reconnect_at.is_some() {
        ctx.request_repaint_after(Duration::from_millis(500));
    }
    // what the network thread reported
    let mut events = Vec::new();
    if let Some(c) = &app.collab.conn {
        while let Some(ev) = c.try_recv() {
            events.push(ev);
        }
    }
    for ev in events {
        match ev {
            NetEvent::Connected => {
                let last = app.session.collab_state().map(|s| s.sync.rev());
                let hello = app.collab.hello(last);
                app.collab.send(hello);
            }
            NetEvent::Message(msg) => on_server_message(app, ctx, msg),
            NetEvent::Closed(reason) => lost(app, ctx, &reason),
        }
    }
    // Observe local changes first so the required object IDs can be reserved before their Op is
    // queued on the wire. Keep previous reservations until the in-flight operation is acknowledged.
    if app.collab.link == Link::Online && app.session.collab_active() {
        let outgoing = app.session.collab_pump();
        if !app.collab.timeline_locking {
            let mut desired: std::collections::BTreeSet<String> =
                selected_object_locks(app).into_iter().collect();
            if let Some(st) = app.session.collab_state() {
                for msg in &outgoing {
                    if let ClientMsg::Op { changes, .. } = msg {
                        desired.extend(object_lock_ids_for_changes(st.sync.mirror(), changes));
                    }
                }
                if !st.sync.is_idle() {
                    desired.extend(app.collab.last_object_locks.iter().flatten().cloned());
                }
            }
            let desired: Vec<String> = desired.into_iter().collect();
            if app.collab.last_object_locks.as_ref() != Some(&desired) {
                // The unbounded connection queue preserves this order: lock first, edit second.
                app.collab.send(ClientMsg::SetObjectLocks {
                    object_ids: desired.clone(),
                });
                app.collab.last_object_locks = Some(desired);
            }
        }
        for m in outgoing {
            app.collab.send(m);
        }
        let now = Instant::now();
        let data = presence(app);
        let due = match &app.collab.last_presence {
            Some((last, at)) => *last != data && now.duration_since(*at) >= PRESENCE_EVERY,
            None => true,
        };
        if due {
            app.collab.send(ClientMsg::Presence { data: data.clone() });
            app.collab.last_presence = Some((data, now));
        }
    }
}

fn user_color(hex: &str) -> Color32 {
    let h = hex.trim_start_matches('#');
    let channel = |i: usize| {
        h.get(i..i + 2)
            .and_then(|s| u8::from_str_radix(s, 16).ok())
            .unwrap_or(136)
    };
    Color32::from_rgb(channel(0), channel(2), channel(4))
}

fn editing_label(app: &FilmcraftApp, u: &UserInfo) -> Option<String> {
    let id = u.presence.get("sequence")?.as_u64()?;
    app.session
        .project
        .items
        .get(&ItemId(id))
        .map(|i| i.name.clone())
}

pub fn show(app: &mut FilmcraftApp, ui: &mut egui::Ui, rect: Rect) {
    ensure_loaded(app);
    let ctx = ui.ctx().clone();
    let mut child = ui.new_child(egui::UiBuilder::new().max_rect(rect.shrink2(vec2(10.0, 8.0))));
    let ui = &mut child;
    egui::ScrollArea::vertical().id_salt("collab-scroll").auto_shrink([false, false]).show(ui, |ui| {
        let active = app.session.collab_active() || app.collab.link != Link::Disconnected;
        egui::Grid::new("collab-form").num_columns(2).spacing([8.0, 6.0]).show(ui, |ui| {
            ui.label(tl!("Relay"));
            ui.add_enabled(!active, egui::TextEdit::singleline(&mut app.collab.relay).hint_text(tl!("address, e.g. 192.168.1.10 (port 9001 if omitted)")).desired_width(240.0));
            ui.end_row();
            ui.label(tl!("User name"));
            ui.add_enabled(!active, egui::TextEdit::singleline(&mut app.collab.name).desired_width(240.0));
            ui.end_row();
            ui.label(tl!("Session"));
            ui.add_enabled(!active, egui::TextEdit::singleline(&mut app.collab.session).hint_text(tl!("shared project id")).desired_width(240.0));
            ui.end_row();
            ui.label(tl!("Password"));
            ui.add_enabled(!active, egui::TextEdit::singleline(&mut app.collab.password).password(true).hint_text(tl!("optional")).desired_width(240.0));
            ui.end_row();
        });
        ui.add_space(6.0);
        if active {
            if ui.button(tl!("Disconnect")).clicked() {
                disconnect(app, tl!("Disconnected. The project stays open as a local project."));
            }
        } else {
            ui.checkbox(&mut app.collab.discard_ok, tl!("Discard unsaved changes when joining"));
            ui.horizontal(|ui| {
                if ui.button(tl!("Share open project")).on_hover_text(tl!("Start a session with the project that is open now")).clicked() {
                    connect(app, &ctx, Intent::Share);
                }
                if ui.button(tl!("Join session")).on_hover_text(tl!("Open the shared project; replaces the open project")).clicked() {
                    connect(app, &ctx, Intent::Join);
                }
            });
        }
        ui.add_space(8.0);
        let (dot, label) = match app.collab.link {
            Link::Online => (Color32::from_rgb(60, 180, 75), tl!("Online")),
            Link::Connecting => (Color32::from_rgb(230, 180, 40), tl!("Connecting…")),
            Link::Reconnecting => (Color32::from_rgb(230, 130, 40), tl!("Reconnecting…")),
            Link::Disconnected => (Color32::GRAY, tl!("Not connected")),
        };
        ui.horizontal(|ui| {
            ui.label(RichText::new("●").color(dot));
            ui.label(label);
        });
        if !app.collab.status.is_empty() {
            ui.label(RichText::new(app.collab.status.clone()).weak());
        }
        if let Some(st) = app.session.collab_state() {
            ui.add_space(4.0);
            ui.label(RichText::new(format!("{}: {}", tl!("Session"), st.session_id)).strong());
            ui.label(RichText::new(tlf!("Revision {rev} · {pending} change(s) not yet confirmed · {clashes} clash(es)", rev = st.sync.rev(), pending = st.sync.pending_keys(), clashes = st.clashes)).weak());
        }
        if app.session.collab_active() {
            ui.add_space(10.0);
            ui.separator();
            ui.label(RichText::new("Editing locks").strong());
            let is_host = app.collab.my_id.is_some() && app.collab.my_id == app.collab.host_id;
            let mut timeline_locking = app.collab.timeline_locking;
            if ui.add_enabled(is_host, egui::Checkbox::new(&mut timeline_locking, "Timeline locking (first come, first served)"))
                .on_hover_text("When enabled, one collaborator reserves the whole timeline. Only the session host can change this mode.")
                .changed()
            {
                app.collab.send(ClientMsg::SetTimelineLocking { enabled: timeline_locking });
                app.collab.lock_status = "Changing locking mode…".into();
            }
            if app.collab.timeline_locking {
                let owner = app.collab.timeline_owner;
                let owner_name = owner.and_then(|id| app.collab.users.iter().find(|u| u.client_id == id).map(|u| u.name.as_str())).unwrap_or("Nobody");
                ui.label(format!("Timeline lock: {owner_name}"));
                if owner == app.collab.my_id && owner.is_some() {
                    if ui.button("Release timeline lock").clicked() {
                        app.collab.send(ClientMsg::ReleaseTimelineLock);
                    }
                } else if owner.is_none() {
                    if ui.button("Acquire timeline lock").clicked() {
                        app.collab.send(ClientMsg::AcquireTimelineLock);
                        app.collab.lock_status = "Requesting timeline lock…".into();
                    }
                } else {
                    ui.label("The timeline is reserved by another collaborator.");
                }
            } else {
                ui.label("Object locking is active: selected clips, captions and transitions are reserved automatically.");
                for lock in &app.collab.object_locks {
                    let who = app.collab.users.iter().find(|u| u.client_id == lock.client_id).map(|u| u.name.as_str()).unwrap_or("Unknown");
                    ui.label(format!("{} — {}", lock.object_id, who));
                }
            }
            if !app.collab.lock_status.is_empty() {
                ui.label(RichText::new(app.collab.lock_status.clone()).color(Color32::LIGHT_RED));
            }
        }
        if !app.collab.users.is_empty() {
            ui.add_space(8.0);
            ui.label(RichText::new(tl!("Users")).strong());
            let me = app.collab.my_id;
            for u in &app.collab.users {
                ui.horizontal(|ui| {
                    ui.label(RichText::new("●").color(user_color(&u.color)));
                    let you = if Some(u.client_id) == me { format!(" ({})", tl!("you")) } else { String::new() };
                    ui.label(format!("{}{you}", u.name));
                    if let Some(seq) = editing_label(app, u) {
                        ui.label(RichText::new(seq).weak());
                    }
                });
            }
        }
    });
}
