//! Real-time collaboration for FilmCraft.
//!
//! - [`proto`]: the JSON messages between clients and the relay.
//! - [`merge`]: three-way merge of JSON values (how two people's edits to one track combine).
//! - [`room`]: one session as the relay sees it; it orders operations and refuses stale ones.
//! - [`sync`]: the client state machine (mirror, local copy, one operation in flight, rebase).
//! - `server` and `conn` (feature `net`): the WebSocket relay and the client connection.
//!
//! Nothing here knows what a FilmCraft project is: the shared document is a map of keys to JSON
//! values. `filmcraft-engine` (`collab` module) maps a project onto that map and back.

#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::unimplemented, clippy::todo, clippy::unreachable))]

pub mod merge;
pub mod proto;
pub mod room;
pub mod sync;

#[cfg(feature = "net")]
pub mod conn;
#[cfg(feature = "net")]
pub mod server;

pub use merge::{Merged, merge3};
pub use proto::{AppliedOp, Changes, ClientMsg, Doc, Key, ObjectLock, PROTOCOL_VERSION, ServerMsg, UserInfo, object_lock_ids_for_changes};
pub use room::{Reject, Room};
pub use sync::{Event, Remote, SyncClient, SyncError};
