//! FilmCraft collaboration relay.
//!
//! Hosts collaborative sessions in memory. Each session is a shared project document; the relay
//! gives every accepted operation the next revision, refuses writes that are based on an out-of-date
//! view of a part somebody else already changed, and tells every client the same story in the same
//! order. See `docs/collaboration-status.md` and `crates/collab`.
//!
//! Usage: `filmcraft-relay [LISTEN_ADDR]` (default `0.0.0.0:9001`).

use std::{env, net::SocketAddr};

const DEFAULT_ADDR: &str = "0.0.0.0:9001";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_ADDR.into())
        .parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!(
        "FilmCraft relay (protocol {}) listening on ws://{}",
        filmcraft_collab::PROTOCOL_VERSION,
        listener.local_addr()?
    );
    tokio::select! {
        served = filmcraft_collab::server::serve(listener) => served?,
        _ = tokio::signal::ctrl_c() => println!("shutting down"),
    }
    Ok(())
}
