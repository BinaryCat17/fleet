//! `fleet-proto-common` — Rust wrapper over `proto/common.proto`.

include!(concat!(env!("OUT_DIR"), "/_connectrpc.rs"));

use common::{ItemResult, ItemResultCode};

// ─────────────────────── common-contract helpers ───────────────────────

/// A per-item result row: the id and what the call did to it.
pub fn item(id: impl Into<String>, code: ItemResultCode) -> ItemResult {
    ItemResult {
        id: id.into(),
        code: code.into(),
        ..Default::default()
    }
}
