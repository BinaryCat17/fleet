//! `fleet-proto-common` — Rust wrapper over `proto/common.proto`.

include!(concat!(env!("OUT_DIR"), "/_connectrpc.rs"));

use common::{Ack, Error as ItemError, ItemResult};

// ─────────────────────── common-contract helpers ───────────────────────

pub fn err(code: &str, message: impl Into<String>) -> ItemError {
    ItemError {
        code: code.to_owned(),
        message: message.into(),
        ..Default::default()
    }
}

/// A typed contract ack (`common.Ack`) from a literal code + message.
pub fn ack(code: &str, message: impl Into<String>) -> Ack {
    Ack {
        code: code.to_owned(),
        message: message.into(),
        ..Default::default()
    }
}

/// A per-item success row for a batch result (`outcome` = `ack`).
pub fn item_ack(id: impl Into<String>, code: &str, message: &str) -> ItemResult {
    ItemResult {
        id: id.into(),
        outcome: ack(code, message).into(),
        ..Default::default()
    }
}

/// A per-item failure row for a batch result (`outcome` = `error`).
/// Takes any typed contract error (incl. a domain error via `From`).
pub fn item_error(id: impl Into<String>, e: impl Into<ItemError>) -> ItemResult {
    let e: ItemError = e.into();
    ItemResult {
        id: id.into(),
        outcome: e.into(),
        ..Default::default()
    }
}

/// `DONE` shorthand: an id was successfully processed.
pub fn item_done(id: impl Into<String>) -> ItemResult {
    item_ack(id, "DONE", "")
}

/// `NO ACTION` shorthand: idempotent no-op (already present/absent).
pub fn item_no_action(id: impl Into<String>, message: &str) -> ItemResult {
    item_ack(id, "NO ACTION", message)
}
