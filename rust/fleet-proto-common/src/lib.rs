//! `fleet-proto-common` — Rust wrapper over `proto/common.proto`.
//!
//! The proto contract is language-neutral; this crate is the **Rust adapter**
//! for it. It bundles:
//!   * generated Buffa/ConnectRPC types (in `OUT_DIR`, never hand-edited), and
//!   * hand-written, contract-level helpers that are specific to `common`
//!     (batch `ItemResult`/`Ack`/`Error` builders).
//!
//! Other contract crates (e.g. `fleet-proto-provider`) reference these types
//! across the crate boundary via codegen `extern_path` (`.common` →
//! `::fleet_proto_common::common`), so `common` is compiled **once** and reused.

// Generated `pub mod common { ... }` (types for the `common` proto package).
// Included at the crate root so the path is `fleet_proto_common::common`, which
// is exactly what `extern_path` maps `.common` onto.
include!(concat!(env!("OUT_DIR"), "/_connectrpc.rs"));

use common::{Ack, Error as ItemError, ItemResult};

// ─────────────────────── common-contract helpers ───────────────────────
//
// These are the idiomatic Rust surface over the generated `common` types. They
// are shared by every consumer (host + all provider impls) so the batch-result
// construction isn't copy-pasted per service. Keep this layer *contract*-level:
// anything provider/host-specific belongs in those crates, not here.

/// A per-item success row for a batch result (`outcome` = `ack`).
pub fn item_ack(id: impl Into<String>, code: &str, message: &str) -> ItemResult {
    ItemResult {
        id: id.into(),
        outcome: Ack {
            code: code.to_owned(),
            message: message.to_owned(),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

/// A per-item failure row for a batch result (`outcome` = `error`).
pub fn item_error(id: impl Into<String>, code: &str, message: impl Into<String>) -> ItemResult {
    ItemResult {
        id: id.into(),
        outcome: ItemError {
            code: code.to_owned(),
            message: message.into(),
            ..Default::default()
        }
        .into(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_row_carries_ack_outcome() {
        let r = item_done("a");
        assert_eq!(r.id, "a");
        assert!(matches!(
            r.outcome,
            Some(common::item_result::Outcome::Ack(_))
        ));
    }

    #[test]
    fn error_row_carries_code() {
        let r = item_error("b", "NOT_FOUND", "gone");
        match r.outcome {
            Some(common::item_result::Outcome::Error(e)) => {
                assert_eq!(e.code, "NOT_FOUND");
                assert_eq!(e.message, "gone");
            }
            other => panic!("expected error outcome, got {other:?}"),
        }
    }
}
