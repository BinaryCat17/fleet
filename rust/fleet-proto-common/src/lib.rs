//! `fleet-proto-common` — Rust wrapper over `proto/common.proto`.

include!(concat!(env!("OUT_DIR"), "/_connectrpc.rs"));

use common::{Ack, AckCode, Error, ErrorCode, ItemResult};
use connectrpc::ConnectError;

// ─────────────────────── common-contract helpers ───────────────────────

/// A typed contract error (`common.Error`) from a code + message.
pub fn err(code: ErrorCode, message: impl Into<String>) -> Error {
    Error {
        code: code.into(),
        message: message.into(),
        ..Default::default()
    }
}

/// A typed contract ack (`common.Ack`) from a code + message.
pub fn ack(code: AckCode, message: impl Into<String>) -> Ack {
    Ack {
        code: code.into(),
        message: message.into(),
        ..Default::default()
    }
}

/// A per-item success row for a batch result (`outcome` = `ack`).
pub fn item_ack(id: impl Into<String>, code: AckCode, message: &str) -> ItemResult {
    ItemResult {
        id: id.into(),
        outcome: ack(code, message).into(),
        ..Default::default()
    }
}

/// A per-item failure row for a batch result (`outcome` = `error`).
/// Takes any typed contract error (incl. a domain error via `From`).
pub fn item_error(id: impl Into<String>, e: impl Into<Error>) -> ItemResult {
    let e: Error = e.into();
    ItemResult {
        id: id.into(),
        outcome: e.into(),
        ..Default::default()
    }
}

/// `ACK_CODE_DONE` shorthand: an id was successfully processed.
pub fn item_done(id: impl Into<String>) -> ItemResult {
    item_ack(id, AckCode::ACK_CODE_DONE, "")
}

/// `ACK_CODE_NO_ACTION` shorthand: idempotent no-op (already present/absent).
pub fn item_no_action(id: impl Into<String>, message: &str) -> ItemResult {
    item_ack(id, AckCode::ACK_CODE_NO_ACTION, message)
}

// ─────────── contract error → transport status (shared bridge) ───────────
// Lives here because Rust coherence allows `From<Error> for ConnectError` only
// in the crate that owns `Error` — and every server crate needs the mapping.

/// Contract error code → connectrpc status code.
impl From<ErrorCode> for connectrpc::ErrorCode {
    fn from(code: ErrorCode) -> Self {
        match code {
            ErrorCode::ERROR_CODE_NOT_FOUND => connectrpc::ErrorCode::NotFound,
            ErrorCode::ERROR_CODE_ALREADY_EXISTS => connectrpc::ErrorCode::AlreadyExists,
            ErrorCode::ERROR_CODE_INVALID_ARGUMENT => connectrpc::ErrorCode::InvalidArgument,
            ErrorCode::ERROR_CODE_FAILED_PRECONDITION => connectrpc::ErrorCode::FailedPrecondition,
            ErrorCode::ERROR_CODE_UNAVAILABLE => connectrpc::ErrorCode::Unavailable,
            ErrorCode::ERROR_CODE_RESOURCE_EXHAUSTED => connectrpc::ErrorCode::ResourceExhausted,
            _ => connectrpc::ErrorCode::Internal, // UNSPECIFIED / unknown
        }
    }
}

/// Whole-call domain failure → `ConnectError`: the call-level outcome travels
/// as the connectrpc status (unary and streams alike); in-band rows carry only
/// per-item batch outcomes. Call sites just use `?` / `.into()`.
impl From<Error> for ConnectError {
    fn from(e: Error) -> Self {
        let code = e
            .code
            .as_known()
            .map(connectrpc::ErrorCode::from)
            .unwrap_or(connectrpc::ErrorCode::Internal);
        ConnectError::new(code, e.message)
    }
}
