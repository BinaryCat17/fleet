//! `fleet-proto-provider` — Rust wrapper over `proto/provider.proto`.
//!
//! The proto contract is language-neutral; this crate is the **Rust adapter**
//! for it (generated Buffa/ConnectRPC types + contract-level helpers shared by
//! every provider implementation). Individual providers (VAST, AWS, …) depend on
//! this crate and implement the `Provider` service, writing only the upstream
//! anti-corruption translation.
//!
//! Cross-crate sharing: `provider.proto` imports `common.proto`. Rather than
//! regenerating `common`'s types here, codegen `extern_path` maps the `.common`
//! package onto `::fleet_proto_common::common`, so provider request/response
//! types embed the *same* `common` types compiled once in `fleet-proto-common`.

// Generated `pub mod provider { ... }`. `common` is NOT generated here — it is
// referenced via `extern_path` from `fleet-proto-common`.
pub mod proto {
    connectrpc::include_generated!();
}

// Re-export the contract surface so consumers can reach both packages from one
// place, with identical types (these are the very types `extern_path` bakes in).
pub use fleet_proto_common::common;
pub use proto::provider;

/// Embedded `FileDescriptorSet` for reflection (grpcurl / buf curl). Includes
/// `provider.proto` **and** `common.proto` (the full set handed to codegen), so
/// a server can serve the complete schema without a second descriptor source.
pub const PROVIDER_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/provider.fds.bin"));

use connectrpc::{ConnectError, ErrorCode};

// ─────────────────────── provider contract error ───────────────────────
//
// The shared, provider-agnostic error shape. Each provider impl maps *its*
// upstream (VAST's HTTP codes, AWS's errors, …) onto this; the batch RPCs then
// map it to `common.Error` rows or Connect codes. Keeping it here means every
// provider speaks the same error vocabulary.

#[derive(thiserror::Error, Debug)]
pub enum ProviderError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("upstream unavailable: {0}")]
    Unavailable(String),
    #[error("upstream refused: {0}")]
    FailedPrecondition(String),
}

impl ProviderError {
    /// Stable per-item code used in batch result rows (names match the Connect codes).
    pub fn code(&self) -> &'static str {
        match self {
            ProviderError::NotFound(_) => "NOT_FOUND",
            ProviderError::InvalidArgument(_) => "INVALID_ARGUMENT",
            ProviderError::Unavailable(_) => "UNAVAILABLE",
            ProviderError::FailedPrecondition(_) => "FAILED_PRECONDITION",
        }
    }
}

impl From<ProviderError> for ConnectError {
    fn from(e: ProviderError) -> Self {
        let code = match &e {
            ProviderError::NotFound(_) => ErrorCode::NotFound,
            ProviderError::InvalidArgument(_) => ErrorCode::InvalidArgument,
            ProviderError::Unavailable(_) => ErrorCode::Unavailable,
            ProviderError::FailedPrecondition(_) => ErrorCode::FailedPrecondition,
        };
        ConnectError::new(code, e.to_string())
    }
}

// ─────────────────────── provider-contract helpers ───────────────────────

/// Per-item failure row for a `RentInstances` batch result (`outcome` = `error`).
///
/// `common`-level builders (`item_ack` / `item_error`) live in
/// `fleet-proto-common`; this one is provider-specific because it constructs the
/// provider's own `RentInstancesResult` row.
///
/// Note the explicit `Some(Outcome::from(...))` rather than `common::Error{..}.into()`:
/// with `extern_path`, `common::Error` is a *foreign* type and `Option<Outcome>`
/// has a foreign head (`core::option::Option`), so the generated
/// `From<Error> for Option<Outcome>` is skipped (orphan rule). The
/// `From<Error> for Outcome` impl *is* generated (`Outcome` is local), so we go
/// through that and wrap in `Some` ourselves. The same applies to any oneof field
/// whose variant type lives in another crate.
pub fn rent_error(offer_id: &str, code: &str, msg: impl Into<String>) -> provider::RentInstancesResult {
    provider::RentInstancesResult {
        offer_id: offer_id.to_owned(),
        outcome: Some(provider::rent_instances_result::Outcome::from(common::Error {
            code: code.to_owned(),
            message: msg.into(),
            ..Default::default()
        })),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_error_codes_are_stable() {
        assert_eq!(ProviderError::NotFound("x".into()).code(), "NOT_FOUND");
        assert_eq!(
            ProviderError::Unavailable("x".into()).code(),
            "UNAVAILABLE"
        );
    }

    #[test]
    fn rent_error_uses_common_error_outcome() {
        let r = rent_error("offer-1", "INVALID_ARGUMENT", "image required");
        assert_eq!(r.offer_id, "offer-1");
        match r.outcome {
            Some(provider::rent_instances_result::Outcome::Error(e)) => {
                assert_eq!(e.code, "INVALID_ARGUMENT");
            }
            other => panic!("expected error outcome, got {other:?}"),
        }
    }
}
