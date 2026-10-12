//! `fleet-sdk` — Rust SDK over the language-neutral contracts in `proto/`.
//!
//! `common` and `provider` are the generated Buffa/ConnectRPC types (one
//! codegen pass, so `provider` references `common` in-crate). The hand-written
//! glue: `item` (result rows), `map_rows` (generic batch stage) and
//! `DESCRIPTOR_SET` (reflection). Request admission stays with the service
//! implementation (`fleet-provider-vast`) — it is the gate to domain logic.

include!(concat!(env!("OUT_DIR"), "/_connectrpc.rs"));

use std::future::Future;

use connectrpc::{ConnectError, ServiceStream};
use futures::StreamExt;

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

// ─────────────────── batch stage: (feature)_one over the batch ───────────────────

/// The batch stage, generically: run `one` over every item of the batch and
/// stream one row per item. Each `one` turns one request item into its final
/// result row (keying it back to the request itself); fan-out is bounded and
/// outcomes stay in request order; the first error ends the stream (a failed
/// item is a connectrpc error on the stream, not a row).
///
/// With no per-item work — `one = |row| ready(Ok(row))` — the already-computed
/// rows simply stream out as-is.
pub async fn map_rows<I, T, F, Fut>(items: Vec<I>, one: F) -> ServiceStream<T>
where
    F: FnMut(I) -> Fut,
    Fut: Future<Output = Result<T, ConnectError>>,
    T: Send + 'static,
{
    let outcomes = futures::stream::iter(items.into_iter().map(one))
        .buffered(8) // bounded fan-out; outcomes stay in request order
        .collect::<Vec<_>>()
        .await;

    let mut rows: Vec<Result<T, ConnectError>> = Vec::with_capacity(outcomes.len());
    for outcome in outcomes {
        match outcome {
            Ok(row) => rows.push(Ok(row)), // one row per batch item
            Err(e) => {
                rows.push(Err(e)); // ends the stream
                break;
            }
        }
    }
    Box::pin(futures::stream::iter(rows))
}

/// File descriptor set of the contracts (gRPC server reflection).
pub const DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/fleet.fds.bin"));
