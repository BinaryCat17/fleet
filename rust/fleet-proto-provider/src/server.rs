//! Server-side helpers for `proto/provider.proto` — the bridge to the domain

use std::future::Future;

use connectrpc::{ConnectError, ErrorCode, ServiceStream};
use futures::StreamExt;

use fleet_proto_common::common::{ItemResult, ItemResultCode, RemoveOptions, RemoveRequest};
use fleet_proto_common::item;

use super::provider::{
    Instance, ListInstancesRequest, RentInstancesItem, RentInstancesRequest, SearchInstancesRequest,
};

// ─────────────────────── admission: the gate to domain logic ───────────────────────

pub fn admit_search_instances(_msg: &SearchInstancesRequest) -> Result<(), ConnectError> {
    Ok(())
}

pub fn admit_rent_instances(msg: &RentInstancesRequest) -> Result<(), ConnectError> {
    if msg.items.is_empty() {
        return Err(ConnectError::new(ErrorCode::InvalidArgument, "items must not be empty"));
    }
    for (i, item) in msg.items.iter().enumerate() {
        if item.offer_id.is_empty() {
            return Err(ConnectError::new(
                ErrorCode::InvalidArgument,
                format!("items[{i}]: offer_id is required"),
            ));
        }
        if item.image.is_empty() {
            return Err(ConnectError::new(
                ErrorCode::InvalidArgument,
                format!("items[{i}] (offer '{}'): image is required", item.offer_id),
            ));
        }
    }
    Ok(())
}

pub fn admit_list_instances(_msg: &ListInstancesRequest) -> Result<(), ConnectError> {
    Ok(())
}

pub fn admit_remove_instances(msg: &RemoveRequest) -> Result<(), ConnectError> {
    if msg.ids.is_empty() {
        return Err(ConnectError::new(ErrorCode::InvalidArgument, "ids must not be empty"));
    }
    for (i, id) in msg.ids.iter().enumerate() {
        if id.is_empty() {
            return Err(ConnectError::new(
                ErrorCode::InvalidArgument,
                format!("ids[{i}]: id must not be empty"),
            ));
        }
    }
    Ok(())
}

// ─────────────────── batch stage: (feature)_one over the batch ───────────────────

pub fn stream_rows<T: Send + 'static>(rows: Vec<T>) -> ServiceStream<T> {
    Box::pin(futures::stream::iter(rows.into_iter().map(Ok::<_, ConnectError>)))
}

pub async fn map_rent<F, Fut>(msg: &RentInstancesRequest, one: F) -> ServiceStream<Instance>
where
    F: FnMut(RentInstancesItem) -> Fut,
    Fut: Future<Output = Result<Instance, ConnectError>>,
{
    let offer_ids: Vec<String> = msg.items.iter().map(|item| item.offer_id.clone()).collect();
    let outcomes = futures::stream::iter(msg.items.clone().into_iter().map(one))
        .buffered(8) // bounded fan-out; outcomes stay in request order
        .collect::<Vec<_>>()
        .await;

    let mut rows: Vec<Result<Instance, ConnectError>> = Vec::with_capacity(outcomes.len());
    for (offer_id, outcome) in offer_ids.into_iter().zip(outcomes) {
        match outcome {
            Ok(mut instance) => {
                if instance.offer_id.is_empty() {
                    instance.offer_id = offer_id; // key the row back to the request item
                }
                rows.push(Ok(instance));
            }
            Err(e) => {
                rows.push(Err(e)); // ends the stream
                break;
            }
        }
    }
    Box::pin(futures::stream::iter(rows))
}

pub async fn map_remove<F, Fut>(msg: &RemoveRequest, mut one: F) -> ServiceStream<ItemResult>
where
    F: FnMut(String, RemoveOptions) -> Fut,
    Fut: Future<Output = Result<ItemResultCode, ConnectError>>,
{
    let opts = msg.options.as_option().cloned().unwrap_or_default(); // call policy, shared by every id
    let ids = msg.ids.clone();
    let outcomes = futures::stream::iter(ids.iter().cloned().map(|id| one(id, opts.clone())))
        .buffered(8) // bounded fan-out; outcomes stay in request order
        .collect::<Vec<_>>()
        .await;

    let mut rows: Vec<Result<ItemResult, ConnectError>> = Vec::with_capacity(outcomes.len());
    for (id, outcome) in ids.into_iter().zip(outcomes) {
        match outcome {
            Ok(code) => rows.push(Ok(item(id, code))), // row = the id + what happened to it
            Err(e) => {
                rows.push(Err(e)); // ends the stream
                break;
            }
        }
    }
    Box::pin(futures::stream::iter(rows))
}
