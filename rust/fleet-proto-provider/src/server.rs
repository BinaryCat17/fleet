//! Server-side helpers for `proto/provider.proto` — the bridge to the domain

use std::future::Future;

use connectrpc::{ConnectError, ErrorCode};
use futures::StreamExt;

use fleet_proto_common::common::{
    Ack, Error, ItemResult, ItemResults, RemoveOptions, RemoveRequest,
};

use super::provider::{
    self, Instance, ListInstancesRequest, RentInstancesItem, RentInstancesRequest,
    RentInstancesResponse, RentInstancesResult, SearchInstancesRequest,
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

pub async fn map_rent<F, Fut>(msg: &RentInstancesRequest, one: F) -> RentInstancesResponse
where
    F: FnMut(RentInstancesItem) -> Fut,
    Fut: Future<Output = Result<Instance, Error>>,
{
    let offer_ids: Vec<String> = msg.items.iter().map(|item| item.offer_id.clone()).collect();
    let outcomes = futures::stream::iter(msg.items.clone().into_iter().map(one))
        .buffered(8)
        .collect::<Vec<_>>()
        .await;

    let results = offer_ids
        .into_iter()
        .zip(outcomes)
        .map(|(offer_id, outcome)| RentInstancesResult {
            offer_id,
            outcome: Some(match outcome {
                Ok(inst) => provider::rent_instances_result::Outcome::from(inst),
                Err(e) => provider::rent_instances_result::Outcome::from(e),
            }),
            ..Default::default()
        })
        .collect();
    RentInstancesResponse { results, ..Default::default() }
}

pub async fn map_remove<F, Fut>(msg: &RemoveRequest, mut one: F) -> ItemResults
where
    F: FnMut(String, RemoveOptions) -> Fut,
    Fut: Future<Output = Result<Ack, Error>>,
{
    let opts = msg.options.as_option().cloned().unwrap_or_default(); // call policy, shared by every id
    let ids = msg.ids.clone();
    let outcomes = futures::stream::iter(ids.iter().cloned().map(|id| one(id, opts.clone())))
        .buffered(8)
        .collect::<Vec<_>>()
        .await;

    let results = ids
        .into_iter()
        .zip(outcomes)
        .map(|(id, outcome)| ItemResult {
            id,
            outcome: match outcome {
                Ok(a) => a.into(),
                Err(e) => e.into(),
            },
            ..Default::default()
        })
        .collect();
    ItemResults { results, ..Default::default() }
}
