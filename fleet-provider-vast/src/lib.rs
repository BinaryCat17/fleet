// fleet-provider-vast/src/lib.rs

#![allow(refining_impl_trait_internal, refining_impl_trait_reachable)] // short `ServiceResult` form

use std::collections::HashMap;
use std::future::ready;

use connectrpc::{
    ConnectError, ErrorCode, RequestContext, Response, ServiceRequest, ServiceResult, ServiceStream,
};

use fleet_sdk::common::remove_options::MissingPolicy;
use fleet_sdk::common::{ItemResult, ItemResultCode, RemoveOptions, RemoveRequest};
use fleet_sdk::provider::{
    Instance, InstanceOffer, ListInstancesRequest, Provider, RentInstancesItem, RentInstancesRequest,
    SearchInstancesRequest,
};
use fleet_sdk::{item, map_rows};

mod vast;
pub use vast::VastProvider;

// ────────────── (feature)_one: the domain logic — one batch item → one row ──────────────

async fn search_one(
    p: &VastProvider,
    msg: &SearchInstancesRequest,
) -> Result<Vec<InstanceOffer>, ConnectError> {
    let limit = if msg.limit == 0 { 25 } else { msg.limit }; // 0 = provider default
    p.find_offers(msg.filter.as_option(), limit).await
}

async fn rent_one(
    p: &VastProvider,
    item: RentInstancesItem,
    rented: &HashMap<String, Instance>,
) -> Result<Instance, ConnectError> {
    if let Some(inst) = rented.get(&item.offer_id) {
        return Ok(inst.clone()); // idempotent replay: this offer is already rented
    }
    let mut instance = p.rent(&item.offer_id, &item.image, item.entrypoint_script.as_deref()).await?;
    if instance.offer_id.is_empty() {
        instance.offer_id = item.offer_id; // key the row back to the request item
    }
    Ok(instance)
}

async fn list_one(p: &VastProvider) -> Result<Vec<Instance>, ConnectError> {
    p.list().await
}

async fn remove_one(
    p: &VastProvider,
    id: String,
    opts: RemoveOptions,
) -> Result<ItemResult, ConnectError> {
    let code = if opts.dry_run {
        ItemResultCode::ITEM_RESULT_CODE_NO_ACTION // dry_run: nothing mutated
    } else {
        let error_on_missing = opts.missing.as_known() == Some(MissingPolicy::MISSING_POLICY_ERROR);
        match p.destroy(&id).await {
            Ok(()) => ItemResultCode::ITEM_RESULT_CODE_DONE,
            Err(e) if e.code == ErrorCode::NotFound && !error_on_missing => {
                ItemResultCode::ITEM_RESULT_CODE_NO_ACTION // already absent
            }
            Err(e) => return Err(e),
        }
    };
    Ok(item(id, code)) // row = the id + what happened to it
}

// ─────────────────────── admission: the gate to domain logic ───────────────────────

fn admit_search_instances(_msg: &SearchInstancesRequest) -> Result<(), ConnectError> {
    Ok(())
}

fn admit_rent_instances(msg: &RentInstancesRequest) -> Result<(), ConnectError> {
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

fn admit_list_instances(_msg: &ListInstancesRequest) -> Result<(), ConnectError> {
    Ok(())
}

fn admit_remove_instances(msg: &RemoveRequest) -> Result<(), ConnectError> {
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

// ─────────────────────── Server implementation ───────────────────────

impl Provider for VastProvider {
    // 1. search — pure read; offers stream out one row at a time
    async fn search_instances(
        &self,
        _ctx: RequestContext,
        req: ServiceRequest<'_, SearchInstancesRequest>,
    ) -> ServiceResult<ServiceStream<InstanceOffer>> {
        let msg = req.to_owned_message();
        admit_search_instances(&msg)?;
        Response::ok(map_rows(search_one(self, &msg).await?, |row| ready(Ok(row))).await)
    }

    // 2. rent — IDEMPOTENT by construction (list-then-rent keyed on offer_id).
    //    One result row per rented item; a failed item is a connectrpc error
    //    on the stream, not a row.
    async fn rent_instances(
        &self,
        _ctx: RequestContext,
        req: ServiceRequest<'_, RentInstancesRequest>,
    ) -> ServiceResult<ServiceStream<Instance>> {
        let msg = req.to_owned_message();
        admit_rent_instances(&msg)?;

        let rented: HashMap<String, Instance> = match self.list().await {
            Ok(list) => list
                .into_iter()
                .filter(|i| !i.offer_id.is_empty())
                .map(|i| (i.offer_id.clone(), i))
                .collect(),
            // pre-flight failed → call-level failure (connectrpc error, before any row)
            Err(e) => return Err(e),
        };
        Response::ok(map_rows(msg.items.clone(), |item| rent_one(self, item, &rented)).await)
    }

    // 3. list — pure read, verbatim upstream truth; instances stream out one row at a time
    async fn list_instances(
        &self,
        _ctx: RequestContext,
        req: ServiceRequest<'_, ListInstancesRequest>,
    ) -> ServiceResult<ServiceStream<Instance>> {
        let msg = req.to_owned_message();
        admit_list_instances(&msg)?;
        Response::ok(map_rows(list_one(self).await?, |row| ready(Ok(row))).await)
    }

    // 4. remove — idempotent: already-gone = success (per RemoveOptions.missing)
    async fn remove_instances(
        &self,
        _ctx: RequestContext,
        req: ServiceRequest<'_, RemoveRequest>,
    ) -> ServiceResult<ServiceStream<ItemResult>> {
        let msg = req.to_owned_message();
        admit_remove_instances(&msg)?;
        let opts = msg.options.as_option().cloned().unwrap_or_default(); // call policy, shared by every id
        Response::ok(map_rows(msg.ids.clone(), |id| remove_one(self, id, opts.clone())).await)
    }
}
