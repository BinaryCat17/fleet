// fleet_provider_vast/src/lib.rs

#![allow(refining_impl_trait_internal, refining_impl_trait_reachable)] // short `ServiceResult` form

use std::collections::HashMap;

use connectrpc::{
    ConnectError, ErrorCode, RequestContext, Response, ServiceRequest, ServiceResult, ServiceStream,
};

use fleet_proto_common::common::remove_options::MissingPolicy;
use fleet_proto_common::common::{ItemResult, ItemResultCode, RemoveOptions, RemoveRequest};
use fleet_proto_provider::provider::{
    Instance, InstanceOffer, ListInstancesRequest, Provider, RentInstancesItem, RentInstancesRequest,
    SearchInstancesRequest,
};
use fleet_proto_provider::server::{
    admit_list_instances, admit_remove_instances, admit_rent_instances, admit_search_instances,
    map_remove, map_rent, stream_rows,
};

mod vast;
pub use vast::VastProvider;

// ─────────────────────── (feature)_one: the domain logic ───────────────────────

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
    p.rent(&item.offer_id, &item.image, item.entrypoint_script.as_deref()).await
}

async fn list_one(p: &VastProvider) -> Result<Vec<Instance>, ConnectError> {
    p.list().await
}

async fn remove_one(
    p: &VastProvider,
    id: String,
    opts: RemoveOptions,
) -> Result<ItemResultCode, ConnectError> {
    if opts.dry_run {
        return Ok(ItemResultCode::ITEM_RESULT_CODE_NO_ACTION); // dry_run: nothing mutated
    }
    let error_on_missing = opts.missing.as_known() == Some(MissingPolicy::MISSING_POLICY_ERROR);
    match p.destroy(&id).await {
        Ok(()) => Ok(ItemResultCode::ITEM_RESULT_CODE_DONE),
        Err(e) if e.code == ErrorCode::NotFound && !error_on_missing => {
            Ok(ItemResultCode::ITEM_RESULT_CODE_NO_ACTION) // already absent
        }
        Err(e) => Err(e),
    }
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
        Response::ok(stream_rows(search_one(self, &msg).await?))
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
        Response::ok(map_rent(&msg, |item| rent_one(self, item, &rented)).await)
    }

    // 3. list — pure read, verbatim upstream truth; instances stream out one row at a time
    async fn list_instances(
        &self,
        _ctx: RequestContext,
        req: ServiceRequest<'_, ListInstancesRequest>,
    ) -> ServiceResult<ServiceStream<Instance>> {
        let msg = req.to_owned_message();
        admit_list_instances(&msg)?;
        Response::ok(stream_rows(list_one(self).await?))
    }

    // 4. remove — idempotent: already-gone = success (per RemoveOptions.missing)
    async fn remove_instances(
        &self,
        _ctx: RequestContext,
        req: ServiceRequest<'_, RemoveRequest>,
    ) -> ServiceResult<ServiceStream<ItemResult>> {
        let msg = req.to_owned_message();
        admit_remove_instances(&msg)?;
        Response::ok(map_remove(&msg, |id, opts| remove_one(self, id, opts)).await)
    }
}
