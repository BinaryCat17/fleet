// fleet_provider_vast/src/lib.rs

#![allow(refining_impl_trait_internal, refining_impl_trait_reachable)] // short `ServiceResult` form

use std::collections::HashMap;

use connectrpc::{RequestContext, Response, ServiceRequest, ServiceResult};

use fleet_proto_common::ack;
use fleet_proto_common::common::remove_options::MissingPolicy;
use fleet_proto_common::common::{
    Ack, AckCode, Error, ErrorCode, ItemResults, RemoveOptions, RemoveRequest,
};
use fleet_proto_provider::provider::{
    Instance, ListInstancesRequest, ListInstancesResponse, Provider, RentInstancesItem,
    RentInstancesRequest, RentInstancesResponse, SearchInstancesRequest, SearchInstancesResponse,
};
use fleet_proto_provider::server::{
    admit_list_instances, admit_remove_instances, admit_rent_instances, admit_search_instances,
    map_remove, map_rent,
};

mod vast;
pub use vast::VastProvider;

// ─────────────────────── (feature)_one: the domain logic ───────────────────────

async fn search_one(
    p: &VastProvider,
    msg: &SearchInstancesRequest,
) -> Result<SearchInstancesResponse, Error> {
    let limit = if msg.limit == 0 { 25 } else { msg.limit }; // 0 = provider default
    Ok(SearchInstancesResponse {
        offers: p.find_offers(msg.filter.as_option(), limit).await?,
        ..Default::default()
    })
}

async fn rent_one(
    p: &VastProvider,
    item: RentInstancesItem,
    rented: &HashMap<String, Instance>,
) -> Result<Instance, Error> {
    if let Some(inst) = rented.get(&item.offer_id) {
        return Ok(inst.clone()); // idempotent replay: this offer is already rented
    }
    p.rent(&item.offer_id, &item.image, item.entrypoint_script.as_deref()).await
}

async fn list_one(p: &VastProvider) -> Result<ListInstancesResponse, Error> {
    Ok(ListInstancesResponse {
        instances: p.list().await?,
        ..Default::default()
    })
}

async fn remove_one(p: &VastProvider, id: String, opts: RemoveOptions) -> Result<Ack, Error> {
    if opts.dry_run {
        return Ok(ack(AckCode::ACK_CODE_NO_ACTION, "dry_run: nothing mutated"));
    }
    let error_on_missing = opts.missing.as_known() == Some(MissingPolicy::MISSING_POLICY_ERROR);
    match p.destroy(&id).await {
        Ok(()) => Ok(ack(AckCode::ACK_CODE_DONE, "")),
        Err(e) if e.code == ErrorCode::ERROR_CODE_NOT_FOUND && !error_on_missing => {
            Ok(ack(AckCode::ACK_CODE_NO_ACTION, "already absent"))
        }
        Err(e) => Err(e),
    }
}

// ─────────────────────── Server implementation ───────────────────────

impl Provider for VastProvider {
    // 1. search — pure read
    async fn search_instances(
        &self,
        _ctx: RequestContext,
        req: ServiceRequest<'_, SearchInstancesRequest>,
    ) -> ServiceResult<SearchInstancesResponse> {
        let msg = req.to_owned_message();
        admit_search_instances(&msg)?;
        let resp = search_one(self, &msg).await?; // `From<Error> for ConnectError` does the rest
        Response::ok(resp)
    }

    // 2. rent — IDEMPOTENT by construction (list-then-rent keyed on offer_id)
    async fn rent_instances(
        &self,
        _ctx: RequestContext,
        req: ServiceRequest<'_, RentInstancesRequest>,
    ) -> ServiceResult<RentInstancesResponse> {
        let msg = req.to_owned_message();
        admit_rent_instances(&msg)?;

        let rented: HashMap<String, Instance> = match self.list().await {
            Ok(list) => list
                .into_iter()
                .filter(|i| !i.offer_id.is_empty())
                .map(|i| (i.offer_id.clone(), i))
                .collect(),
            // pre-flight failed → call-level failure (connectrpc error)
            Err(e) => return Err(e.into()),
        };
        Response::ok(map_rent(&msg, |item| rent_one(self, item, &rented)).await) // batch: rent_one per item
    }

    // 3. list — pure read, verbatim upstream truth
    async fn list_instances(
        &self,
        _ctx: RequestContext,
        req: ServiceRequest<'_, ListInstancesRequest>,
    ) -> ServiceResult<ListInstancesResponse> {
        let msg = req.to_owned_message();
        admit_list_instances(&msg)?;
        let resp = list_one(self).await?; // `From<Error> for ConnectError` does the rest
        Response::ok(resp)
    }

    // 4. remove — idempotent: already-gone = success (per RemoveOptions.missing)
    async fn remove_instances(
        &self,
        _ctx: RequestContext,
        req: ServiceRequest<'_, RemoveRequest>,
    ) -> ServiceResult<ItemResults> {
        let msg = req.to_owned_message();
        admit_remove_instances(&msg)?;
        Response::ok(map_remove(&msg, |id, opts| remove_one(self, id, opts)).await)
    }
}
