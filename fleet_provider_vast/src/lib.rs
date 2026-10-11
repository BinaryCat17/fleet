// fleet_provider_vast/src/lib.rs
//
#![allow(refining_impl_trait_internal, refining_impl_trait_reachable)] // short `ServiceResult` form

use std::collections::HashMap;

use connectrpc::{ConnectError, ErrorCode, RequestContext, Response, ServiceRequest, ServiceResult};
use futures::StreamExt;
use serde_json::json;

// ─────────────────────── the contract (shared crates) ───────────────────────
//
// Contract types come from the shared crates — not a local `include_generated!()`.
// `common` is compiled once in `fleet-proto-common`; `provider` in
// `fleet-proto-provider` (which references `common` via codegen `extern_path`).
// This crate only writes the VAST anti-corruption translation.

use fleet_proto_common::common::remove_options::MissingPolicy;
use fleet_proto_common::common::{ItemResult, ItemResults, RemoveRequest};
use fleet_proto_common::{item_ack, item_error};
use fleet_proto_provider::provider::{
    Instance, InstanceOffer, InstanceStatus, ListInstancesRequest, ListInstancesResponse, Provider,
    RentInstancesItem, RentInstancesRequest, RentInstancesResponse, RentInstancesResult,
    SearchInstancesRequest, SearchInstancesResponse,
};
// Contract-level error + the provider batch-row builder live in `fleet-proto-provider`.
use fleet_proto_provider::rent_error;
pub use fleet_proto_provider::ProviderError;

// ─────────────────────── the provider ───────────────────────

pub struct VastProvider {
    http: reqwest::Client,
    api_key: String,
    base: String, // e.g. https://console.vast.ai/api/v0
}

impl VastProvider {
    pub fn new() -> anyhow::Result<Self> {
        let api_key = std::env::var("VAST_API_KEY")
            .or_else(|_| std::fs::read_to_string("/run/secrets/vast_api_key").map(|s| s.trim().to_owned()))
            .map_err(|_| anyhow::anyhow!("VAST_API_KEY or /run/secrets/vast_api_key required"))?;
        Ok(Self {
            http: reqwest::Client::builder().timeout(std::time::Duration::from_secs(30)).build()?,
            api_key,
            base: std::env::var("VAST_API_URL").unwrap_or_else(|_| "https://console.vast.ai/api/v0".into()),
        })
    }

    // ── thin upstream layer: one method per vast resource ──

    /// POST /searches — vast "asks" query. Endpoint/fields per current vast docs.
    async fn find_offers(&self, f: Option<&Filter>, limit: u32) -> Result<Vec<InstanceOffer>, ProviderError> {
        let resp = self.post("searches", &offer_query(f)).await?;
        Ok(field_array(&resp, "offers").iter().take(limit as usize).map(map_offer).collect())
    }

    /// GET /instances — the stateless provider's only "memory" for rent replays.
    async fn list(&self) -> Result<Vec<Instance>, ProviderError> {
        let resp = self.get("instances").await?;
        Ok(field_array(&resp, "instances").iter().map(map_instance).collect())
    }

    /// PUT /asks/{id}/rent/ — `onstart_cmd` runs before the image command (vast).
    async fn rent(&self, offer_id: &str, image: &str, onstart_cmd: Option<&str>) -> Result<Instance, ProviderError> {
        let resp = self.put(&format!("asks/{offer_id}/rent/"), &rent_body(image, onstart_cmd)).await?;
        Ok(map_instance(&resp["new_instance"]))
    }

    /// DELETE /instances/{id}/ — a missing id surfaces as `NotFound`; the
    /// Remove handler applies the call's `RemoveOptions.missing` policy.
    async fn destroy(&self, instance_id: &str) -> Result<(), ProviderError> {
        self.delete(&format!("instances/{instance_id}/")).await
    }

    // ── per-item helpers for the batch RPCs (contract-level, not upstream) ──

    /// Rent one requested item. `prior` (if any) is the replay case: the offer is
    /// already ours, so return the existing instance and rent nothing — this is
    /// what makes `rent_instances` idempotent (first call wins on entrypoint_script).
    async fn rent_one(&self, item: RentInstancesItem, prior: Option<Instance>) -> RentInstancesResult {
        if let Some(inst) = prior {
            return RentInstancesResult { offer_id: item.offer_id, outcome: inst.into(), ..Default::default() };
        }
        if item.image.is_empty() {
            return rent_error(&item.offer_id, "INVALID_ARGUMENT", "image is required");
        }
        match self.rent(&item.offer_id, &item.image, item.entrypoint_script.as_deref()).await {
            Ok(inst) => RentInstancesResult { offer_id: item.offer_id, outcome: inst.into(), ..Default::default() },
            // UNAVAILABLE per item: caller may safely retry the WHOLE call
            // (idempotency_level = IDEMPOTENT makes that legal).
            Err(e) => rent_error(&item.offer_id, e.code(), e.to_string()),
        }
    }

    /// Remove one id honoring `dry_run` and the missing policy (already-gone = success).
    async fn remove_one(&self, id: String, dry_run: bool, error_on_missing: bool) -> ItemResult {
        if dry_run {
            return item_ack(id, "NO ACTION", "dry_run: nothing mutated");
        }
        match self.destroy(&id).await {
            Ok(()) => item_ack(id, "DONE", ""),
            Err(ProviderError::NotFound(m)) if error_on_missing => item_error(id, "NOT_FOUND", m),
            Err(ProviderError::NotFound(_)) => item_ack(id, "NO ACTION", "already absent"),
            Err(e) => item_error(id, "UNAVAILABLE", e.to_string()),
        }
    }

    // ── reqwest plumbing: bearer auth + upstream error mapping ──
    // Unavailable on network/5xx, NotFound on 404, InvalidArgument on 400/422,
    // FailedPrecondition otherwise (upstream refused: 401/402/409/...).

    async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> Result<serde_json::Value, ProviderError> {
        let req = self
            .http
            .request(method, format!("{}/{}", self.base, path))
            .bearer_auth(&self.api_key);
        let req = match body {
            Some(body) => req.json(body),
            None => req,
        };
        let resp = req.send().await.map_err(|e| ProviderError::Unavailable(e.to_string()))?;

        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(ProviderError::NotFound(path.to_owned()));
        }
        if !status.is_success() {
            let msg = format!("{} {}: {}", status.as_u16(), status.canonical_reason().unwrap_or(""), path);
            return Err(match status.as_u16() {
                400 | 422 => ProviderError::InvalidArgument(msg),
                500..=599 => ProviderError::Unavailable(msg),
                _ => ProviderError::FailedPrecondition(msg),
            });
        }

        let text = resp.text().await.map_err(|e| ProviderError::Unavailable(e.to_string()))?;
        if text.is_empty() {
            return Ok(serde_json::Value::Null); // e.g. DELETE with an empty body
        }
        serde_json::from_str(&text).map_err(|e| ProviderError::Unavailable(format!("bad upstream JSON: {e}")))
    }

    async fn get(&self, path: &str) -> Result<serde_json::Value, ProviderError> {
        self.call(reqwest::Method::GET, path, None).await
    }

    async fn post(&self, path: &str, body: &serde_json::Value) -> Result<serde_json::Value, ProviderError> {
        self.call(reqwest::Method::POST, path, Some(body)).await
    }

    async fn put(&self, path: &str, body: &serde_json::Value) -> Result<serde_json::Value, ProviderError> {
        self.call(reqwest::Method::PUT, path, Some(body)).await
    }

    async fn delete(&self, path: &str) -> Result<(), ProviderError> {
        self.call(reqwest::Method::DELETE, path, None).await.map(|_| ())
    }
}

// alias used above to keep the filter code readable
type Filter = fleet_proto_provider::provider::SearchInstancesFilter;

// ─────────────────────── THE CONTRACT ───────────────────────

impl Provider for VastProvider {
    // 1. search — pure read
    async fn search_instances(
        &self,
        _ctx: RequestContext,
        req: ServiceRequest<'_, SearchInstancesRequest>,
    ) -> ServiceResult<SearchInstancesResponse> {
        let msg = req.to_owned_message(); // (views allow zero-copy access; owned form for clarity)
        let limit = if msg.limit == 0 { 25 } else { msg.limit };
        let offers = self.find_offers(msg.filter.as_option(), limit).await?;
        Response::ok(SearchInstancesResponse { offers, ..Default::default() })
    }

    // 2. rent — IDEMPOTENT by construction (list-then-rent keyed on offer_id)
    async fn rent_instances(
        &self,
        _ctx: RequestContext,
        req: ServiceRequest<'_, RentInstancesRequest>,
    ) -> ServiceResult<RentInstancesResponse> {
        let items: Vec<RentInstancesItem> = req.to_owned_message().items;
        if items.is_empty() {
            return Err(ConnectError::new(ErrorCode::InvalidArgument, "items must not be empty"));
        }

        // One upstream list up-front: the replay "memory" (see contract comment).
        let existing: HashMap<String, Instance> = self.list().await?
            .into_iter()
            .filter_map(|i| (!i.offer_id.is_empty()).then(|| (i.offer_id.clone(), i)))
            .collect();

        // Per-item outcomes, in request order (`buffered`, not `buffer_unordered`);
        // fan-out capped at 8 concurrent upstream calls.
        let results = futures::stream::iter(items.into_iter().map(|item| {
            let prior = existing.get(&item.offer_id).cloned();
            let this = self;
            async move { this.rent_one(item, prior).await }
        }))
        .buffered(8)
        .collect()
        .await;

        Response::ok(RentInstancesResponse { results, ..Default::default() })
    }

    // 3. list — pure read, verbatim upstream truth
    async fn list_instances(
        &self,
        _ctx: RequestContext,
        _req: ServiceRequest<'_, ListInstancesRequest>,
    ) -> ServiceResult<ListInstancesResponse> {
        let instances = self.list().await?;
        Response::ok(ListInstancesResponse { instances, ..Default::default() })
    }

    // 4. remove — idempotent: already-gone = success (per RemoveOptions.missing)
    async fn remove_instances(
        &self,
        _ctx: RequestContext,
        req: ServiceRequest<'_, RemoveRequest>,
    ) -> ServiceResult<ItemResults> {
        let msg = req.to_owned_message();
        let opts = msg.options.into_option().unwrap_or_default();
        let dry_run = opts.dry_run;
        let error_on_missing = opts.missing.as_known() == Some(MissingPolicy::MISSING_POLICY_ERROR);

        let results = futures::stream::iter(msg.ids.into_iter().map(|id| {
            let this = self;
            async move { this.remove_one(id, dry_run, error_on_missing).await }
        }))
        .buffered(8)
        .collect()
        .await;

        Response::ok(ItemResults { results, ..Default::default() })
    }
}

// ─────────────────────── conversion layer: JSON ⇄ contract messages ───────────────────────

/// SearchInstancesFilter → vast "asks" query.
fn offer_query(f: Option<&Filter>) -> serde_json::Value {
    json!({
        "verified": { "eq": true },
        "external": { "eq": false },
        "rentable": { "eq": true },
        "num_gpus":     f.and_then(|f| f.num_gpus).map(|n| json!({ "eq": n })),
        "gpu_name":     f.and_then(|f| f.gpu_name.as_deref()).map(|s| json!({ "eq": s })),
        "cpu_cores":    f.and_then(|f| f.min_cpu_cores).map(|n| json!({ "gte": n })),
        "ram":          f.and_then(|f| f.min_ram_mb).map(|n| json!({ "gte": n })),
        "disk_space":   f.and_then(|f| f.min_disk_gb).map(|n| json!({ "gte": n })),
        "dph_total":    f.and_then(|f| f.max_price_per_hour).map(|d| json!({ "lte": d })),
        "geolocation":  f.and_then(|f| f.location.as_deref()).map(|s| json!({ "eq": s })),
    })
}

/// Rent request → vast rent body (`onstart_cmd` runs before the image command).
fn rent_body(image: &str, onstart_cmd: Option<&str>) -> serde_json::Value {
    json!({
        "client_id": "me",
        "image": image,
        "onstart_cmd": onstart_cmd.unwrap_or(""),
        "runtype": "ssh",        // vast needs a runtype; ssh-mode without keys = script-only access
        "disk": 16,
    })
}

// ── upstream JSON → contract (field accessors + response mappers) ──

fn field_array<'a>(v: &'a serde_json::Value, key: &str) -> &'a [serde_json::Value] {
    v[key].as_array().map(Vec::as_slice).unwrap_or(&[])
}

/// Scalar → String: vast ids may be numbers or strings; missing/null → "".
fn field_str(v: &serde_json::Value, key: &str) -> String {
    match &v[key] {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn field_u64(v: &serde_json::Value, key: &str) -> u64 {
    v[key].as_u64().unwrap_or(0)
}

fn field_f64(v: &serde_json::Value, key: &str) -> f64 {
    v[key].as_f64().unwrap_or(0.0)
}

/// vast `actual_status` → the contract's InstanceStatus.
fn map_status(raw: &str) -> InstanceStatus {
    match raw {
        "running" => InstanceStatus::Running,
        "loading" | "created" | "provisioning" => InstanceStatus::Pending,
        "stopped" | "offline" => InstanceStatus::Stopped,
        "exited" => InstanceStatus::Error,
        _ => InstanceStatus::Unspecified,
    }
}

fn map_offer(v: &serde_json::Value) -> InstanceOffer {
    InstanceOffer {
        id: field_str(v, "id"),
        gpu_name: field_str(v, "gpu_name"),
        num_gpus: field_u64(v, "num_gpus") as u32,
        cpu_cores: field_u64(v, "cpu_cores") as u32,
        ram_mb: (field_f64(v, "ram") / 1e6) as u64,        // vast reports bytes
        disk_gb: (field_f64(v, "disk_space") / 1e3) as u64,
        location: field_str(v, "geolocation"),
        price_per_hour: field_f64(v, "dph_total"),
        reliability: field_f64(v, "reliability2"),
        ..Default::default()
    }
}

fn map_instance(v: &serde_json::Value) -> Instance {
    let raw = field_str(v, "actual_status");
    Instance {
        id: field_str(v, "id"),
        offer_id: field_str(v, "ask_contract_id"),
        status: map_status(&raw).into(),
        provider_status: raw,   // verbatim, per contract
        gpu_name: field_str(v, "gpu_name"),
        num_gpus: field_u64(v, "num_gpus") as u32,
        price_per_hour: field_f64(v, "dph_total"),
        location: field_str(v, "geolocation"),
        started_at_unix_ms: (field_f64(v, "start_date") * 1000.0) as i64,
        ..Default::default()
    }
}
