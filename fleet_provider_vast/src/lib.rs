// fleet_provider_vast/src/lib.rs
//
// Implements the Provider contract (proto/provider.proto) statelessly
// against the vast.ai REST API. No local state: all truth comes upstream.
#![allow(refining_impl_trait_internal, refining_impl_trait_reachable)] // short `ServiceResult` form

use std::collections::HashMap;

use connectrpc::{ConnectError, ErrorCode, RequestContext, Response, ServiceRequest, ServiceResult};
use futures::StreamExt;
use serde_json::json;

// ─────────────────────── the contract (generated) ───────────────────────

pub mod proto {
    connectrpc::include_generated!();
}

use proto::common::remove_options::MissingPolicy;
use proto::common::{Ack, Error as ItemError, ItemResult, ItemResults, RemoveRequest};
use proto::provider::{
    Instance, InstanceOffer, InstanceStatus, ListInstancesRequest, ListInstancesResponse, Provider,
    RentInstancesItem, RentInstancesRequest, RentInstancesResponse, RentInstancesResult,
    SearchInstancesRequest, SearchInstancesResponse,
};

// ─────────────────────── provider error type (FaaS shape) ───────────────────────

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

impl From<ProviderError> for ConnectError {
    fn from(e: ProviderError) -> Self {
        match e {
            ProviderError::NotFound(m) => ConnectError::new(ErrorCode::NotFound, m),
            ProviderError::InvalidArgument(m) => ConnectError::new(ErrorCode::InvalidArgument, m),
            ProviderError::Unavailable(m) => ConnectError::new(ErrorCode::Unavailable, m),
            ProviderError::FailedPrecondition(m) => ConnectError::new(ErrorCode::FailedPrecondition, m),
        }
    }
}

// per-item rows for batch results (oneof outcome is filled via the generated `From` impls)
fn rent_error(offer_id: &str, code: &str, msg: impl Into<String>) -> RentInstancesResult {
    RentInstancesResult {
        offer_id: offer_id.to_owned(),
        outcome: ItemError { code: code.to_owned(), message: msg.into(), ..Default::default() }.into(),
        ..Default::default()
    }
}

fn item_ack(id: String, code: &str, message: &str) -> ItemResult {
    ItemResult {
        id,
        outcome: Ack { code: code.to_owned(), message: message.to_owned(), ..Default::default() }.into(),
        ..Default::default()
    }
}

fn item_error(id: String, code: &str, msg: impl Into<String>) -> ItemResult {
    ItemResult {
        id,
        outcome: ItemError { code: code.to_owned(), message: msg.into(), ..Default::default() }.into(),
        ..Default::default()
    }
}

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
        let query = json!({
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
        });
        let resp: serde_json::Value = self.post("searches", &query).await?;
        let offers = resp["offers"].as_array().cloned().unwrap_or_default();
        Ok(offers.iter().take(limit as usize).map(map_offer).collect())
    }

    /// GET /instances — the stateless provider's only "memory" for rent replays.
    async fn list(&self) -> Result<Vec<Instance>, ProviderError> {
        let resp: serde_json::Value = self.get("instances").await?;
        let arr = resp["instances"].as_array().cloned().unwrap_or_default();
        Ok(arr.iter().map(map_instance).collect())
    }

    /// PUT /asks/{id}/rent/ — `onstart_cmd` runs before the image command (vast).
    async fn rent(&self, offer_id: &str, image: &str, onstart_cmd: Option<&str>) -> Result<Instance, ProviderError> {
        let body = json!({
            "client_id": "me",
            "image": image,
            "onstart_cmd": onstart_cmd.unwrap_or(""),
            "runtype": "ssh",        // vast needs a runtype; ssh-mode without keys = script-only access
            "disk": 16,
        });
        let resp: serde_json::Value = self.put(&format!("asks/{offer_id}/rent/"), &body).await?;
        Ok(map_instance(&resp["new_instance"]))
    }

    /// DELETE /instances/{id}/ — a missing id surfaces as `NotFound`; the
    /// Remove handler applies the call's `RemoveOptions.missing` policy.
    async fn destroy(&self, instance_id: &str) -> Result<(), ProviderError> {
        self.delete(&format!("instances/{instance_id}/")).await
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
type Filter = proto::provider::SearchInstancesFilter;

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

        // Per-item outcomes, in request order (`buffered`, not `buffer_unordered`).
        let results = futures::stream::iter(items.into_iter().map(|item| {
            let existing = existing.get(&item.offer_id).cloned();
            let this = self; // fan-out capped at 8 concurrent upstream calls
            async move {
                match existing {
                    // CASE replay: offer already rented by us → return it, rent nothing.
                    // entrypoint_script is ignored (first call wins) — keeps us idempotent.
                    Some(inst) => RentInstancesResult {
                        offer_id: item.offer_id,
                        outcome: inst.into(),
                        ..Default::default()
                    },
                    None => {
                        let image = item.image.clone().unwrap_or_else(|| DEFAULT_IMAGE.into());
                        let rented = this
                            .rent(&item.offer_id, &image, item.entrypoint_script.as_deref())
                            .await;
                        match rented {
                            Ok(inst) => RentInstancesResult {
                                offer_id: item.offer_id,
                                outcome: inst.into(),
                                ..Default::default()
                            },
                            Err(ProviderError::NotFound(m)) => rent_error(&item.offer_id, "NOT_FOUND", m),
                            Err(ProviderError::InvalidArgument(m)) => rent_error(&item.offer_id, "INVALID_ARGUMENT", m),
                            Err(ProviderError::FailedPrecondition(m)) => rent_error(&item.offer_id, "FAILED_PRECONDITION", m),
                            // UNAVAILABLE per item: caller may safely retry the WHOLE call
                            // (idempotency_level = IDEMPOTENT makes that legal).
                            Err(ProviderError::Unavailable(m)) => rent_error(&item.offer_id, "UNAVAILABLE", m),
                        }
                    }
                }
            }
        })).buffered(8).collect().await;

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

        let results = futures::stream::iter(msg.ids.into_iter().map(|id| async move {
            if dry_run {
                return item_ack(id, "NO ACTION", "dry_run: nothing mutated");
            }
            let removed = self.destroy(&id).await;
            match removed {
                Ok(()) => item_ack(id, "DONE", ""),
                Err(ProviderError::NotFound(m)) if error_on_missing => item_error(id, "NOT_FOUND", m),
                Err(ProviderError::NotFound(_)) => item_ack(id, "NO ACTION", "already absent"),
                Err(e) => item_error(id, "UNAVAILABLE", e.to_string()),
            }
        })).buffered(8).collect().await;

        Response::ok(ItemResults { results, ..Default::default() })
    }
}

// ─────────────────────── upstream JSON → contract messages ───────────────────────

const DEFAULT_IMAGE: &str = "pytorch/pytorch:latest"; // pick your fleet default

fn map_offer(v: &serde_json::Value) -> InstanceOffer {
    InstanceOffer {
        id: v["id"].to_string().trim_matches('"').to_owned(),
        gpu_name: v["gpu_name"].as_str().unwrap_or("").into(),
        num_gpus: v["num_gpus"].as_u64().unwrap_or(0) as u32,
        cpu_cores: v["cpu_cores"].as_u64().unwrap_or(0) as u32,
        ram_mb: (v["ram"].as_f64().unwrap_or(0.0) / 1e6) as u64,   // vast reports bytes
        disk_gb: (v["disk_space"].as_f64().unwrap_or(0.0) / 1e3) as u64,
        location: v["geolocation"].as_str().unwrap_or("").into(),
        price_per_hour: v["dph_total"].as_f64().unwrap_or(0.0),
        reliability: v["reliability2"].as_f64().unwrap_or(0.0),
        ..Default::default()
    }
}

fn map_instance(v: &serde_json::Value) -> Instance {
    let raw = v["actual_status"].as_str().unwrap_or("");
    Instance {
        id: v["id"].to_string().trim_matches('"').to_owned(),
        offer_id: v["ask_contract_id"].to_string().trim_matches('"').to_owned(),
        status: match raw {
            "running" => InstanceStatus::Running,
            "loading" | "created" | "provisioning" => InstanceStatus::Pending,
            "stopped" | "offline" => InstanceStatus::Stopped,
            "exited" => InstanceStatus::Error,
            _ => InstanceStatus::Unspecified,
        }
        .into(),
        provider_status: raw.into(),   // verbatim, per contract
        gpu_name: v["gpu_name"].as_str().unwrap_or("").into(),
        num_gpus: v["num_gpus"].as_u64().unwrap_or(0) as u32,
        price_per_hour: v["dph_total"].as_f64().unwrap_or(0.0),
        location: v["geolocation"].as_str().unwrap_or("").into(),
        started_at_unix_ms: (v["start_date"].as_f64().unwrap_or(0.0) * 1000.0) as i64,
        ..Default::default()
    }
}
