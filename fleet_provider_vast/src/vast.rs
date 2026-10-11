// fleet_provider_vast/src/vast.rs

use serde_json::json;

use connectrpc::{ConnectError, ErrorCode};
use fleet_proto_provider::provider::{Instance, InstanceOffer, InstanceStatus, SearchInstancesFilter};

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
    pub(crate) async fn find_offers(
        &self,
        f: Option<&SearchInstancesFilter>,
        limit: u32,
    ) -> Result<Vec<InstanceOffer>, ConnectError> {
        let resp = self.post("searches", &offer_query(f)).await?;
        Ok(field_array(&resp, "offers").iter().take(limit as usize).map(map_offer).collect())
    }

    /// GET /instances — the stateless provider's only "memory" for rent replays.
    pub(crate) async fn list(&self) -> Result<Vec<Instance>, ConnectError> {
        let resp = self.get("instances").await?;
        Ok(field_array(&resp, "instances").iter().map(map_instance).collect())
    }

    /// PUT /asks/{id}/rent/ — `onstart_cmd` runs before the image command (vast).
    pub(crate) async fn rent(&self, offer_id: &str, image: &str, onstart_cmd: Option<&str>) -> Result<Instance, ConnectError> {
        let resp = self.put(&format!("asks/{offer_id}/rent/"), &rent_body(image, onstart_cmd)).await?;
        Ok(map_instance(&resp["new_instance"]))
    }

    /// DELETE /instances/{id}/ — a missing id surfaces as `not_found`; the remove
    /// handler applies the call's missing policy.
    pub(crate) async fn destroy(&self, instance_id: &str) -> Result<(), ConnectError> {
        self.delete(&format!("instances/{instance_id}/")).await
    }

    // ── reqwest plumbing: bearer auth + upstream error mapping ──

    async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> Result<serde_json::Value, ConnectError> {
        let req = self
            .http
            .request(method, format!("{}/{}", self.base, path))
            .bearer_auth(&self.api_key);
        let req = match body {
            Some(body) => req.json(body),
            None => req,
        };
        let resp = req.send().await.map_err(|e| ConnectError::new(ErrorCode::Unavailable, e.to_string()))?;

        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(ConnectError::new(ErrorCode::NotFound, path));
        }
        if !status.is_success() {
            let msg = format!("{} {}: {}", status.as_u16(), status.canonical_reason().unwrap_or(""), path);
            return Err(match status.as_u16() {
                400 | 422 => ConnectError::new(ErrorCode::InvalidArgument, msg),
                500..=599 => ConnectError::new(ErrorCode::Unavailable, msg),
                _ => ConnectError::new(ErrorCode::FailedPrecondition, msg),
            });
        }

        let text = resp.text().await.map_err(|e| ConnectError::new(ErrorCode::Unavailable, e.to_string()))?;
        if text.is_empty() {
            return Ok(serde_json::Value::Null); // e.g. DELETE with an empty body
        }
        serde_json::from_str(&text).map_err(|e| ConnectError::new(ErrorCode::Unavailable, format!("bad upstream JSON: {e}")))
    }

    async fn get(&self, path: &str) -> Result<serde_json::Value, ConnectError> {
        self.call(reqwest::Method::GET, path, None).await
    }

    async fn post(&self, path: &str, body: &serde_json::Value) -> Result<serde_json::Value, ConnectError> {
        self.call(reqwest::Method::POST, path, Some(body)).await
    }

    async fn put(&self, path: &str, body: &serde_json::Value) -> Result<serde_json::Value, ConnectError> {
        self.call(reqwest::Method::PUT, path, Some(body)).await
    }

    async fn delete(&self, path: &str) -> Result<(), ConnectError> {
        self.call(reqwest::Method::DELETE, path, None).await.map(|_| ())
    }
}

// ─────────────────────── conversion layer: JSON ⇄ contract messages ───────────────────────

/// SearchInstancesFilter → vast "asks" query.
fn offer_query(f: Option<&SearchInstancesFilter>) -> serde_json::Value {
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
