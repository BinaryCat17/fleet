use connectrpc::client::{ClientConfig, Http2Connection};
use fleet_host::proto::provider::{ListInstancesRequest, ProviderClient};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── step 1: transport ─────────────────────────────────────────
    let provider_url: http::Uri = std::env::var("PROVIDER_URL")
        .expect("PROVIDER_URL, e.g. http://fleet_provider_vast:50051")
        .parse()?;

    let transport = Http2Connection::connect_plaintext(provider_url.clone())
        .await?
        .shared(8);

    // ── step 2: config ────────────────────────────────────────────
    let config = ClientConfig::new(provider_url)
        .with_default_timeout(Duration::from_secs(10));

    // ── step 3: generated client ──────────────────────────────────
    let client = ProviderClient::new(transport, config);         // 🔄 VARIABLE: which service client

    // ── step 4: smoke call — proves client ↔ server end-to-end ───
    match client.list_instances(ListInstancesRequest::default()).await {
        // domain outcome travels in-band (errors-as-data)
        Ok(resp) => {
            let resp = resp.into_owned();
            match resp.error.as_option() {
                Some(e) => println!("provider reachable; list_instances failed: {} {}", e.code, e.message),
                None => println!("provider online: {} instance(s)", resp.instances.len()),
            }
        }
        // transport/admission failure — the call was not accepted
        Err(e) => println!("provider reachable; list_instances: {e}"),
    }

    Ok(())
}
