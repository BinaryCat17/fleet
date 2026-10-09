use connectrpc::client::{ClientConfig, Http2Connection};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── step 1: transport ─────────────────────────────────────────
    let provider_url: hyper::Uri = std::env::var("PROVIDER_URL")
        .expect("PROVIDER_URL, e.g. http://fleet_provider_vast:50051")?
        .parse()?;

    let transport = Http2Connection::connect_plaintext(provider_url.clone())
        .await?
        .shared(8);

    // ── step 2: config ────────────────────────────────────────────
    let config = ClientConfig::new(provider_url)
        .with_default_timeout(Duration::from_secs(10));

    // ── step 3: generated client ──────────────────────────────────
    let client = ProviderClient::new(transport, config);         // 🔄 VARIABLE: which service client


    Ok(())
}
