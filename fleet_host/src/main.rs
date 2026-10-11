use connectrpc::client::{ClientConfig, Http2Connection};
use fleet_host::proto::provider::{Instance, ListInstancesRequest, ProviderClient};
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

    // ── step 4: smoke call — proves client ↔ server end-to-end (server-stream) ──
    match client.list_instances(ListInstancesRequest::default()).await {
        Ok(mut stream) => {
            let mut count = 0u32;
            loop {
                match stream.message::<Instance>().await {
                    Ok(Some(msg)) => {
                        let inst = msg.to_owned_message();
                        println!("  instance {} ({})", inst.id, inst.provider_status);
                        count += 1;
                    }
                    Ok(None) => break, // clean end of stream — the RPC succeeded
                    // connectrpc error raised mid-stream (e.g. upstream failure)
                    Err(e) => {
                        println!("list_instances stream failed after {count} row(s): {e}");
                        return Ok(());
                    }
                }
            }
            println!("provider online: {count} instance(s)");
        }
        // call-level failure (admission / transport) — connectrpc error
        Err(e) => println!("list_instances failed: {e}"),
    }

    Ok(())
}
