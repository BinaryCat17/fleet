use std::sync::Arc;
use connectrpc::Router;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // VARIABLE: this server's impl + constructor
    let router = Router::new().add_service(Arc::new(VastProvider::new()));

    // VARIABLE: generated name constant (changes with the service name in the proto)
    let (router, _health) = connectrpc_health::install_static(router, [PROVIDER_SERVICE_NAME]);

    let bytes: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/provider.fds.bin"));
    let router = connectrpc_reflection::install(...);

    let app = axum::Router::new()
        .route("/health", axum::routing::get(|| async { "OK" }))
        .fallback_service(router.into_axum_service());
    
    // VARIABLE: port
    let listener = tokio::net::TcpListener::bind("0.0.0.0:50051").await?;

    axum::serve(listener, app).await?; 
    Ok(())
}
