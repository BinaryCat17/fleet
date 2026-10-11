use std::sync::Arc;

use connectrpc::Router;
use connectrpc_reflection::Reflector;
use fleet_provider_vast::VastProvider;
use fleet_proto_provider::provider::PROVIDER_SERVICE_NAME;
use fleet_proto_provider::PROVIDER_DESCRIPTOR_SET;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // VARIABLE: this server's impl + constructor
    let router = Router::new().add_service(Arc::new(VastProvider::new()?));

    // VARIABLE: generated name constant (changes with the service name in the proto)
    let (router, _health) = connectrpc_health::install_static(router, [PROVIDER_SERVICE_NAME]);

    // gRPC server reflection over the embedded descriptor set (grpcurl / buf curl)
    let reflector = Reflector::from_descriptor_set_bytes(PROVIDER_DESCRIPTOR_SET)?;
    let router = connectrpc_reflection::install(router, reflector);

    let app = axum::Router::new()
        .route("/health", axum::routing::get(|| async { "OK" }))
        .fallback_service(router.into_axum_service());
    
    let listener = tokio::net::TcpListener::bind("0.0.0.0:50051").await?;

    axum::serve(listener, app).await?; 
    Ok(())
}
