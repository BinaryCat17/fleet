//! `fleet-proto-provider` — Rust wrapper over `proto/provider.proto`.

pub mod proto {
    connectrpc::include_generated!();
}

pub mod server;

pub use fleet_proto_common::common;
pub use proto::provider;

pub const PROVIDER_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/provider.fds.bin"));
