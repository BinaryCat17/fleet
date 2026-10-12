use std::path::PathBuf;

fn main() {
    // Vendored protoc => no system dependency (keeps the alpine/scratch image build working).
    if std::env::var_os("PROTOC").is_none() {
        if let Ok(protoc) = protoc_bin_vendored::protoc_bin_path() {
            std::env::set_var("PROTOC", protoc);
        }
    }

    // proto/ lives at the repo root; when vendored into an image it is copied
    // next to the crate. Walk up until we find the contracts.
    let proto_dir = find_proto_dir();

    // ONE codegen pass over the whole contract: `provider` imports `common`, and
    // compiled together the generated code references it in-crate
    // (`super::common::…`) — no `extern_path` mapping needed.
    connectrpc_build::Config::new()
        .files(&[
            proto_dir.join("common.proto"),
            proto_dir.join("provider.proto"),
        ])
        .includes(&[proto_dir.as_path()])
        .include_file("_connectrpc.rs")
        .emit_descriptor_set("fleet.fds.bin") // for reflection
        .compile()
        .unwrap();
}

/// Locate the directory holding the contracts, searching upward from the crate.
fn find_proto_dir() -> PathBuf {
    let mut dir = std::env::current_dir().unwrap();
    for _ in 0..5 {
        let candidate = dir.join("proto");
        if candidate.join("provider.proto").exists() {
            return candidate;
        }
        if dir.join("provider.proto").exists() {
            return dir.clone();
        }
        if !dir.pop() {
            break;
        }
    }
    panic!("could not locate provider.proto relative to the crate root");
}
