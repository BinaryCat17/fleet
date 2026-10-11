use std::path::PathBuf;

fn main() {
    // Vendored protoc => no system dependency (keeps the alpine/scratch image build working).
    if std::env::var_os("PROTOC").is_none() {
        if let Ok(protoc) = protoc_bin_vendored::protoc_bin_path() {
            std::env::set_var("PROTOC", protoc);
        }
    }

    // proto/ lives at the repo root; when vendored into an image it is copied
    // next to the crate. Walk up until we find common.proto.
    let proto_dir = find_proto_dir();

    connectrpc_build::Config::new()
        .files(&[proto_dir.join("common.proto")])
        .includes(&[proto_dir.as_path()])
        .include_file("_connectrpc.rs")
        .emit_descriptor_set("common.fds.bin") // for reflection
        .compile()
        .unwrap();
}

/// Locate the directory holding common.proto, searching upward from the crate.
fn find_proto_dir() -> PathBuf {
    let mut dir = std::env::current_dir().unwrap();
    for _ in 0..5 {
        let candidate = dir.join("proto");
        if candidate.join("common.proto").exists() {
            return candidate;
        }
        // Also allow the protos copied directly next to / beside the crate.
        if dir.join("common.proto").exists() {
            return dir.clone();
        }
        if !dir.pop() {
            break;
        }
    }
    panic!("could not locate common.proto relative to the crate root");
}
