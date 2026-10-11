use std::path::PathBuf;

fn main() {
    // Vendored protoc => no system dependency (keeps the alpine/scratch image build working).
    if std::env::var_os("PROTOC").is_none() {
        if let Ok(protoc) = protoc_bin_vendored::protoc_bin_path() {
            std::env::set_var("PROTOC", protoc);
        }
    }

    // proto/ lives at the repo root; when vendored into an image it is copied
    // next to the crate. Walk up until we find provider.proto.
    let proto_dir = find_proto_dir();

    // The `common` package lives in the `fleet-proto-common` crate. Map it onto
    // that crate's module path so `provider`'s generated code references the
    // shared compiled types instead of regenerating them.
    //
    // `Config::new()` derives from `Options::default()` (whose buffa config is
    // `CodeGenConfig::default()` + `generate_json = true`). `buffa_config()`
    // replaces the embedded buffa config wholesale, so we reproduce those
    // defaults exactly, then add the `extern_path`. Non-buffa options (client
    // feature gating, encodable impls) are untouched.
    let mut buffa = connectrpc_build::CodeGenConfig::default();
    buffa.generate_json = true; // matches Options::default()
    buffa.extern_paths.push((
        ".common".to_string(),
        "::fleet_proto_common::common".to_string(),
    ));

    connectrpc_build::Config::new()
        .files(&[proto_dir.join("provider.proto")])
        .includes(&[proto_dir.as_path()])
        .buffa_config(buffa)
        .include_file("_connectrpc.rs")
        .emit_descriptor_set("provider.fds.bin") // for reflection
        .compile()
        .unwrap();
}

/// Locate the directory holding provider.proto, searching upward from the crate.
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
