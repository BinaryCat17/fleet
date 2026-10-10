use std::path::Path;

fn main() {
    // Vendored protoc => no system dependency (keeps the alpine/scratch image build working).
    if std::env::var_os("PROTOC").is_none() {
        if let Ok(protoc) = protoc_bin_vendored::protoc_bin_path() {
            std::env::set_var("PROTOC", protoc);
        }
    }

    // Protos live at the workspace root; the Docker build copies them next to the crate.
    let proto_dir = if Path::new("proto/provider.proto").exists() {
        "proto/"
    } else {
        "../proto/"
    };

    connectrpc_build::Config::new()
        .files(&[
            format!("{proto_dir}common.proto"),   // imported by provider.proto => generated too
            format!("{proto_dir}provider.proto"),
        ])
        .includes(&[proto_dir])
        .include_file("_connectrpc.rs")
        .emit_descriptor_set("provider.fds.bin")   // for reflection
        .compile()
        .unwrap();
}