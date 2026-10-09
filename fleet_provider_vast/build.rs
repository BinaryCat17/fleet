fn main() {
    connectrpc_build::Config::new()
        .files(&["proto/provider.proto"])
        .includes(&["proto/"])
        .include_file("_connectrpc.rs")
        .emit_descriptor_set("provider.fds.bin")   // for reflection
        .compile()
        .unwrap();
}