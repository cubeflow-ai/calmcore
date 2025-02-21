fn main() -> std::io::Result<()> {
    let proto_dir = "proto/src";

    //find proto files in proto_dir
    let proto_files = vec!["proto/src/core.proto", "proto/src/calmserver.proto"];

    tonic_build::configure()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .protoc_arg("--experimental_allow_proto3_optional")
        .out_dir("proto/rust_pb/src/")
        .compile_protos(&proto_files, &[proto_dir])
        .unwrap();

    Ok(())
}
