fn main() {
    tonic_build::compile_protos("../proto/dataserver.proto").unwrap();
    tonic_build::compile_protos("../proto/metaserver.proto").unwrap();
    tonic_build::compile_protos("../proto/proxy.proto").unwrap();
}
