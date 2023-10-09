pub mod service_pb {
    include!(concat!(env!("OUT_DIR"), "/bedrock.dataserver.rs"));
}

pub mod metaserver_pb {
    include!(concat!(env!("OUT_DIR"), "/bedrock.metaserver.rs"));
}
