pub mod dataserver {
    include!(concat!(env!("OUT_DIR"), "/bedrock.dataserver.rs"));
}

pub mod metaserver {
    include!(concat!(env!("OUT_DIR"), "/bedrock.metaserver.rs"));
}

pub mod proxy {
    include!(concat!(env!("OUT_DIR"), "/bedrock.proxy.rs"));
}
