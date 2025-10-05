use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use once_cell::sync::Lazy;
use tokio::sync::RwLock;

use crate::shard_route;

static ROUTE_MANAGER: Lazy<
    parking_lot::RwLock<HashMap<u32, Arc<RwLock<shard_route::ShardRouter>>>>,
> = Lazy::new(|| parking_lot::RwLock::new(HashMap::new()));

pub fn get_shard_route(storage_id: u32) -> Result<Arc<RwLock<shard_route::ShardRouter>>> {
    if let Some(route) = ROUTE_MANAGER.read().get(&storage_id) {
        return Ok(route.clone());
    }
    let mut lg = ROUTE_MANAGER.write();
    if let Some(route) = lg.get(&storage_id) {
        return Ok(route.clone());
    }

    let router = Arc::new(RwLock::new({
        let mut r = shard_route::ShardRouter::new(storage_id);
        r.start()?;
        r
    }));

    lg.insert(storage_id, router.clone());

    Ok(router)
}
