use clickhouse::Row;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use cached::{Cached, SizedCache};
use near_lake_framework::near_indexer_primitives::CryptoHash;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Row, Serialize)]
pub struct EventRow {
    pub block_height: u64,
    pub block_timestamp: u64,
    pub block_hash: String,
    pub contract_id: String,
    pub execution_status: String,
    pub version: String,
    pub standard: String,
    pub index_in_log: u64,
    pub event: String,
    pub data: String,
    pub related_receipt_id: String,
    pub related_receipt_receiver_id: String,
    pub related_receipt_predecessor_id: String,
    pub tx_hash: String,
}

#[derive(Deserialize)]
pub struct EventJson {
    pub version: String,
    pub standard: String,
    pub event: String,
    pub data: Value,
}

pub type ParentTransactionHashString = String;

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub enum ReceiptOrDataId {
    ReceiptId(CryptoHash),
}

pub struct ReceiptsCache {
    cache: SizedCache<ReceiptOrDataId, ParentTransactionHashString>,
}

impl ReceiptsCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: SizedCache::with_size(capacity),
        }
    }

    pub fn cache_get(&mut self, key: &ReceiptOrDataId) -> Option<&ParentTransactionHashString> {
        self.cache.cache_get(key)
    }

    pub fn cache_set(&mut self, key: ReceiptOrDataId, value: ParentTransactionHashString) {
        self.cache.cache_set(key, value);
    }
}

pub type ReceiptsCacheArc = Arc<Mutex<ReceiptsCache>>;
