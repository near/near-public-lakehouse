use cached::SizedCache;
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;

use near_lake_framework::near_indexer_primitives;

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub enum ReceiptOrDataId {
    ReceiptId(near_indexer_primitives::CryptoHash),
    DataId(near_indexer_primitives::CryptoHash),
}
// Creating type aliases to make HashMap types for cache more explicit
pub type ParentTransactionHashString = String;
// Simple cache for Receipts to find their parent Transactions
// The key is ReceiptID
// The value is TransactionHash (the very parent of the Receipt)
pub type ReceiptsCacheArc =
    std::sync::Arc<Mutex<SizedCache<ReceiptOrDataId, ParentTransactionHashString>>>;

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
}

#[derive(Deserialize)]
pub struct EventJson {
    pub version: String,
    pub standard: String,
    pub event: String,
    pub data: Value,
}
