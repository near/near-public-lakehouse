use clickhouse::Row;
use serde::{Deserialize, Serialize};
use serde_json::Value;

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