use cached::SizedCache;
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;

use near_lake_framework::near_indexer_primitives::{self, near_primitives};

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

#[derive(Row, Serialize)]
pub struct TransactionRow {
    pub block_height: u64,
    pub block_timestamp: u64,
    pub block_hash: String,
    pub transaction_hash: String,
    pub signer_id: String,
    pub receiver_id: String,
    pub actions: String,
}

#[derive(Row, Serialize)]
pub struct ReceiptRow {
    pub block_height: u64,
    pub block_timestamp: u64,
    pub block_hash: String,
    pub parent_transaction_hash: String,
    pub receipt_id: String,
    pub receiver_id: String,
    pub predecessor_id: String,
    pub actions: String,
}

#[derive(Row, Serialize)]
pub struct ExecutionOutcomeRow {
    pub block_height: u64,
    pub block_timestamp: u64,
    pub block_hash: String,
    pub parent_transaction_hash: String,
    pub executor_id: String,
    pub receipt_ids: Vec<String>,
    pub status: String,
    pub logs: String,
    pub tokens_burnt: String,
    pub gas_burnt: u64,
}

#[derive(Serialize)]
pub struct Action {
    pub action_type: String,
    pub params: serde_json::Value,
}

impl From<&near_primitives::views::ActionView> for Action {
    fn from(action: &near_primitives::views::ActionView) -> Self {
        match action {
            near_primitives::views::ActionView::CreateAccount => Action {
                action_type: "CreateAccount".to_string(),
                params: serde_json::json!({}),
            },
            near_primitives::views::ActionView::DeployContract { .. } => Action {
                action_type: "DeployContract".to_string(),
                params: serde_json::json!({}), // code might be too big and we don't need it at all
            },
            near_primitives::views::ActionView::FunctionCall {
                method_name,
                args,
                gas,
                deposit,
            } => Action {
                action_type: "FunctionCall".to_string(),
                params: serde_json::json!({
                    "method_name": method_name,
                    "args": match serde_json::from_slice::<serde_json::Value>(&args) {
                        Ok(json) => json,
                        Err(_) => serde_json::to_value(args).unwrap(),
                    },
                    "gas": gas.to_string(),
                    "deposit": deposit.to_string(),
                }),
            },
            near_primitives::views::ActionView::Transfer { deposit } => Action {
                action_type: "Transfer".to_string(),
                params: serde_json::json!({
                    "deposit": deposit.to_string(),
                }),
            },
            near_primitives::views::ActionView::Stake { stake, public_key } => Action {
                action_type: "Stake".to_string(),
                params: serde_json::json!({
                    "stake": stake.to_string(),
                    "public_key": public_key.to_string(),
                }),
            },
            near_primitives::views::ActionView::AddKey {
                public_key,
                access_key,
            } => {
                let permission = match &access_key.permission {
                    near_primitives::views::AccessKeyPermissionView::FullAccess => {
                        serde_json::json!("FullAccess")
                    }
                    near_primitives::views::AccessKeyPermissionView::FunctionCall {
                        allowance,
                        receiver_id,
                        method_names,
                    } => serde_json::json!({
                        "allowance": allowance.map(|a| a.to_string()),
                        "receiver_id": receiver_id,
                        "method_names": method_names,
                    }),
                };
                Action {
                    action_type: "AddKey".to_string(),
                    params: serde_json::json!({
                        "public_key": public_key.to_string(),
                        "access_key": {
                            "nonce": access_key.nonce,
                            "permission": permission,
                        },
                    }),
                }
            }
            near_primitives::views::ActionView::DeleteKey { public_key } => Action {
                action_type: "DeleteKey".to_string(),
                params: serde_json::json!({
                    "public_key": public_key.to_string(),
                }),
            },
            near_primitives::views::ActionView::DeleteAccount { beneficiary_id } => Action {
                action_type: "DeleteAccount".to_string(),
                params: serde_json::json!({
                    "beneficiary_id": beneficiary_id.to_string(),
                }),
            },
            near_primitives::views::ActionView::Delegate {
                delegate_action,
                signature,
            } => Action {
                action_type: "Delegate".to_string(),
                params: serde_json::json!({
                    "delegate_action": serde_json::to_value(delegate_action).unwrap_or(serde_json::json!({})),
                    "signature": signature.to_string(),
                }),
            },
            near_primitives::views::ActionView::DeployGlobalContract { .. } => Action {
                action_type: "DeployGlobalContract".to_string(),
                params: serde_json::json!({}),
            },
            near_primitives::views::ActionView::DeployGlobalContractByAccountId { .. } => Action {
                action_type: "DeployGlobalContractByAccountId".to_string(),
                params: serde_json::json!({}),
            },
            near_primitives::views::ActionView::UseGlobalContract { code_hash } => Action {
                action_type: "UseGlobalContract".to_string(),
                params: serde_json::json!({
                    "code_hash": code_hash.to_string(),
                }),
            },
            near_primitives::views::ActionView::UseGlobalContractByAccountId { account_id } => {
                Action {
                    action_type: "UseGlobalContractByAccountId".to_string(),
                    params: serde_json::json!({
                        "account_id": account_id.to_string(),
                    }),
                }
            }
        }
    }
}
