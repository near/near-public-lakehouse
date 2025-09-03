use cached::Cached;

use near_lake_framework::near_indexer_primitives;

use crate::types;

const TRANSACTIONS_CLICKHOUSE_TABLE: &str = "transactions";

pub async fn handle_transactions(
    message: &near_indexer_primitives::StreamerMessage,
    client: &clickhouse::Client,
    receipts_cache_arc: types::ReceiptsCacheArc,
) -> anyhow::Result<()> {
    let transactions = extract_transactions(message, receipts_cache_arc.clone()).await?;

    // TODO: uncomment this after development and testing
    // if let Err(err) = crate::database::insert_rows(client, TRANSACTIONS_CLICKHOUSE_TABLE, &transactions).await {
    //     eprintln!("Error inserting rows into Clickhouse: {}", err);
    //     anyhow::bail!("Failed to insert rows into Clickhouse: {}", err)
    // }
    Ok(())
}

pub async fn extract_transactions(
    message: &near_indexer_primitives::StreamerMessage,
    receipts_cache_arc: types::ReceiptsCacheArc,
) -> anyhow::Result<Vec<types::TransactionRow>> {
    let mut receipts_cache_lock = receipts_cache_arc.lock().await;
    let block_height = message.block.header.height;
    let block_hash = message.block.header.hash.to_string();
    let block_timestamp = message.block.header.timestamp;

    let transactions = message
        .shards
        .iter()
        .filter_map(|shard| shard.chunk.as_ref())
        .flat_map(|chunk| chunk.transactions.iter().map(Clone::clone))
        .filter_map(|tx| {
            if tx.transaction.signer_id.as_str() == "intents.near"
                || tx.transaction.receiver_id.as_str() == "intents.near"
                || tx.transaction.signer_id.as_str() == "defuse-alpha.near"
                || tx.transaction.receiver_id.as_str() == "defuse-alpha.near"
            {
                let transaction_hash = tx.transaction.hash.to_string();
                let converted_into_receipt_id = tx
                    .outcome
                    .execution_outcome
                    .outcome
                    .receipt_ids
                    .first()
                    .expect("`receipt_ids` must contain one Receipt Id");

                // Save this Transaction hash to ReceiptsCache
                // we use the Receipt ID to which this transaction was converted
                // and the Transaction hash as a value.
                // Later, while Receipt will be looking for a parent Transaction hash
                // it will be able to find it in the ReceiptsCache
                receipts_cache_lock.cache_set(
                    types::ReceiptOrDataId::ReceiptId(*converted_into_receipt_id),
                    transaction_hash.clone(),
                );

                Some(types::TransactionRow {
                    block_height: block_height,
                    block_timestamp: block_timestamp,
                    block_hash: block_hash.clone(),
                    transaction_hash: transaction_hash,
                    signer_id: tx.transaction.signer_id.to_string(),
                    receiver_id: tx.transaction.receiver_id.to_string(),
                    actions: serde_json::json!(tx
                        .transaction
                        .actions
                        .iter()
                        .map(|action| { types::Action::from(action) })
                        .collect::<Vec<types::Action>>()),
                })
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    drop(receipts_cache_lock);

    println!(
        "intents.near transactions in block: {}, {:#?}",
        transactions.len(),
        transactions
            .iter()
            .map(|tx| tx.transaction_hash.to_string())
            .collect::<Vec<_>>()
    );
    Ok(transactions)
}
