# Databricks notebook source
# MAGIC %md
# MAGIC # BQ Writer Backfill from Genesis 2020-07-21 to 2023-08-28

# COMMAND ----------

def bq_writer_backfill(table_name, sql):
    df = spark.sql(sql)

    df.write \
        .format("bigquery") \
        .option("temporaryGcsBucket", 'databricks-bq-buffer-near-lakehouse') \
        .option("table", f"pagoda-data-platform.crypto_near_mainnet.{table_name}") \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("partitionField", "block_date") \
        .option("partitionType", "DAY") \
        .option("allowFieldAddition", "true") \
        .option("allowFieldRelaxation", "true") \
        .option("writeMethod", "indirect") \
        .mode("append").save()  

# COMMAND ----------

sql = """
        SELECT 
            block_date, 
            block_height, 
            block_timestamp, 
            block_timestamp_utc, 
            block_hash, 
            prev_block_hash, 
            CAST(total_supply AS DOUBLE) AS total_supply, 
            CAST(gas_price AS DOUBLE) AS gas_price, 
            author_account_id 
        FROM hive_metastore.mainnet.silver_blocks 
        WHERE block_date >= '2020-01-01' AND  block_date <= '2023-08-28'
    """
bq_writer_backfill("blocks", sql)

# COMMAND ----------

sql = """
        SELECT 
            block_date, 
            block_height, 
            block_timestamp, 
            block_timestamp_utc, 
            block_hash, 
            chunk_hash, 
            shard_id, 
            hash AS transaction_hash, 
            index_in_chunk, 
            signer_id AS signer_account_id, 
            public_key AS signer_public_key, 
            nonce, 
            receiver_id AS receiver_account_id, 
            signature, 
            status, 
            converted_into_receipt_id, 
            receipt_conversion_gas_burnt, 
            CAST(receipt_conversion_tokens_burnt AS DOUBLE) AS receipt_conversion_tokens_burnt 
        FROM hive_metastore.mainnet.silver_transactions 
        WHERE block_date >= '2020-01-01' AND  block_date <= '2023-08-28'
    """
bq_writer_backfill("transactions", sql)


# COMMAND ----------

sql = """
        SELECT 
            block_date, 
            block_height, 
            block_timestamp, 
            block_timestamp_utc, 
            block_hash, 
            chunk_hash, 
            shard_id, 
            index_in_chunk, 
            receipt_kind, 
            receipt_id, 
            data_id, 
            predecessor_account_id, 
            receiver_account_id, 
            receipt 
        FROM hive_metastore.mainnet.silver_receipts 
        WHERE block_date >= '2020-01-01' AND  block_date <= '2023-08-28'
    """

bq_writer_backfill("receipt_details", sql)

# COMMAND ----------

sql = """
        SELECT 
            block_date, 
            block_height, 
            block_timestamp, 
            block_timestamp_utc, 
            block_hash, 
            chunk_hash, 
            shard_id, 
            index_in_action_receipt, 
            receipt_id, 
            args, 
            receipt_predecessor_account_id, 
            action_kind, 
            receipt_receiver_account_id, 
            is_delegate_action 
        FROM hive_metastore.mainnet.silver_action_receipt_actions 
        WHERE block_date >= '2020-01-01' AND  block_date <= '2023-08-28'
    """

bq_writer_backfill("receipt_actions", sql)

# COMMAND ----------

sql = """
        SELECT 
            block_date, 
            block_height, 
            block_timestamp, 
            block_timestamp_utc, 
            block_hash, 
            chunk_hash, 
            index_in_block, 
            affected_account_id, 
            caused_by_transaction_hash, 
            caused_by_receipt_id, 
            update_reason, 
            CAST(affected_account_nonstaked_balance AS DOUBLE) AS affected_account_nonstaked_balance, 
            CAST(affected_account_staked_balance AS DOUBLE) AS affected_account_staked_balance, 
            CAST(affected_account_storage_usage AS DOUBLE) AS affected_account_storage_usage 
        FROM hive_metastore.mainnet.silver_account_changes 
        WHERE block_date >= '2020-01-01' AND  block_date <= '2023-08-28'
    """

bq_writer_backfill("account_changes", sql)

# COMMAND ----------

sql = """
        SELECT 
            block_date, 
            block_height, 
            block_timestamp, 
            block_timestamp_utc, 
            block_hash, 
            chunk_hash, 
            shard_id, 
            receipt_id, 
            executed_in_block_hash, 
            outcome_receipt_ids, 
            index_in_chunk, 
            CAST(gas_burnt AS DOUBLE) AS gas_burnt, 
            CAST(tokens_burnt AS DOUBLE) AS tokens_burnt, 
            executor_account_id, 
            status ,
            logs 
        FROM hive_metastore.mainnet.silver_execution_outcomes 
        WHERE block_date >= '2020-01-01' AND  block_date <= '2023-08-28'
    """

bq_writer_backfill("execution_outcomes", sql)

# COMMAND ----------

sql = """
        SELECT 
            block_date, 
            block_height, 
            block_timestamp, 
            block_timestamp_utc, 
            block_hash, 
            chunk_hash, 
            shard_id, 
            signature, 
            CAST(gas_limit AS DOUBLE) AS gas_limit, 
            CAST(gas_used AS DOUBLE) AS gas_used, 
            author_account_id 
        FROM hive_metastore.mainnet.silver_chunks 
    """

bq_writer_backfill("chunks", f"{sql} WHERE block_date >= '2020-01-01' AND  block_date <= '2020-12-31'")
bq_writer_backfill("chunks", f"{sql} WHERE block_date >= '2021-01-01' AND  block_date <= '2021-12-31'")
bq_writer_backfill("chunks", f"{sql} WHERE block_date >= '2022-01-01' AND  block_date <= '2022-06-30'")
bq_writer_backfill("chunks", f"{sql} WHERE block_date >= '2022-07-01' AND  block_date <= '2022-12-31'")
bq_writer_backfill("chunks", f"{sql} WHERE block_date >= '2023-01-01' AND  block_date <= '2023-08-28'")

# COMMAND ----------

sql = """
        SELECT 
            block_height,
            block_timestamp_utc,
            block_date,
            signer_id,
            true_signer_id,
            predecessor_id,
            receipt_id,
            contract_id,
            method_name,
            deposit,
            gas,
            account_object,
            widget, 
            post, 
            profile, 
            graph, 
            settings,
            badge, 
            index
        FROM hive_metastore.mainnet.silver_near_social_txs_parsed 
        WHERE block_date >= '2020-01-01' AND  block_date <= '2023-10-09'
    """

bq_writer_backfill("near_social_transactions", sql)

# COMMAND ----------


