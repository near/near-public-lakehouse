# Databricks notebook source
# MAGIC %md
# MAGIC # BQ Writer Stream

# COMMAND ----------

spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")

def create_stream_temp_view(view_name, source_table_name, source_filter):
    return spark.readStream \
    .format("delta") \
    .option("skipChangeCommits", "true") \
    .table(source_table_name) \
    .where(source_filter) \
    .createOrReplaceTempView(view_name)

# COMMAND ----------

def bq_writer_stream(table_name, df_stream, checkpoint_suffix=""):
    return df_stream.writeStream \
        .format("bigquery") \
        .option("temporaryGcsBucket", "databricks-bq-buffer-near-lakehouse") \
        .option("table", f"pagoda-data-platform.crypto_near_mainnet.{table_name}") \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("partitionField", "block_date") \
        .option("partitionType", "DAY") \
        .option("allowFieldAddition", "true") \
        .option("allowFieldRelaxation", "true") \
        .option("writeMethod", "indirect") \
        .option("checkpointLocation", f"/pipelines/checkpoints/bc_{table_name}{checkpoint_suffix}") \
        .trigger(availableNow=True) \
        .start()

# COMMAND ----------

create_stream_temp_view("tmp_vw_blocks", "hive_metastore.mainnet.silver_blocks", "block_date >= '2024-04-23'")

df_stream_blocks = spark.sql("""
        select 
            block_date, 
            block_height, 
            block_timestamp, 
            block_timestamp_utc, 
            block_hash, 
            prev_block_hash, 
            CAST(total_supply AS DOUBLE) as total_supply, 
            CAST(gas_price AS DOUBLE) as gas_price, 
            author_account_id 
        from tmp_vw_blocks
    """)

bq_writer_stream("blocks", df_stream_blocks, "_v3")

# COMMAND ----------

create_stream_temp_view("tmp_vw_chunks", "hive_metastore.mainnet.silver_chunks", "block_date >= '2023-08-29'")

df_stream_chunks = spark.sql("""
        select 
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
        from tmp_vw_chunks
    """)

bq_writer_stream("chunks", df_stream_chunks)

# COMMAND ----------

create_stream_temp_view("tmp_vw_transactions", "hive_metastore.mainnet.silver_transactions", "block_date >= '2023-08-29'")

df_stream_transactions = spark.sql("""
        select 
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
        from tmp_vw_transactions
    """)

bq_writer_stream("transactions", df_stream_transactions)

# COMMAND ----------

# MAGIC %md
# MAGIC  for BQ Writer receipt_origin_transaction see https://4221960800361869.9.gcp.databricks.com/?o=4221960800361869#notebook/2866455144712106/command/2866455144712107

# COMMAND ----------

create_stream_temp_view("tmp_vw_receipt_details", "hive_metastore.mainnet.silver_receipts", "block_date >= '2023-08-29'")

df_stream_receipt_details = spark.sql("""
        select 
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
        from tmp_vw_receipt_details
    """)

bq_writer_stream("receipt_details", df_stream_receipt_details)

# COMMAND ----------

create_stream_temp_view("tmp_vw_receipt_actions_v2", "hive_metastore.mainnet.silver_action_receipt_actions", "block_date >= '2024-06-01'")

df_stream_receipt_actions_v2 = spark.sql("""
        select 
            block_date, 
            block_height, 
            block_timestamp, 
            block_timestamp_utc, 
            block_hash, 
            chunk_hash, 
            shard_id, 
            index_in_action_receipt, 
            receipt_id, 
            gas_price,
            signer_account_id,
            signer_public_key,
            args, 
            receipt_predecessor_account_id, 
            action_kind, 
            receipt_receiver_account_id, 
            is_delegate_action 
        from tmp_vw_receipt_actions_v2
    """)

bq_writer_stream("receipt_actions", df_stream_receipt_actions_v2, 'v2')

# COMMAND ----------

create_stream_temp_view("tmp_vw_account_changes_v2", "hive_metastore.mainnet.silver_account_changes", "block_date >= '2024-06-06'")

df_stream_account_changes_v2 = spark.sql("""
        select 
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
        from tmp_vw_account_changes_v2
    """)

bq_writer_stream("account_changes", df_stream_account_changes_v2, 'v2')

# COMMAND ----------

create_stream_temp_view("tmp_vw_execution_outcomes_v2", "hive_metastore.mainnet.silver_execution_outcomes", "block_date >= '2024-06-01'")

df_stream_execution_outcomes = spark.sql("""
        select 
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
            status, 
            logs
        from tmp_vw_execution_outcomes_v2
    """)

bq_writer_stream("execution_outcomes", df_stream_execution_outcomes, "v2")

# COMMAND ----------

create_stream_temp_view("tmp_vw_ft_events_v4", "hive_metastore.mainnet.silver_execution_outcome_ft_event_logs", "block_date >= '2024-06-01'")

df_stream_ft_event_logs = spark.sql("""
        SELECT 
            block_date,
            block_height,
            block_timestamp,
            block_timestamp_utc,
            block_hash,
            chunk_hash,
            shard_id,
            standard, 
            receipt_id, 
            contract_account_id, 
            affected_account_id, 
            involved_account_id, 
            CAST(delta_amount AS STRING) as delta_amount, 
            cause, 
            status, 
            event_memo,
            event_index,
            token_id
        FROM tmp_vw_ft_events_v4
    """)


df_stream_ft_event_logs.writeStream \
    .format("bigquery") \
    .option("temporaryGcsBucket", "databricks-bq-buffer-near-lakehouse") \
    .option("table", f"pagoda-data-platform.crypto_near_mainnet.ft_events") \
    .option("createDisposition", "CREATE_IF_NEEDED") \
    .option("partitionField", "block_date") \
    .option("clusteredFields", "contract_account_id") \
    .option("partitionType", "DAY") \
    .option("allowFieldAddition", "true") \
    .option("allowFieldRelaxation", "true") \
    .option("writeMethod", "indirect") \
    .option("checkpointLocation", "/pipelines/checkpoints/bc_ft_events_v4") \
    .trigger(availableNow=True) \
    .start()

# COMMAND ----------

create_stream_temp_view("tmp_vw_nft_events_v4", "hive_metastore.mainnet.silver_execution_outcome_nft_event_logs", "block_date >= '2024-06-01'")

df_stream_nft_event_logs = spark.sql("""
        SELECT 
            block_date,
            block_height,
            block_timestamp,
            block_timestamp_utc,
            block_hash,
            chunk_hash,
            shard_id,
            standard, 
            receipt_id, 
            contract_account_id, 
            token_id,
            old_owner_account_id, 
            new_owner_account_id, 
            authorized_account_id,
            cause, 
            status, 
            event_memo,
            event_index
        FROM tmp_vw_nft_events_v4
    """)


df_stream_nft_event_logs.writeStream \
    .format("bigquery") \
    .option("temporaryGcsBucket", "databricks-bq-buffer-near-lakehouse") \
    .option("table", f"pagoda-data-platform.crypto_near_mainnet.nft_events") \
    .option("createDisposition", "CREATE_IF_NEEDED") \
    .option("partitionField", "block_date") \
    .option("clusteredFields", "contract_account_id") \
    .option("partitionType", "DAY") \
    .option("allowFieldAddition", "true") \
    .option("allowFieldRelaxation", "true") \
    .option("writeMethod", "indirect") \
    .option("checkpointLocation", "/pipelines/checkpoints/bc_nft_events_v4") \
    .trigger(availableNow=True) \
    .start()

# COMMAND ----------

create_stream_temp_view("tmp_vw_ft_balances_daily_v3", "hive_metastore.mainnet.silver_accounts_daily_ft_balances", "epoch_date > '2024-02-20'")

df_stream_nft_event_logs = spark.sql("""
        SELECT 
            CAST(epoch_date AS DATE) AS epoch_date,
            epoch_block_height,
            account_id,
            liquid,
            storage_usage,
            unstaked_not_liquid,
            staked,
            reward,
            lockup_account_id,
            lockup_liquid,
            lockup_unstaked_not_liquid,
            lockup_staked,
            lockup_reward
        FROM tmp_vw_ft_balances_daily_v3
    """)

df_stream_nft_event_logs.writeStream \
    .format("bigquery") \
    .option("temporaryGcsBucket", "databricks-bq-buffer-near-lakehouse") \
    .option("table", f"pagoda-data-platform.crypto_near_mainnet.ft_balances_daily") \
    .option("createDisposition", "CREATE_IF_NEEDED") \
    .option("partitionField", "epoch_date") \
    .option("clusteredFields", "account_id") \
    .option("partitionType", "DAY") \
    .option("allowFieldAddition", "true") \
    .option("allowFieldRelaxation", "true") \
    .option("writeMethod", "indirect") \
    .option("checkpointLocation", "/pipelines/checkpoints/bc_ft_balances_daily_v3") \
    .trigger(availableNow=True) \
    .start()

# COMMAND ----------

create_stream_temp_view("tmp_vw_circulating_supply", "hive_metastore.mainnet.gold_aggregated_circulating_supply", "block_date > '2024-05-01'")

df_stream_nft_event_logs = spark.sql("""
        SELECT 
            block_date,
            computed_at_block_timestamp,
            computed_at_block_hash,
            computed_at_block_height,
            circulating_tokens_supply,
            total_tokens_supply
        FROM tmp_vw_circulating_supply
    """)

df_stream_nft_event_logs.writeStream \
    .format("bigquery") \
    .option("temporaryGcsBucket", "databricks-bq-buffer-near-lakehouse") \
    .option("table", f"pagoda-data-platform.crypto_near_mainnet.circulating_supply") \
    .option("createDisposition", "CREATE_IF_NEEDED") \
    .option("allowFieldAddition", "true") \
    .option("allowFieldRelaxation", "true") \
    .option("writeMethod", "indirect") \
    .option("checkpointLocation", "/pipelines/checkpoints/bc_circulating_supply") \
    .trigger(availableNow=True) \
    .start()

# COMMAND ----------

create_stream_temp_view("tmp_vw_transaction_actions", "hive_metastore.mainnet.silver_transaction_actions", "block_date >= '2024-06-06'")

df_stream_transaction_actions = spark.sql("""
        select 
            block_date,
            block_height,
            block_timestamp,
            block_timestamp_utc,
            transaction_hash,
            transaction_status,
            converted_into_receipt_id,
            signer_id AS signer_account_id, 
            public_key AS signer_public_key, 
            receiver_id AS receiver_account_id, 
            index_in_transaction,
            action_kind,
            args
        from tmp_vw_transaction_actions
    """)

bq_writer_stream("transaction_actions", df_stream_transaction_actions)

# COMMAND ----------


