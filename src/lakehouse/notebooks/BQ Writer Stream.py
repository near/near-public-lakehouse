# Databricks notebook source
# MAGIC %md
# MAGIC # BQ Writer Stream

# COMMAND ----------

def create_stream_temp_view(view_name, source_table_name, source_filter):
    return spark.readStream \
    .format("delta") \
    .option("skipChangeCommits", "true") \
    .table(source_table_name) \
    .where(source_filter) \
    .createOrReplaceTempView(view_name)

# COMMAND ----------

def bq_writer_stream(table_name, df_stream):
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
        .option("checkpointLocation", f"/pipelines/checkpoints/bc_{table_name}") \
        .start()

# COMMAND ----------

create_stream_temp_view("tmp_vw_blocks", "hive_metastore.mainnet.silver_blocks", "block_date >= '2023-08-29'")

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

bq_writer_stream("blocks", df_stream_blocks)

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

create_stream_temp_view("tmp_vw_receipt_actions", "hive_metastore.mainnet.silver_action_receipt_actions", "block_date >= '2023-08-29'")

df_stream_receipt_actions = spark.sql("""
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
            args, 
            receipt_predecessor_account_id, 
            action_kind, 
            receipt_receiver_account_id, 
            is_delegate_action 
        from tmp_vw_receipt_actions
    """)

bq_writer_stream("receipt_actions", df_stream_receipt_actions)

# COMMAND ----------

create_stream_temp_view("tmp_vw_account_changes", "hive_metastore.mainnet.silver_account_changes", "block_date >= '2023-08-29'")

df_stream_account_changes = spark.sql("""
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
        from tmp_vw_account_changes
    """)

bq_writer_stream("account_changes", df_stream_account_changes)

# COMMAND ----------

create_stream_temp_view("tmp_vw_execution_outcomes", "hive_metastore.mainnet.silver_execution_outcomes", "block_date >= '2023-08-29'")

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
        from tmp_vw_execution_outcomes
    """)

bq_writer_stream("execution_outcomes", df_stream_execution_outcomes)

# COMMAND ----------

create_stream_temp_view("tmp_vw_receipt_origin", "hive_metastore.mainnet.silver_receipt_originated_from_transaction", "originated_from_transaction_hash IS NOT NULL")

df_stream_receipt_origin = spark.sql("""
        select 
            block_date, 
            block_height, 
            receipt_id, 
            data_id, 
            receipt_kind, 
            originated_from_transaction_hash 
        from tmp_vw_receipt_origin
    """)

bq_writer_stream("receipt_origin", df_stream_receipt_origin)

# COMMAND ----------


