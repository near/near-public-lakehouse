# Databricks notebook source
# MAGIC %md
# MAGIC https://cloud.google.com/storage/docs/requester-pays

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Initial setup

# COMMAND ----------

from datetime import datetime, timedelta

private_key_id = dbutils.secrets.get(scope = "gcp_data_platform_bq", key = "private_key_id")
private_key = dbutils.secrets.get(scope = "gcp_data_platform_bq", key = "private_key")
project_id = dbutils.secrets.get(scope = "gcp_data_platform_bq", key = "project_id")
client_email = dbutils.secrets.get(scope = "gcp_data_platform_bq", key = "client_email")
credentials = dbutils.secrets.get(scope = "gcp_data_platform_bq", key = "credentials")

spark.conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")

spark.conf.set("spark.hadoop.fs.gs.requester.pays.mode", "CUSTOM")
spark.conf.set("spark.hadoop.fs.gs.requester.pays.bucket", "near-lakehouse-public")
spark.conf.set("spark.hadoop.fs.gs.requester.pays.project.id", project_id)

spark.conf.set("spark.hadoop.fs.gs.auth.service.account.private.key.id", private_key_id)
spark.conf.set("spark.hadoop.fs.gs.auth.service.account.private.key", private_key)
spark.conf.set("spark.hadoop.fs.gs.project.id", project_id)
spark.conf.set("spark.hadoop.fs.gs.auth.service.account.email", client_email)

spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")

spark.conf.set("credentials", credentials)

# COMMAND ----------

# define the processed_time hour for the exports
def truncate_to_hour(dt):
    return dt.replace(minute=0, second=0, microsecond=0)

processed_time = truncate_to_hour(datetime.now()).strftime("%Y-%m-%d %H:%M:%S")

print("_processed_time:", processed_time)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Update the `public_lakehouse` dataset Delta tables 

# COMMAND ----------

# MAGIC %md
# MAGIC ## block_chunks

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW temp_view_block_chunks AS 
    SELECT 
        b.block_date AS date 
        , b.block_height height 
        , b.block_timestamp_utc AS time
        , b.block_hash AS hash
        , b.prev_block_hash AS prev_hash
        , b.total_supply
        , b.gas_price
        , b.author_account_id
        , b.header.epoch_id
        , c.shard_id
        , c.chunk_hash
        , c.signature AS chunk_signature
        , c.gas_limit AS chunk_gas_limit 
        , c.gas_used AS chunk_gas_used
        , c.author_account_id AS chunk_author_account_id
        , CAST('{processed_time}' AS TIMESTAMP) AS _processed_time
    FROM mainnet.silver_chunks c
    -- limit to last 1 day partitions for potential later arrival records
    JOIN mainnet.silver_blocks b ON c.chunk.header.prev_block_hash = b.prev_block_hash 
        AND c.block_date = b.block_date 
    WHERE
        c.block_date >= date_trunc('day', now() - interval 1 day) AND b.block_date >= date_trunc('day', now() - interval 1 day);
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS  public_lakehouse.block_chunks 
    PARTITIONED BY (date)
    COMMENT 'NEAR can have multiple shards(chunks) per block, this table contains blocks and chunks denormalized' AS
    SELECT *
    FROM temp_view_block_chunks
    WHERE 1=2;
""")
spark.sql(f"""
    MERGE INTO public_lakehouse.block_chunks as t
    USING (SELECT * FROM temp_view_block_chunks) s
    ON s.hash = t.hash AND s.chunk_hash = t.chunk_hash
    WHEN NOT MATCHED THEN INSERT *;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## actions

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW temp_view_actions AS 
    ( 
    WITH actions AS (
        SELECT 
        ra.block_date
        , ra.block_height
        , ra.block_timestamp_utc AS block_time
        , ra.block_hash
        , ra.chunk_hash
        , ra.shard_id
        , ra.index_in_action_receipt
        , ra.receipt_id
        , ra.gas_price
        , ra.receipt_predecessor_account_id
        , ra.receipt_receiver_account_id
        , t.receipt_conversion_gas_burnt
        , t.receipt_conversion_tokens_burnt
        , o.originated_from_transaction_hash tx_hash
        , t.signer_id tx_from
        , t.receiver_id tx_to
        , t.public_key tx_signer_public_key
        , t.nonce tx_nonce
        , t.signature tx_signature
        , t.status tx_status
        , (ra.action_kind = 'DELEGATE_ACTION') AS is_delegate_action
        , eo.gas_burnt execution_gas_burnt
        , eo.tokens_burnt execution_tokens_burnt
        , eo.status execution_status
        , eo.outcome_receipt_ids AS execution_outcome_receipt_ids
        , ra.action_kind
        , args
        , IF(ra.action_kind = 'DEPLOY_CONTRACT', from_json(args, 'STRUCT<code_sha256: STRING>'), NULL) AS deploy_contract
        , IF(ra.action_kind = 'TRANSFER', from_json(args, 'STRUCT<deposit: STRING>'), NULL) AS transfer
        , IF(ra.action_kind = 'STAKE', from_json(args, 'STRUCT<public_key: STRING, stake: STRING>'), NULL) AS stake
        , IF(ra.action_kind = 'ADD_KEY', from_json(args, 'STRUCT<access_key: STRUCT<nonce: STRING, permission:STRING>, public_key: STRING>'), NULL) AS add_key
        , IF(ra.action_kind = 'DELETE_KEY', from_json(args, 'STRUCT<public_key: STRING>'), NULL) AS delete_key
        , IF(ra.action_kind = 'DELETE_ACCOUNT', from_json(args, 'STRUCT<beneficiary_id: STRING>'), NULL) AS delete_account
        , IF(ra.action_kind = 'DELEGATE_ACTION', from_json(args, 'STRUCT<Delegate: STRUCT<delegate_action: STRUCT<actions: ARRAY<STRING>, max_block_height: STRING, nonce: STRING, public_key: STRING, receiver_id: STRING, sender_id: STRING>, signature: STRING>>').Delegate, NULL) AS delegate
        , IF(ra.action_kind = 'FUNCTION_CALL', from_json(args, 'STRUCT<gas: BIGINT, deposit: STRING, args_base64: STRING, method_name: STRING>'), NULL) AS call
        , IF(ra.action_kind = 'FUNCTION_CALL', CAST(unbase64(call.args_base64) AS string), NULL) AS args_parsed
        , struct(call, args_parsed) AS function_call
        , CAST('{processed_time}' AS TIMESTAMP) AS _processed_time
        FROM mainnet.silver_action_receipt_actions ra
        -- limit to last 3 days partitions for potential later arrival records
        JOIN mainnet.silver_receipts r ON r.receipt_id = ra.receipt_id AND r.block_date = ra.block_date AND r.block_date >= date_trunc('day', now() - interval 3 day)
        JOIN mainnet.silver_receipt_originated_from_transaction o ON ra.receipt_id = o.receipt_id AND ra.block_date = o.block_date AND o.originated_from_transaction_hash <> '' AND o.block_date >= date_trunc('day', now() - interval 3 day)
        JOIN mainnet.silver_execution_outcomes eo ON eo.receipt_id = ra.receipt_id AND eo.block_date >= date_trunc('day', now() - interval 3 day)
        JOIN mainnet.silver_transactions t ON t.hash = o.originated_from_transaction_hash AND t.block_date >= date_trunc('day', now() - interval 3 day)
        WHERE ra.block_date >= date_trunc('day', now() - interval 3 day)
    )

    SELECT 
        * EXCEPT (args, deploy_contract, transfer, stake, add_key, delete_key, delete_account, delegate, call, args_parsed, function_call )
        , struct(deploy_contract, transfer, stake, add_key, delete_key, delete_account, delegate, function_call) AS action
    FROM actions
    );
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS  public_lakehouse.actions 
    PARTITIONED BY (block_date)
    COMMENT 'A receipt action that includes Execution Outcomes + Originated from Transaction Hash and Original Signer + Transaction Details
    decoded by action_kind ( Function Calls, Add Keys, Delete Keys, Create Account, Deploy Contract, etc ) depending on actions kind we will fill the corresponding struct column' AS
    SELECT *
    FROM temp_view_actions
    WHERE 1=2;
""")
spark.sql(f"""
    MERGE INTO public_lakehouse.actions as t
    USING (SELECT * FROM temp_view_actions) s
    ON s.block_date = t.block_date AND s.receipt_id = t.receipt_id AND s.index_in_action_receipt = t.index_in_action_receipt
    WHEN NOT MATCHED THEN INSERT *;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## logs

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW temp_view_logs AS 
    SELECT 
    l.block_date
    , l.block_height
    , l.block_timestamp_utc AS block_time
    , l.status AS execution_status
    , l.contract_id AS executor_account_id
    , l.receipt_id
    , l.index_in_execution_outcome_logs
    , l.log
    , IF(startswith(log, 'EVENT_JSON'), substring(log,12), NULL) AS event
    , CAST('{processed_time}' AS TIMESTAMP) AS _processed_time
    FROM mainnet.silver_execution_outcome_logs l
    -- limit to last 1 day partitions for potential later arrival records
    WHERE l.block_date >= date_trunc('day', now() - interval 1 day);
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS  public_lakehouse.logs 
    PARTITIONED BY (block_date)
    COMMENT 'Execution Outcome Logs decoded. Could have multiple logs per action' AS
    SELECT *
    FROM temp_view_logs
    WHERE 1=2;
""")
spark.sql(f"""
    MERGE INTO public_lakehouse.logs as t
    USING (SELECT * FROM temp_view_logs) s
    ON s.block_date = t.block_date AND s.receipt_id = t.receipt_id AND s.index_in_execution_outcome_logs = t.index_in_execution_outcome_logs
    WHEN NOT MATCHED THEN INSERT *;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ft_transfers

# COMMAND ----------

spark.sql(f"""
    -- ft_transfers
    CREATE OR REPLACE TEMPORARY VIEW temp_view_ft_transfers AS 
    SELECT 
        block_date
        , block_height
        , block_timestamp_utc AS block_time
        , block_hash
        , chunk_hash
        , shard_id
        , standard
        , token_id
        , receipt_id
        , contract_account_id
        , cause
        , status
        , event_memo
        , event_index
        , affected_account_id
        , involved_account_id
        , delta_amount
        , CAST('{processed_time}' AS TIMESTAMP) AS _processed_time
    FROM mainnet.silver_execution_outcome_ft_event_logs ft
    -- limit to last 1 days partitions for potential later arrival records
    WHERE ft.block_date >= date_trunc('day', now() - interval 1 day);
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS  public_lakehouse.ft_transfers 
    PARTITIONED BY (block_date)
    COMMENT 'Fungible tokens transfers' AS
    SELECT *
    FROM temp_view_ft_transfers
    WHERE 1=2;
""")
spark.sql(f"""
    MERGE INTO public_lakehouse.ft_transfers as t
    USING (SELECT * FROM temp_view_ft_transfers) s
    ON s.block_date = t.block_date AND s.receipt_id = t.receipt_id AND s.event_index = t.event_index
    WHEN NOT MATCHED THEN INSERT *;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## nft_transfers

# COMMAND ----------

spark.sql(f"""
    -- nft_transfers
    CREATE OR REPLACE TEMPORARY VIEW temp_view_nft_transfers AS 
    SELECT 
        block_date
        , block_height
        , block_timestamp_utc AS block_time
        , block_hash
        , chunk_hash
        , shard_id
        , standard
        , token_id
        , receipt_id
        , contract_account_id
        , cause
        , status
        , event_memo
        , event_index
        , old_owner_account_id
        , new_owner_account_id
        , authorized_account_id
        , CAST('{processed_time}' AS TIMESTAMP) AS _processed_time
    FROM mainnet.silver_execution_outcome_nft_event_logs nft
    -- limit to last 1 day partitions for potential later arrival records
    WHERE nft.block_date >= date_trunc('day', now() - interval 1 day);
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS  public_lakehouse.nft_transfers 
    PARTITIONED BY (block_date)
    COMMENT 'Non Fungible Tokens transfers' AS
    SELECT *
    FROM temp_view_nft_transfers
    WHERE 1=2;
""")
spark.sql(f"""
    MERGE INTO public_lakehouse.nft_transfers as t
    USING (SELECT * FROM temp_view_nft_transfers) s
    ON s.block_date = t.block_date AND s.receipt_id = t.receipt_id AND s.event_index = t.event_index
    WHEN NOT MATCHED THEN INSERT *;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## circulating_supply

# COMMAND ----------

spark.sql(f"""
    -- circulating_supply
    CREATE OR REPLACE TEMPORARY VIEW temp_view_circulating_supply AS 
    SELECT 
    block_date
    , CAST(computed_at_block_timestamp / 1000000000 AS timestamp) AS computed_at_block_timestamp
    , computed_at_block_hash
    , computed_at_block_height
    , circulating_tokens_supply
    , total_tokens_supply
    , CAST('{processed_time}' AS TIMESTAMP) AS _processed_time
    FROM mainnet.gold_aggregated_circulating_supply c;
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS  public_lakehouse.circulating_supply 
    COMMENT 'Aggregated Daily Total Tokens vs Circulating Supply' AS
    SELECT *
    FROM temp_view_circulating_supply
    WHERE 1=2;
""")
spark.sql(f"""
    MERGE INTO public_lakehouse.circulating_supply as t
    USING (SELECT * FROM temp_view_circulating_supply) s
    ON s.block_date = t.block_date AND s.computed_at_block_hash = t.computed_at_block_hash
    WHEN NOT MATCHED THEN INSERT *;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## near_balances

# COMMAND ----------

spark.sql(f"""
    -- near_balances
    CREATE OR REPLACE TEMPORARY VIEW temp_view_near_balances AS 
    SELECT 
        CAST(epoch_date AS DATE) AS epoch_date
        , CAST(epoch_block_height AS BIGINT) AS epoch_block_height
        , account_id
        , liquid
        , storage_usage
        , unstaked_not_liquid
        , staked
        , reward
        , lockup_account_id
        , lockup_liquid
        , lockup_unstaked_not_liquid
        , lockup_staked
        , lockup_reward
        , CAST('{processed_time}' AS TIMESTAMP) AS _processed_time
    FROM mainnet.silver_accounts_daily_ft_balances b;
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS  public_lakehouse.near_balances 
    COMMENT 'Aggregated Daily Total Tokens vs Circulating Supply' AS
    SELECT *
    FROM temp_view_near_balances
    WHERE 1=2;
""")
spark.sql(f"""
    MERGE INTO public_lakehouse.near_balances as t
    USING (SELECT * FROM temp_view_near_balances) s
    ON s.epoch_date = t.epoch_date AND s.epoch_block_height = t.epoch_block_height AND s.account_id = t.account_id
    WHEN NOT MATCHED THEN INSERT *;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Write AVRO files in GCS 

# COMMAND ----------

def delta_to_avro(table_name, processed_time=processed_time):
    sql = f"SELECT * FROM public_lakehouse.{table_name} WHERE _processed_time = '{processed_time}'"
    df = spark.sql(sql)

    processed_time_folder = datetime.strptime(processed_time, "%Y-%m-%d %H:%M:%S")
    folder = f"gs://near-lakehouse-public/mainnet/avro/{table_name}/{processed_time_folder.strftime('%Y/%m/%d/%H')}" 
    try:
        if df.count() > 0:
            df.write.format("avro").save(folder)
            print("OK", folder, df.count())
        else:
            print("No records ", folder)
    except Exception as e:
        print("ERROR", folder, e)

# COMMAND ----------

delta_to_avro("block_chunks", processed_time)

# COMMAND ----------

delta_to_avro("actions", processed_time)

# COMMAND ----------

delta_to_avro("logs", processed_time)

# COMMAND ----------

delta_to_avro("ft_transfers", processed_time)

# COMMAND ----------

delta_to_avro("nft_transfers", processed_time)

# COMMAND ----------

delta_to_avro("circulating_supply", processed_time)

# COMMAND ----------

delta_to_avro("near_balances", processed_time)

# COMMAND ----------


