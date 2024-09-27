-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver Lake Mainnet - SCD tables
-- MAGIC
-- MAGIC This notebook creates the SCD silver tables enriching each one with the most common columns to query

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_accounts
COMMENT "Stream of parsed accounts. This table is a SCD (slow changing dimension) type 1 that upsert rows to have the latest account state based on receipt actions CREATE_ACCOUNT, TRANSFER, and DELETE_ACCOUNT."
TBLPROPERTIES ("quality" = "silver", delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
PARTITIONED BY (block_date);

APPLY CHANGES INTO live.silver_accounts
FROM (
  SELECT 
    block_date,
    block_height,
    block_timestamp,
    block_timestamp_utc,
    block_hash,
    chunk_hash,
    shard_id,
    NOW() AS _dlt_synced_utc,
    BIGINT(NOW() - block_timestamp_utc) as _dlt_synced_lag_seconds,
    index_in_action_receipt,
    receipt_id,
    receipt_receiver_account_id as account_id,
    action_kind,
    not (action_kind = 'DELETE_ACCOUNT') as is_active,
    IF(action_kind <> 'DELETE_ACCOUNT', receipt_id, NULL) as created_by_receipt_id,
    IF(action_kind = 'DELETE_ACCOUNT', receipt_id, NULL) as deleted_by_receipt_id
  FROM STREAM(mainnet.silver_action_receipt_actions) 
  WHERE action_kind IN ('CREATE_ACCOUNT', 'DELETE_ACCOUNT', 'TRANSFER')
)
KEYS (account_id)
IGNORE NULL UPDATES
SEQUENCE BY block_timestamp
STORED AS SCD TYPE 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Access Keys

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_access_keys
COMMENT "Stream of parsed access keys. This table is a SCD (slow changing dimension) type 1 that upsert rows to have the latest keys based on receipt actions ADD_KEY and DELETE_KEY"
TBLPROPERTIES ("quality" = "silver", delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
PARTITIONED BY (block_date);

APPLY CHANGES INTO live.silver_access_keys
FROM (
  SELECT 
    block_date,
    block_height,
    block_timestamp,
    block_timestamp_utc,
    block_hash,
    chunk_hash,
    shard_id,
    NOW() AS _dlt_synced_utc,
    BIGINT(NOW() - block_timestamp_utc) as _dlt_synced_lag_seconds,
    receipt_receiver_account_id as account_id,
    args:public_key,
    IF(args:access_key:permission == 'FullAccess', 'FULL_ACCESS', 'FUNCTION_CALL') as permission_kind,
    action_kind,
    receipt_id,
    not action_kind = 'DELETE_KEY' as is_active
  FROM STREAM(mainnet.silver_action_receipt_actions) 
  WHERE action_kind in ('ADD_KEY', 'DELETE_KEY') 
)
KEYS (account_id, public_key)
IGNORE NULL UPDATES
SEQUENCE BY block_timestamp
STORED AS SCD TYPE 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Action receipt function call methods - for queryapi wizard

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_action_function_call_methods
COMMENT "Stream of action receipt function call methods. This table is a SCD (slow changing dimension) type 1 that upsert rows to have the latest record"
TBLPROPERTIES ("quality" = "silver", delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);

APPLY CHANGES INTO live.silver_action_function_call_methods
FROM (
  SELECT
      eo.block_date,
      eo.block_height,
      eo.block_timestamp,
      eo.block_timestamp_utc,
      eo.block_hash,
      eo.chunk_hash,
      eo.shard_id,
      eo.receipt_id,
      eo.status,
      ara.receipt_receiver_account_id,
      ara.args:method_name as method_name,
      try_cast(unbase64(ara.args:args_base64) AS STRING) AS args
    FROM STREAM(mainnet.silver_action_receipt_actions) WATERMARK block_timestamp_utc AS ts_ara DELAY OF INTERVAL 1 DAY AS ara
    JOIN STREAM(mainnet.silver_execution_outcomes) WATERMARK block_timestamp_utc AS ts_eo DELAY OF INTERVAL 1 DAY AS eo 
      ON ara.block_date = eo.block_date 
      AND ara.receipt_id = eo.receipt_id 
      AND eo.block_timestamp_utc >= ara.block_timestamp_utc
      AND eo.block_timestamp_utc <= ara.block_timestamp_utc + INTERVAL 1 DAY
    WHERE ara.action_kind = 'FUNCTION_CALL'
)
KEYS (receipt_receiver_account_id, method_name)
IGNORE NULL UPDATES
SEQUENCE BY block_timestamp
STORED AS SCD TYPE 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Execution Outcome Logs Events - for queryapi wizard

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_execution_outcome_events
COMMENT "Stream of execution outcome events. This table is a SCD (slow changing dimension) type 1 that upsert rows to have the latest record"
TBLPROPERTIES ("quality" = "silver", delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);

APPLY CHANGES INTO live.silver_execution_outcome_events
FROM (
      WITH log_events AS (
        SELECT
            *,
            substring(log,12) as event
        FROM STREAM(mainnet.silver_execution_outcome_logs) WATERMARK block_timestamp_utc AS ts_eo DELAY OF INTERVAL 1 DAY AS eo 
        WHERE startswith(log, 'EVENT_JSON')
        AND status <> 'FAILURE'
      )
      SELECT 
          le.* except (log), 
          le.event:event as cause, 
          le.event:standard 
      FROM log_events le
      WHERE le.event:event IS NOT NULL
)
KEYS (contract_id, cause)
IGNORE NULL UPDATES
SEQUENCE BY block_timestamp
STORED AS SCD TYPE 1
