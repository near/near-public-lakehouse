-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver Lake Testnet - SCD Tables
-- MAGIC
-- MAGIC This notebook creates the silver SCD tables enriching each one with the most common columns to query

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Accounts

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
  FROM STREAM(testnet.silver_action_receipt_actions)
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
  FROM STREAM(testnet.silver_action_receipt_actions)
  WHERE action_kind in ('ADD_KEY', 'DELETE_KEY') 
)
KEYS (account_id, public_key)
IGNORE NULL UPDATES
SEQUENCE BY block_timestamp
STORED AS SCD TYPE 1
