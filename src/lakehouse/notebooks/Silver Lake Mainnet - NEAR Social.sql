-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver Lake Mainnet - NEAR Social
-- MAGIC
-- MAGIC This notebook creates the NEAR Social silver tables enriching each one with the most common columns to query
-- MAGIC
-- MAGIC Notes:
-- MAGIC - Set the pipeline config `spark.sql.streaming.stateStore.providerClass` to `com.databricks.sql.streaming.state.RocksDBStateStoreProvider`
-- MAGIC - Set the pipeline config channel to `Current`, the option `Preview` doesn't allow stream left join
-- MAGIC - We start the streaming from `2024-06-01` for older data we do it with INSERTs. Notebook: [Silver Lake Mainnet - NEAR Social Backfill](https://4221960800361869.9.gcp.databricks.com/?o=4221960800361869#notebook/3990047340828052/command/3990047340828053)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_near_social_txs
COMMENT "Stream of receipts on mainnet for social.near."
TBLPROPERTIES ("quality" = "silver", delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
PARTITIONED BY (block_date) AS
SELECT 
  sara.block_height,
  sara.block_timestamp_utc,
  sara.block_date,
  sara.args,
  sara.signer_account_id,
  sara.receipt_predecessor_account_id as predecessor_id,
  sara.receipt_id,
  seo.executor_account_id as contract_id,
  seo.gas_burnt, 
  seo.status
FROM STREAM(public_lakehouse.silver_action_receipt_actions) WATERMARK block_timestamp_utc AS sara_ts DELAY OF INTERVAL 1 DAY AS sara 
JOIN STREAM(public_lakehouse.silver_execution_outcomes) WATERMARK block_timestamp_utc AS seo_ts DELAY OF INTERVAL 1 DAY AS seo ON seo.receipt_id = sara.receipt_id AND seo.block_date >= '2024-06-01'
AND sara.receipt_receiver_account_id = 'social.near' 
AND sara.block_date >= '2024-06-01'

-- COMMAND ----------

-- This table fields were update based on jo.yang@near.org query
CREATE OR REFRESH STREAMING LIVE TABLE silver_near_social_txs_parsed
COMMENT "Stream of parsed txs on mainnet for social.near."
TBLPROPERTIES ("quality" = "silver", delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
PARTITIONED BY (block_date)
(
with 
  parsed AS (
      SELECT 
          block_height,
          nst.block_timestamp_utc,
          block_date,
          CAST(unbase64(args:args_base64) AS STRING) AS args,
          signer_account_id AS signer_id,
          case when signer_account_id = 'relayer.pagodaplatform.near' then predecessor_id else signer_account_id end as true_signer_id, -- in case of action being delegated to relayer
          predecessor_id,
          receipt_id,
          contract_id,
          status,
          args:method_name,
          args:deposit,
          args:gas
      FROM STREAM(live.silver_near_social_txs) WATERMARK block_timestamp_utc AS nst_ts DELAY OF INTERVAL 1 DAY AS nst
      WHERE signer_account_id != "social.near"
      AND nst.block_date >= '2024-06-01'
  )
  -- Dec 2023 Fix #1: address edge case where socialDB user is different from signer; example receipt id = '3qrRxs6bGQ5Dpds6rABST4ukZEmkcXDxnifN3fpw8D1W'
  , object_keys AS (
      SELECT 
        receipt_id, 
        block_timestamp_utc,
        explode(json_object_keys(CAST(UNBASE64(args:args_base64) AS STRING):data)) AS object_key
      FROM STREAM(live.silver_near_social_txs) WATERMARK block_timestamp_utc AS nst_ts DELAY OF INTERVAL 1 DAY AS ok
      WHERE status != 'FAILURE'
      AND ok.block_date >= '2024-06-01'
  )
  , account_object AS (
      SELECT 
        block_height,
        -- block_utc,
        parsed.block_timestamp_utc,
        block_date,
        get_json_object(args:data,concat('$["', obj.object_key ,'"]')) AS account_object,
        signer_id,
        COALESCE(obj.object_key, true_signer_id) AS true_signer_id, -- technically socialDB user; Dec 2023 Fix #1
        predecessor_id,
        parsed.receipt_id,
        contract_id,
        status,
        method_name,
        deposit,
        gas
      FROM parsed WATERMARK block_timestamp_utc AS parsed_ts DELAY OF INTERVAL 1 DAY
      LEFT JOIN object_keys WATERMARK block_timestamp_utc AS obj_ts DELAY OF INTERVAL 1 DAY AS obj ON obj.receipt_id = parsed.receipt_id
    )

    SELECT 
        block_height,
        block_timestamp_utc,
        block_date,
        signer_id,
        true_signer_id,
        predecessor_id,
        receipt_id,
        contract_id,
        status,
        method_name,
        deposit,
        gas,
        account_object,
        account_object:widget, -- edit widget
        account_object:post, -- post or comment
        account_object:profile, -- edit profile
        account_object:graph, -- follow / hide 
        account_object:settings,
        account_object:badge, 
        account_object:index -- like / follow / poke / comment / post / notify
    FROM account_object
    where status != 'FAILURE' -- Dec 2023 Fix #2: the downstream pipelines are built on the assumption that this table only contains successful txs  
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Deployed Contracts

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_deployed_contracts
COMMENT "Stream of parsed deployed contracts"
TBLPROPERTIES ("quality" = "silver", delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
PARTITIONED BY (block_date) AS
SELECT
  sara.block_date,
  sara.block_height,
  sara.block_timestamp,
  sara.block_timestamp_utc,
  sara.block_hash,
  sara.chunk_hash,
  sara.shard_id,
  NOW() AS _dlt_synced_utc,
  BIGINT(NOW() - sara.block_timestamp_utc) as _dlt_synced_lag_seconds,
  eo.block_timestamp_utc as deployed_at_block_timestamp,
  eo.block_hash as deployed_at_block_hash,
  sara.receipt_id as deployed_by_receipt_id,
  sara.args:code_sha256 as contract_code_sha256,
  sara.receipt_receiver_account_id as deployed_to_account_id
FROM STREAM(public_lakehouse.silver_action_receipt_actions) WATERMARK block_timestamp_utc AS sara_ts DELAY OF INTERVAL 1 DAY AS sara
  JOIN STREAM(public_lakehouse.silver_execution_outcomes)  WATERMARK block_timestamp_utc AS eo_ts DELAY OF INTERVAL 1 DAY AS eo ON sara.block_date = eo.block_date AND sara.receipt_id = eo.receipt_id AND eo.status = 'SUCCESS_VALUE' AND eo.block_date >= '2024-06-01'
WHERE sara.action_kind = 'DEPLOY_CONTRACT' AND sara.block_date >= '2024-06-01'

