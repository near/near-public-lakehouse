-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver Lake Mainnet Pipeline Prod
-- MAGIC
-- MAGIC This notebook creates the silver tables enriching each one with the most common columns to query

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Blocks

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_blocks
COMMENT "Stream of parsed blocks"
TBLPROPERTIES ("quality" = "silver", delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
PARTITIONED BY (block_date) AS
SELECT 
  CAST(header.timestamp / 1000000000 AS timestamp)::date block_date,
  CAST(header.height AS BIGINT) AS block_height,
  CAST(header.timestamp AS BIGINT) as block_timestamp,
  CAST(header.timestamp / 1000000000 AS timestamp) AS block_timestamp_utc,
  header.hash AS block_hash,
  NOW() AS _dlt_synced_utc,
  BIGINT(NOW() - CAST(header.timestamp / 1000000000 AS timestamp)) as _dlt_synced_lag_seconds,
  header.prev_hash AS prev_block_hash,
  header.total_supply AS total_supply,
  header.gas_price AS gas_price,
  author AS author_account_id,
  header,
  chunks
FROM STREAM(mainnet.blocks)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Chunks

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_chunks 
COMMENT "Stream of parsed chunks"
TBLPROPERTIES ("quality" = "silver", delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
PARTITIONED BY (block_date) AS
(
    SELECT 
        b.block_date,
        b.block_height,
        b.block_timestamp,
        b.block_timestamp_utc,
        b.block_hash,
        NOW() AS _dlt_synced_utc,
        BIGINT(NOW() - b.block_timestamp_utc) AS _dlt_synced_lag_seconds,
        c.chunk.header.chunk_hash,
        CAST(c.chunk.header.shard_id AS BIGINT) AS shard_id,
        c.chunk.header.signature,
        CAST(c.chunk.header.gas_limit AS BIGINT) AS gas_limit,
        CAST(c.chunk.header.gas_used AS BIGINT) AS gas_used,
        c.chunk.author AS author_account_id,
        c.chunk,
        CAST(c.receipt_execution_outcomes AS ARRAY<STRING>) AS receipt_execution_outcomes,
        c.state_changes
    FROM STREAM(mainnet.chunks) AS c
    JOIN STREAM(live.silver_blocks) AS b ON c.chunk.header.prev_block_hash = b.header.prev_hash
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Account Changes

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_account_changes
COMMENT "Stream of parsed account changes"
TBLPROPERTIES ("quality" = "silver", delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
PARTITIONED BY (block_date) AS
WITH state_changes AS (
	SELECT 
		c.block_date,
		c.block_height,
		c.block_timestamp,
		c.block_timestamp_utc,
		c.block_hash,
		c.chunk_hash,
		NOW() AS _dlt_synced_utc,
		BIGINT(NOW() - c.block_timestamp_utc) as _dlt_synced_lag_seconds,
		posexplode(c.state_changes) AS (index_in_block, state_change)
	FROM STREAM(live.silver_chunks) AS c
)

SELECT 
	block_date,
	block_height,
	block_timestamp,
	block_timestamp_utc,
	block_hash,
	chunk_hash,
	_dlt_synced_utc,
	_dlt_synced_lag_seconds,
	index_in_block,
	sc.state_change.change.account_id AS affected_account_id,
	IF(sc.state_change.cause.type = 'transaction_processing', sc.state_change.cause.tx_hash, NULL) AS caused_by_transaction_hash,
	IF(sc.state_change.cause.type IN ('action_receipt_processing_started', 'action_receipt_gas_reward', 'receipt_processing', 'postponed_receipt'), sc.state_change.cause.receipt_hash, NULL) AS caused_by_receipt_id,
	sc.state_change.cause.type AS update_reason,
	sc.state_change.change.amount AS affected_account_nonstaked_balance,
	sc.state_change.change.locked AS affected_account_staked_balance,
	sc.state_change.change.storage_usage AS affected_account_storage_usage,
	sc.state_change
FROM state_changes sc
WHERE state_change.type = "account_update"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Transactions

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_transactions
COMMENT "Stream of parsed transactions"
TBLPROPERTIES ("quality" = "silver", delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
PARTITIONED BY (block_date) AS
WITH txs AS (
  SELECT 
    c.block_date,
		c.block_height,
		c.block_timestamp,
		c.block_timestamp_utc,
		c.block_hash,
		c.chunk_hash,
    c.shard_id,
		NOW() AS _dlt_synced_utc,
		BIGINT(NOW() - c.block_timestamp_utc) as _dlt_synced_lag_seconds,
    posexplode(c.chunk.transactions) AS (index_in_chunk, tx)
  FROM STREAM(live.silver_chunks) AS c 
), txs_parsed AS (
  SELECT
    t.*,
    t.index_in_chunk,
    from_json(tx, 'STRUCT<outcome: STRUCT<execution_outcome: STRUCT<block_hash: STRING, id: STRING, outcome: STRUCT<executor_id: STRING, gas_burnt: BIGINT, logs: ARRAY<STRING>, metadata: STRUCT<gas_profile: STRING, version: BIGINT>, receipt_ids: ARRAY<STRING>, status: STRUCT<SuccessValue: STRING, SuccessReceiptId: STRING, Failure: STRUCT<ActionError: STRUCT<index: BIGINT, kind: STRUCT<FunctionCallError: STRUCT<HostError: STRUCT<GuestPanic: STRUCT<panic_msg: STRING>>>>>>>, tokens_burnt: STRING>, proof: ARRAY<STRING>>, receipt: STRUCT<predecessor_id: STRING, receipt: STRUCT<Action: STRUCT<actions: ARRAY<STRUCT<FunctionCall: STRUCT<args: STRING, deposit: STRING, gas: BIGINT, method_name: STRING>>>, gas_price: STRING, input_data_ids: ARRAY<STRING>, output_data_receivers: ARRAY<STRING>, signer_id: STRING, signer_public_key: STRING>>>, receipt_id: STRING, receiver_id: STRING>, transaction: STRUCT<hash: STRING, nonce: BIGINT, public_key: STRING, receiver_id: STRING, signature: STRING, signer_id: STRING, actions: ARRAY<STRING>>>') as json 
  FROM txs t
)
SELECT 
  tp.block_date,
  tp.block_height,
  tp.block_timestamp,
  tp.block_timestamp_utc,
  tp.block_hash,
  tp.chunk_hash,
  tp.shard_id,
  tp._dlt_synced_utc,
  tp._dlt_synced_lag_seconds,
  tp.json.transaction.hash,
  tp.index_in_chunk,
  tp.json.transaction.nonce,
  tp.json.transaction.signer_id,
  tp.json.transaction.public_key,
  tp.json.transaction.signature,
  tp.json.transaction.receiver_id,
  tp.json.outcome.execution_outcome.outcome.receipt_ids[0] as converted_into_receipt_id,
  CASE
    WHEN tp.json.outcome.execution_outcome.outcome.status.SuccessReceiptId IS NOT NULL THEN 'SUCCESS_RECEIPT_ID'
    WHEN tp.json.outcome.execution_outcome.outcome.status.SuccessValue IS NOT NULL THEN 'SUCCESS_VALUE'
    WHEN tp.json.outcome.execution_outcome.outcome.status.Failure IS NOT NULL THEN 'FAILURE'
    ELSE 'UNKNOWN'
  END as status,
  tp.json.outcome.execution_outcome.outcome.gas_burnt as receipt_conversion_gas_burnt,
  tp.json.outcome.execution_outcome.outcome.tokens_burnt as receipt_conversion_tokens_burnt,
  tp.json.transaction.actions
FROM txs_parsed tp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Transaction Actions

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_transaction_actions
COMMENT "Stream of parsed transaction actions"
TBLPROPERTIES ("quality" = "silver", delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
PARTITIONED BY (block_date) AS
WITH ta_parsed AS (
  SELECT 
    block_date,
    block_height,
    block_timestamp,
    block_timestamp_utc,
    hash as transaction_hash,
    status as transaction_status,
    converted_into_receipt_id,
    signer_id,
    receiver_id,
    public_key,
    posexplode(t.actions) AS (index_in_transaction, action)
  FROM STREAM(live.silver_transactions) AS t
)
SELECT 
  block_date,
  block_height,
  block_timestamp,
  block_timestamp_utc,
  NOW() AS _dlt_synced_utc,
  BIGINT(NOW() - CAST(block_timestamp / 1000000000 AS timestamp)) as _dlt_synced_lag_seconds,
  transaction_hash,
  transaction_status,
  converted_into_receipt_id,
  signer_id,
  receiver_id,
  public_key,
  index_in_transaction,
  CASE
    WHEN contains(action, 'CreateAccount') THEN 'CREATE_ACCOUNT'
    WHEN contains(action, 'DeployContract') THEN 'DEPLOY_CONTRACT'
    WHEN contains(action, 'Transfer') THEN 'TRANSFER'
    WHEN contains(action, 'Stake') THEN 'STAKE'
    WHEN contains(action, 'AddKey') THEN 'ADD_KEY'
    WHEN contains(action, 'DeleteKey') THEN 'DELETE_KEY'
    WHEN contains(action, 'DeleteAccount') THEN 'DELETE_ACCOUNT'
    WHEN contains(action, 'Delegate') THEN 'DELEGATE_ACTION'
    WHEN contains(action, 'FunctionCall') THEN 'FUNCTION_CALL'
    ELSE 'UNKNOWN'
  END as action_kind,
  CASE
    WHEN contains(action, 'CreateAccount') THEN '{}'
    WHEN contains(action, 'DeployContract') THEN CONCAT('{"code_sha256":"', lower(CAST(hex(unbase64(from_json(action, 'STRUCT<DeployContract: STRUCT<code:STRING>>').DeployContract.code)) AS STRING)), '"}')
    WHEN contains(action, 'Transfer') THEN CAST(from_json(action, 'STRUCT<Transfer: STRING>').Transfer AS STRING)
    WHEN contains(action, 'Stake') THEN CAST(from_json(action, 'STRUCT<Stake: STRING>').Stake AS STRING)
    WHEN contains(action, 'AddKey') THEN CAST(from_json(action, 'STRUCT<AddKey: STRING>').AddKey AS STRING)
    WHEN contains(action, 'DeleteKey') THEN CAST(from_json(action, 'STRUCT<DeleteKey: STRING>').DeleteKey AS STRING)
    WHEN contains(action, 'DeleteAccount') THEN CAST(from_json(action, 'STRUCT<DeleteAccount: STRING>').DeleteAccount AS STRING)
    WHEN contains(action, 'Delegate') THEN action
    WHEN contains(action, 'FunctionCall') THEN 
      CONCAT('{',
        '"gas": "', CAST(from_json(action, 'STRUCT<FunctionCall: STRUCT<gas: BIGINT>>').FunctionCall.gas AS STRING), '",',
        '"deposit": "', CAST(from_json(action, 'STRUCT<FunctionCall: STRUCT<deposit: STRING>>').FunctionCall.deposit AS STRING), '",',
        '"args_base64": "', CAST(from_json(action, 'STRUCT<FunctionCall: STRUCT<args: STRING>>').FunctionCall.args AS STRING), '",',
        '"method_name": "', CAST(from_json(action, 'STRUCT<FunctionCall: STRUCT<method_name: STRING>>').FunctionCall.method_name AS STRING), '"',
       '}')
    ELSE action
  END as args
FROM ta_parsed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Transaction Actions Function Calls

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_transaction_actions_function_calls
COMMENT "Stream of parsed transaction actions function calls"
TBLPROPERTIES ("quality" = "silver", delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
PARTITIONED BY (block_date) AS
SELECT
  block_date,
  block_height,
  block_timestamp,
  block_timestamp_utc,
  NOW() AS _dlt_synced_utc,
  BIGINT(NOW() - CAST(block_timestamp / 1000000000 AS timestamp)) as _dlt_synced_lag_seconds,
  transaction_hash,
  transaction_status,
  converted_into_receipt_id,
  signer_id,
  receiver_id,
  public_key,
  CAST(from_json(args, 'STRUCT<method_name: STRING>').method_name AS STRING) as method_name, 
  CAST(from_json(args, 'STRUCT<gas: STRING>').gas AS STRING) AS gas,
  CAST(from_json(args, 'STRUCT<deposit: STRING>').deposit AS STRING) AS deposit,
  CAST(from_json(args, 'STRUCT<args_base64: STRING>').args_base64 AS STRING) as args_base64,
  CAST(unbase64(from_json(args, 'STRUCT<args_base64: STRING>').args_base64) AS STRING) as args_parsed
FROM STREAM(live.silver_transaction_actions) 
WHERE action_kind = 'FUNCTION_CALL' AND block_date >= '2024-06-01'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Execution Outcomes

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_execution_outcomes
COMMENT "Stream of parsed execution outcomes"
TBLPROPERTIES ("quality" = "silver", delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
PARTITIONED BY (block_date) AS
WITH reos AS (
  SELECT 
    c.block_date,
		c.block_height,
		c.block_timestamp,
		c.block_timestamp_utc,
		c.block_hash,
		c.chunk_hash,
    c.shard_id,
		NOW() AS _dlt_synced_utc,
		BIGINT(NOW() - c.block_timestamp_utc) as _dlt_synced_lag_seconds,
    posexplode(c.receipt_execution_outcomes) AS (index_in_chunk, reo)
  FROM STREAM(live.silver_chunks) AS c
)
, reo_parsed AS (
  SELECT
    reos.*,
    from_json(reos.reo, 'STRUCT<execution_outcome: STRUCT<block_hash: STRING, id: STRING, outcome: STRUCT<executor_id: STRING, gas_burnt: BIGINT, logs: ARRAY<STRING>, metadata: STRUCT<gas_profile: STRING, version: BIGINT>, receipt_ids: ARRAY<STRING>, status: STRUCT<SuccessValue: STRING, SuccessReceiptId: STRING, Failure: STRUCT<ActionError: STRUCT<index: BIGINT, kind: STRING>>>, tokens_burnt: STRING>, proof: ARRAY<STRING>>, receipt: STRUCT<predecessor_id: STRING, receipt: STRUCT<Action: STRUCT<actions: ARRAY<STRUCT<FunctionCall: STRUCT<args: STRING, deposit: STRING, gas: BIGINT, method_name: STRING>>>, gas_price: STRING, input_data_ids: ARRAY<STRING>, output_data_receivers: ARRAY<STRING>, signer_id: STRING, signer_public_key: STRING>>, receipt_id: STRING, receiver_id: STRING>, transaction: STRUCT<hash: STRING, nonce: BIGINT, public_key: STRING, receiver_id: STRING, signature: STRING, signer_id: STRING, actions: ARRAY<STRING>>>') as json
  FROM reos
)
SELECT
  reop.block_date,
	reop.block_height,
	reop.block_timestamp,
	reop.block_timestamp_utc,
	reop.block_hash,
	reop.chunk_hash,
  reop.shard_id,
	reop._dlt_synced_utc,
	reop._dlt_synced_lag_seconds,
  reop.json.execution_outcome.id as receipt_id,
  reop.json.execution_outcome.block_hash as executed_in_block_hash,
  reop.json.execution_outcome.outcome.receipt_ids as outcome_receipt_ids,
  reop.index_in_chunk,
  reop.json.execution_outcome.outcome.gas_burnt as gas_burnt,
  reop.json.execution_outcome.outcome.tokens_burnt as tokens_burnt,
  reop.json.execution_outcome.outcome.executor_id as executor_account_id,
    CASE
    WHEN reop.json.execution_outcome.outcome.status.SuccessReceiptId IS NOT NULL THEN 'SUCCESS_RECEIPT_ID'
    WHEN reop.json.execution_outcome.outcome.status.SuccessValue IS NOT NULL THEN 'SUCCESS_VALUE'
    WHEN reop.json.execution_outcome.outcome.status.Failure IS NOT NULL THEN 'FAILURE'
    ELSE 'UNKNOWN'
  END as status,
  reop.json.execution_outcome.outcome.logs
FROM reo_parsed reop

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Execution Outcomes Logs

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_execution_outcome_logs
COMMENT "Stream of parsed execution outcome logs"
TBLPROPERTIES ("quality" = "silver", delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
PARTITIONED BY (block_date) AS
WITH eo_parsed AS (
  SELECT
    *,
    posexplode(eo.logs) AS (index_in_execution_outcome_logs, log)
  FROM STREAM(live.silver_execution_outcomes) AS eo
  WHERE size(eo.logs) > 0
)
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
  index_in_chunk,
  executor_account_id as contract_id,
  status,
  receipt_id,
  index_in_execution_outcome_logs,
  log
FROM eo_parsed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Execution Outcomes FT and NFT Event Logs
-- MAGIC Please check the Scala Pipeline [Silver Lake Mainnet - FT and NFT events](https://4221960800361869.9.gcp.databricks.com/?o=4221960800361869#notebook/56301286935549/command/56301286935570)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Execution Outcomes Receipts

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_execution_outcome_receipts
COMMENT "Stream of parsed execution outcome receipts"
TBLPROPERTIES ("quality" = "silver", delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
PARTITIONED BY (block_date) AS
WITH eo_parsed AS (
  SELECT
    *,
    posexplode(eo.outcome_receipt_ids) AS (index_in_execution_outcome, outcome_receipt_id)
  FROM STREAM(live.silver_execution_outcomes) AS eo
)
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
  index_in_chunk,
  gas_burnt,
  tokens_burnt,
  executor_account_id,
  status,
  receipt_id as executed_receipt_id,
  index_in_execution_outcome,
  outcome_receipt_id as produced_receipt_id
FROM eo_parsed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Receipts

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_receipts
COMMENT "Stream of parsed receipts"
TBLPROPERTIES ("quality" = "silver", delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
PARTITIONED BY (block_date) AS
WITH rs AS (
  SELECT 
    c.block_date,
		c.block_height,
		c.block_timestamp,
		c.block_timestamp_utc,
		c.block_hash,
		c.chunk_hash,
    c.shard_id,
		NOW() AS _dlt_synced_utc,
		BIGINT(NOW() - c.block_timestamp_utc) as _dlt_synced_lag_seconds,
    posexplode(c.chunk.receipts) AS (index_in_chunk, receipt)
  FROM STREAM(live.silver_chunks) AS c
) 

SELECT 
  rs.block_date,
  rs.block_height,
  rs.block_timestamp,
  rs.block_timestamp_utc,
  rs.block_hash,
  rs.chunk_hash,
  rs.shard_id,
  NOW() AS _dlt_synced_utc,
  BIGINT(NOW() - rs.block_timestamp_utc) as _dlt_synced_lag_seconds,
  CASE
    WHEN rs.receipt:receipt:data:data_id IS NOT NULL THEN 'DATA'
    WHEN rs.receipt:receipt_id IS NOT NULL THEN 'ACTION'
    ELSE 'UNKNOWN'
  END as receipt_kind,
  rs.receipt:receipt_id,
  rs.receipt:receipt:data:data_id,
  rs.index_in_chunk,
  rs.receipt:predecessor_id as predecessor_account_id,
  rs.receipt:receiver_id as receiver_account_id,
  rs.receipt
FROM rs 

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_receipt_originated_from_transaction
COMMENT "Stream of parsed receipt originated from transaction"
TBLPROPERTIES ("quality" = "silver", delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
PARTITIONED BY (block_date) AS
SELECT 
  rs.block_date,
  rs.block_height,
  NOW() as _dlt_synced_utc,
  BIGINT(NOW() - rs.block_timestamp_utc) as _dlt_synced_lag_seconds,
  rs.receipt_id,
  rs.data_id,
  rs.receipt_kind,
  '' as originated_from_transaction_hash,
   NOW() as _record_last_updated_utc
FROM STREAM(live.silver_receipts) AS rs 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Action Receipts

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_action_receipts
COMMENT "Stream of parsed action receipts"
TBLPROPERTIES ("quality" = "silver", delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
PARTITIONED BY (block_date) AS
SELECT
  sr.block_date,
  sr.block_height,
  sr.block_timestamp,
  sr.block_timestamp_utc,
  sr.block_hash,
  sr.chunk_hash,
  sr.shard_id,
  NOW() AS _dlt_synced_utc,
  BIGINT(NOW() - sr.block_timestamp_utc) as _dlt_synced_lag_seconds,
  sr.receipt_id,
  sr.receipt:receipt:Action:gas_price,
  sr.receipt:receipt:Action:signer_id as signer_account_id,
  sr.receipt:receipt:Action:signer_public_key,
  receipt
FROM STREAM(live.silver_receipts) AS sr
WHERE sr.receipt_kind = 'ACTION'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Action Receipt Actions

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_action_receipt_actions
COMMENT "Stream of parsed action receipt actions"
TBLPROPERTIES ("quality" = "silver", delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
PARTITIONED BY (block_date) AS
WITH receipt_actions AS (
  SELECT
    ar.*,
    posexplode(from_json(ar.receipt:receipt:Action, 'STRUCT<actions: ARRAY<STRING>>').actions) AS (index_in_action_receipt, action)
  FROM STREAM(live.silver_action_receipts) AS ar
)

SELECT 
  ra.block_date,
  ra.block_height,
  ra.block_timestamp,
  ra.block_timestamp_utc,
  ra.block_hash,
  ra.chunk_hash,
  ra.shard_id,
  NOW() AS _dlt_synced_utc,
  BIGINT(NOW() - ra.block_timestamp_utc) as _dlt_synced_lag_seconds,
  ra.index_in_action_receipt,
  ra.receipt_id,
  ra.gas_price,
  ra.signer_account_id,
  ra.signer_public_key,
  CASE
    WHEN contains(action, 'CreateAccount') THEN '{}'
    WHEN contains(action, 'DeployContract') THEN CONCAT('{"code_sha256":"', lower(CAST(hex(unbase64(from_json(action, 'STRUCT<DeployContract: STRUCT<code:STRING>>').DeployContract.code)) AS STRING)), '"}')
    WHEN contains(action, 'Transfer') THEN CAST(from_json(action, 'STRUCT<Transfer: STRING>').Transfer AS STRING)
    WHEN contains(action, 'Stake') THEN CAST(from_json(action, 'STRUCT<Stake: STRING>').Stake AS STRING)
    WHEN contains(action, 'AddKey') THEN CAST(from_json(action, 'STRUCT<AddKey: STRING>').AddKey AS STRING)
    WHEN contains(action, 'DeleteKey') THEN CAST(from_json(action, 'STRUCT<DeleteKey: STRING>').DeleteKey AS STRING)
    WHEN contains(action, 'DeleteAccount') THEN CAST(from_json(action, 'STRUCT<DeleteAccount: STRING>').DeleteAccount AS STRING)
    WHEN contains(action, 'Delegate') THEN action
    WHEN contains(action, 'FunctionCall') THEN 
      CONCAT('{',
        '"gas": "', CAST(from_json(action, 'STRUCT<FunctionCall: STRUCT<gas: BIGINT>>').FunctionCall.gas AS STRING), '",',
        '"deposit": "', CAST(from_json(action, 'STRUCT<FunctionCall: STRUCT<deposit: STRING>>').FunctionCall.deposit AS STRING), '",',
        '"args_base64": "', CAST(from_json(action, 'STRUCT<FunctionCall: STRUCT<args: STRING>>').FunctionCall.args AS STRING), '",',
        '"method_name": "', CAST(from_json(action, 'STRUCT<FunctionCall: STRUCT<method_name: STRING>>').FunctionCall.method_name AS STRING), '"',
       '}')    
    ELSE action
  END as args,
  ra.receipt:predecessor_id as receipt_predecessor_account_id,
  CASE
    WHEN contains(action, 'CreateAccount') THEN 'CREATE_ACCOUNT'
    WHEN contains(action, 'DeployContract') THEN 'DEPLOY_CONTRACT'
    WHEN contains(action, 'Transfer') THEN 'TRANSFER'
    WHEN contains(action, 'Stake') THEN 'STAKE'
    WHEN contains(action, 'AddKey') THEN 'ADD_KEY'
    WHEN contains(action, 'DeleteKey') THEN 'DELETE_KEY'
    WHEN contains(action, 'DeleteAccount') THEN 'DELETE_ACCOUNT'
    WHEN contains(action, 'Delegate') THEN 'DELEGATE_ACTION'
    WHEN contains(action, 'FunctionCall') THEN 'FUNCTION_CALL'
    ELSE 'UNKNOWN'
  END as action_kind,
  ra.receipt:receiver_id as receipt_receiver_account_id,
  contains(action, 'DelegateAction') as is_delegate_action
FROM receipt_actions ra

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Validators Receipt Actions

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_validators_receipt_actions
COMMENT "Stream of validators receipt actions"
TBLPROPERTIES ("quality" = "silver", delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
PARTITIONED BY (block_date) AS
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
  args,
  action_kind,
  CASE 
    WHEN action_kind = 'FUNCTION_CALL' AND args:method_name LIKE 'deposit%' THEN 'STAKE'
    WHEN action_kind = 'FUNCTION_CALL' AND args:method_name IN ('unstake', 'unstake_all') THEN 'UNSTAKE' 
    WHEN action_kind = 'TRANSFER' THEN 'WITHDRAW' 
  END AS direction,
  CASE 
    WHEN action_kind = 'FUNCTION_CALL' AND args:method_name LIKE 'deposit%' THEN receipt_receiver_account_id
    WHEN action_kind = 'FUNCTION_CALL' AND args:method_name IN ('unstake', 'unstake_all') THEN receipt_receiver_account_id
    WHEN action_kind = 'TRANSFER' THEN receipt_predecessor_account_id
  END AS validator_id,
  CASE 
    WHEN action_kind = 'FUNCTION_CALL' AND args:method_name LIKE 'deposit%' THEN receipt_predecessor_account_id
    WHEN action_kind = 'FUNCTION_CALL' AND args:method_name IN ('unstake', 'unstake_all') THEN receipt_predecessor_account_id
    WHEN action_kind = 'TRANSFER' THEN receipt_receiver_account_id
  END AS delegator_id
FROM STREAM(live.silver_action_receipt_actions) 
WHERE
  (
    (action_kind = 'FUNCTION_CALL' AND (args:method_name LIKE 'deposit%' OR args:method_name IN ('unstake', 'unstake_all'))) 
    OR (action_kind = 'TRANSFER')
  )
  AND
  ( receipt_receiver_account_id LIKE ANY('%.poolv1.near','%.pool.near')
    OR receipt_predecessor_account_id LIKE ANY('%.poolv1.near','%.pool.near')
  )


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Data Receipts

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_data_receipts
COMMENT "Stream of parsed data receipts"
TBLPROPERTIES ("quality" = "silver", delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
PARTITIONED BY (block_date) AS
SELECT
  sr.block_date,
  sr.block_height,
  sr.block_timestamp,
  sr.block_timestamp_utc,
  sr.block_hash,
  sr.chunk_hash,
  sr.shard_id,
  NOW() AS _dlt_synced_utc,
  BIGINT(NOW() - sr.block_timestamp_utc) as _dlt_synced_lag_seconds,
  sr.receipt_id,
  sr.data_id,
  unbase64(receipt:receipt:Data:data) as data
FROM STREAM(live.silver_receipts) AS sr
WHERE sr.receipt_kind = 'DATA'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Action Receipt Output Data

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_action_receipt_output_data
COMMENT "Stream of parsed action receipt output data"
TBLPROPERTIES ("quality" = "silver", delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
PARTITIONED BY (block_date) AS
WITH odr_parsed AS (
  SELECT 
    block_date,
    block_height,
    block_timestamp,
    block_timestamp_utc,
    block_hash,
    chunk_hash,
    shard_id,
    receipt_id,
    explode(from_json(r.receipt:receipt:Action:output_data_receivers, 'ARRAY<STRING>')) as json_data 
    FROM STREAM(live.silver_receipts) AS r 
) 
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
  json_data:data_id as output_data_id,
  receipt_id as output_from_receipt_id, 
  json_data:receiver_id as receiver_account_id 
from odr_parsed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Action Receipt Input Data

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_action_receipt_input_data
COMMENT "Stream of parsed action receipt input data"
TBLPROPERTIES ("quality" = "silver", delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
PARTITIONED BY (block_date) AS
SELECT
  block_date,
  block_height,
  block_timestamp,
  block_timestamp_utc,
  block_hash,
  chunk_hash,
  shard_id,
  receipt_id as input_to_receipt_id,
  explode(from_json(r.receipt:receipt:Action:input_data_ids, 'ARRAY<STRING>')) as input_data_id
FROM STREAM(live.silver_receipts) AS r

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### NEAR Social Txs and Deployed Contracts
-- MAGIC
-- MAGIC Please check the [Silver Lake Mainnet - NEAR Social](https://4221960800361869.9.gcp.databricks.com/?o=4221960800361869#notebook/1224517226747107/command/1224517226747154) pipeline.

-- COMMAND ----------


