# Databricks notebook source
# MAGIC %md
# MAGIC # BQ Writer Views & Data Dictionary

# COMMAND ----------

# MAGIC %pip install --upgrade google-api-python-client
# MAGIC %pip install --upgrade google-cloud
# MAGIC %pip install --upgrade google-cloud-bigquery
# MAGIC %pip install --upgrade google-cloud-storage

# COMMAND ----------

import base64
import json
from google.oauth2 import service_account
from google.cloud import bigquery

credentials = dbutils.secrets.get(scope = "gcp_data_platform_bq", key = "credentials")
project_id = dbutils.secrets.get(scope = "gcp_data_platform_bq", key = "project_id")

decoded_string = base64.b64decode(credentials).decode("ascii")
sa_credentials = service_account.Credentials.from_service_account_info(json.loads(decoded_string))
scoped_credentials = sa_credentials.with_scopes(['https://www.googleapis.com/auth/cloud-platform'])

bq_client = bigquery.Client(project=project_id, credentials=scoped_credentials)

# COMMAND ----------

# Blocks
dataset_ref = bigquery.DatasetReference(project_id, "crypto_near_mainnet")
table_ref = dataset_ref.table('blocks')
table = bq_client.get_table(table_ref)

table.description = "A structure that represents an entire block in the NEAR blockchain. Block is the main entity in NEAR Protocol blockchain. Blocks are produced in NEAR Protocol every second."
bq_client.update_table(table, ["description"])

sql = (
        f'ALTER TABLE `{project_id}.crypto_near_mainnet.blocks` '
        'ALTER COLUMN block_date SET OPTIONS (description = \'The date of the Block. Used to partition the table\'), '
        'ALTER COLUMN block_height SET OPTIONS (description = \'The height of the Block\'),'
        'ALTER COLUMN block_timestamp SET OPTIONS (description = \'The timestamp of the Block in nanoseconds\'),'
        'ALTER COLUMN block_timestamp_utc SET OPTIONS (description = \'The timestamp of the Block in UTC\'),'
        'ALTER COLUMN block_hash SET OPTIONS (description = \'The hash of the Block\'),'
        'ALTER COLUMN prev_block_hash SET OPTIONS (description = \'The hash of the previous Block\'),'
        'ALTER COLUMN gas_price SET OPTIONS (description = \'The gas price of the Block\'),'
        'ALTER COLUMN total_supply SET OPTIONS (description = \'The total supply of the Block\'),'
        'ALTER COLUMN author_account_id SET OPTIONS (description = \'The AccountId of the author of the Block\')'
    )
query_job = bq_client.query(sql)  
query_job.result()

# COMMAND ----------

# Chunks
dataset_ref = bigquery.DatasetReference(project_id, "crypto_near_mainnet")
table_ref = dataset_ref.table('chunks')
table = bq_client.get_table(table_ref)

table.description = "A structure that represents a chunk in the NEAR blockchain. Chunk of a Block is a part of a Block from a Shard. The collection of Chunks of the Block forms the NEAR Protocol Block. Chunk contains all the structures that make the Block: Transactions, Receipts, and Chunk Header."
bq_client.update_table(table, ["description"])

sql = (
        f'ALTER TABLE `{project_id}.crypto_near_mainnet.chunks` '
        'ALTER COLUMN block_date SET OPTIONS (description = \'The date of the Block. Used to partition the table\'), '
        'ALTER COLUMN block_height SET OPTIONS (description = \'The height of the Block\'),'
        'ALTER COLUMN block_timestamp SET OPTIONS (description = \'The timestamp of the Block in nanoseconds\'),'
        'ALTER COLUMN block_timestamp_utc SET OPTIONS (description = \'The timestamp of the Block in UTC\'),'
        'ALTER COLUMN block_hash SET OPTIONS (description = \'The hash of the Block\'),'
        'ALTER COLUMN chunk_hash SET OPTIONS (description = \'The hash of the Chunk\'),'
        'ALTER COLUMN shard_id SET OPTIONS (description = \'The shard ID of the Chunk\'),'
        'ALTER COLUMN signature SET OPTIONS (description = \'The signature of the Chunk\'),'
        'ALTER COLUMN gas_limit SET OPTIONS (description = \'The gas limit of the Chunk\'),'
        'ALTER COLUMN gas_used SET OPTIONS (description = \'The amount of gas spent on computations of the Chunk\'),'
        'ALTER COLUMN author_account_id SET OPTIONS (description = \'The AccountId of the author of the Chunk\')'
    )
query_job = bq_client.query(sql)  
query_job.result()

# COMMAND ----------

# Transactions
dataset_ref = bigquery.DatasetReference(project_id, "crypto_near_mainnet")
table_ref = dataset_ref.table('transactions')
table = bq_client.get_table(table_ref)

table.description = "Transaction is the main way of interraction between a user and a blockchain. Transaction contains: Signer account ID, Receiver account ID, and Actions."
bq_client.update_table(table, ["description"])

sql = (
        f'ALTER TABLE `{project_id}.crypto_near_mainnet.transactions` '
        'ALTER COLUMN block_date SET OPTIONS (description = \'The date of the Block. Used to partition the table\'), '
        'ALTER COLUMN block_height SET OPTIONS (description = \'The height of the Block\'),'
        'ALTER COLUMN block_timestamp SET OPTIONS (description = \'The timestamp of the Block in nanoseconds\'),'
        'ALTER COLUMN block_timestamp_utc SET OPTIONS (description = \'The timestamp of the Block in UTC\'),'
        'ALTER COLUMN block_hash SET OPTIONS (description = \'The hash of the Block\'),'
        'ALTER COLUMN chunk_hash SET OPTIONS (description = \'The hash of the Chunk\'),'
        'ALTER COLUMN shard_id SET OPTIONS (description = \'The shard ID of the Chunk\'),'
        'ALTER COLUMN transaction_hash SET OPTIONS (description = \'The transaction hash\'),'
        'ALTER COLUMN index_in_chunk SET OPTIONS (description = \'The index in the Chunk\'),'
        'ALTER COLUMN signer_account_id SET OPTIONS (description = \'An account on which behalf transaction is signed\'),'
        'ALTER COLUMN signer_public_key SET OPTIONS (description = \'An access key which was used to sign a transaction\'),'
        'ALTER COLUMN nonce SET OPTIONS (description = \'Nonce is used to determine order of transaction in the pool. It increments for a combination of `signer_id` and `public_key`\'),'
        'ALTER COLUMN receiver_account_id SET OPTIONS (description = \'Receiver account for this transaction\'),'
        'ALTER COLUMN signature SET OPTIONS (description = \'A signature of a hash of the Borsh-serialized Transaction\'),'
        'ALTER COLUMN converted_into_receipt_id SET OPTIONS (description = \'Receipt ID that the transaction was converted.\'),'
        'ALTER COLUMN receipt_conversion_gas_burnt SET OPTIONS (description = \'Gas burnt in the receipt conversion\'),'
        'ALTER COLUMN receipt_conversion_tokens_burnt SET OPTIONS (description = \'Tokens burnt in the receipt conversion\'),'
        'ALTER COLUMN status SET OPTIONS (description = \'Transaction status\')'
    )
query_job = bq_client.query(sql)  
query_job.result()

# COMMAND ----------

# Execution Outcomes
dataset_ref = bigquery.DatasetReference(project_id, "crypto_near_mainnet")
table_ref = dataset_ref.table('execution_outcomes')
table = bq_client.get_table(table_ref)

table.description = "ExecutionOutcome is the result of execution of Transaction or Receipt. In the result of the Transaction execution will always be a Receipt."
bq_client.update_table(table, ["description"])

sql = (
        f'ALTER TABLE `{project_id}.crypto_near_mainnet.execution_outcomes` '
        'ALTER COLUMN block_date SET OPTIONS (description = \'The date of the Block. Used to partition the table\'), '
        'ALTER COLUMN block_height SET OPTIONS (description = \'The height of the Block\'),'
        'ALTER COLUMN block_timestamp SET OPTIONS (description = \'The timestamp of the Block in nanoseconds\'),'
        'ALTER COLUMN block_timestamp_utc SET OPTIONS (description = \'The timestamp of the Block in UTC\'),'
        'ALTER COLUMN block_hash SET OPTIONS (description = \'The hash of the Block\'),'
        'ALTER COLUMN chunk_hash SET OPTIONS (description = \'The hash of the Chunk\'),'
        'ALTER COLUMN shard_id SET OPTIONS (description = \'The shard ID of the Chunk\'),'
        'ALTER COLUMN receipt_id SET OPTIONS (description = \'The receipt ID\'),'
        'ALTER COLUMN executed_in_block_hash SET OPTIONS (description = \'The Block hash\'),'
        'ALTER COLUMN outcome_receipt_ids SET OPTIONS (description = \'Receipt IDs generated by this transaction or receipt\'),'
        'ALTER COLUMN index_in_chunk SET OPTIONS (description = \'The index in the Chunk\'),'
        'ALTER COLUMN gas_burnt SET OPTIONS (description = \'The amount of the gas burnt by the given transaction or receipt\'),'
        'ALTER COLUMN tokens_burnt SET OPTIONS (description = \'The amount of tokens burnt corresponding to the burnt gas amount. This value does not always equal to the `gas_burnt` multiplied by the gas price, because the prepaid gas price might be lower than the actual gas price and it creates a deficit\'),'
        'ALTER COLUMN executor_account_id SET OPTIONS (description = \'The id of the account on which the execution happens. For transaction this is signer_id, for receipt this is receiver_id\'),'
        'ALTER COLUMN logs SET OPTIONS (description = \'Execution outcome logs\'),'
        'ALTER COLUMN status SET OPTIONS (description = \'Execution status. Contains the result in case of successful execution\')'
    )
query_job = bq_client.query(sql)  
query_job.result()

# COMMAND ----------

# account_changes
dataset_ref = bigquery.DatasetReference(project_id, "crypto_near_mainnet")
table_ref = dataset_ref.table('account_changes')
table = bq_client.get_table(table_ref)

table.description = "Describes how account's state has changed and the reason."
bq_client.update_table(table, ["description"])

sql = (
        f'ALTER TABLE `{project_id}.crypto_near_mainnet.account_changes` '
        'ALTER COLUMN block_date SET OPTIONS (description = \'The date of the Block. Used to partition the table\'), '
        'ALTER COLUMN block_height SET OPTIONS (description = \'The height of the Block\'),'
        'ALTER COLUMN block_timestamp SET OPTIONS (description = \'The timestamp of the Block in nanoseconds\'),'
        'ALTER COLUMN block_timestamp_utc SET OPTIONS (description = \'The timestamp of the Block in UTC\'),'
        'ALTER COLUMN block_hash SET OPTIONS (description = \'The hash of the Block\'),'
        'ALTER COLUMN chunk_hash SET OPTIONS (description = \'The hash of the Chunk\'),'
        'ALTER COLUMN index_in_block SET OPTIONS (description = \'The index in the Block\'),'
        'ALTER COLUMN affected_account_id SET OPTIONS (description = \'Account ID affected by the change\'),'
        'ALTER COLUMN caused_by_transaction_hash SET OPTIONS (description = \'The transaction hash that caused the change\'),'
        'ALTER COLUMN caused_by_receipt_id SET OPTIONS (description = \'The receipt ID that caused the change\'),'
        'ALTER COLUMN update_reason SET OPTIONS (description = \'The update reason\'),'
        'ALTER COLUMN affected_account_nonstaked_balance SET OPTIONS (description = \'Non stacked balance\'),'
        'ALTER COLUMN affected_account_staked_balance SET OPTIONS (description = \'Stacked balance\'),'
        'ALTER COLUMN affected_account_storage_usage SET OPTIONS (description = \'Storage usage\')'
    )
query_job = bq_client.query(sql)  
query_job.result()

# COMMAND ----------

# receipt_details
dataset_ref = bigquery.DatasetReference(project_id, "crypto_near_mainnet")
table_ref = dataset_ref.table('receipt_details')
table = bq_client.get_table(table_ref)

table.description = "All cross-contract (we assume that each account lives in its own shard) communication in Near happens through Receipts. Receipts are stateful in a sense that they serve not only as messages between accounts but also can be stored in the account storage to await DataReceipts. Each receipt has a predecessor_id (who sent it) and receiver_id the current account. "
bq_client.update_table(table, ["description"])

sql = (
        f'ALTER TABLE `{project_id}.crypto_near_mainnet.receipt_details` '
        'ALTER COLUMN block_date SET OPTIONS (description = \'The date of the Block. Used to partition the table\'), '
        'ALTER COLUMN block_height SET OPTIONS (description = \'The height of the Block\'),'
        'ALTER COLUMN block_timestamp SET OPTIONS (description = \'The timestamp of the Block in nanoseconds\'),'
        'ALTER COLUMN block_timestamp_utc SET OPTIONS (description = \'The timestamp of the Block in UTC\'),'
        'ALTER COLUMN block_hash SET OPTIONS (description = \'The hash of the Block\'),'
        'ALTER COLUMN chunk_hash SET OPTIONS (description = \'The hash of the Chunk\'),'
        'ALTER COLUMN shard_id SET OPTIONS (description = \'The shard ID of the Chunk\'),'
        'ALTER COLUMN index_in_chunk SET OPTIONS (description = \'The index in the Chunk\'),'
        'ALTER COLUMN receipt_kind SET OPTIONS (description = \'There are 2 types of Receipt: ACTION and DATA. An ACTION receipt is a request to apply Actions, while a DATA receipt is a result of the application of these actions\'),'
        'ALTER COLUMN receipt_id SET OPTIONS (description = \'An unique id for the receipt\'),'
        'ALTER COLUMN data_id SET OPTIONS (description = \'An unique DATA receipt identifier\'),'
        'ALTER COLUMN predecessor_account_id SET OPTIONS (description = \'The account ID which issued a receipt. In case of a gas or deposit refund, the account ID is system\'),'
        'ALTER COLUMN receiver_account_id SET OPTIONS (description = \'The destination account ID\'),'
        'ALTER COLUMN receipt SET OPTIONS (description = \'Receipt details\')'
    )
query_job = bq_client.query(sql)  
query_job.result()

# COMMAND ----------

# receipt_origin_transaction
dataset_ref = bigquery.DatasetReference(project_id, "crypto_near_mainnet")
table_ref = dataset_ref.table('receipt_origin_transaction')
table = bq_client.get_table(table_ref)

table.description = "Tracks the transaction that originated the receipt"
bq_client.update_table(table, ["description"])

sql = (
        f'ALTER TABLE `{project_id}.crypto_near_mainnet.receipt_origin_transaction` '
        'ALTER COLUMN block_date SET OPTIONS (description = \'The date of the Block. Used to partition the table\'), '
        'ALTER COLUMN block_height SET OPTIONS (description = \'The height of the Block\'),'
        'ALTER COLUMN receipt_kind SET OPTIONS (description = \'There are 2 types of Receipt: ACTION and DATA. An ACTION receipt is a request to apply Actions, while a DATA receipt is a result of the application of these actions\'),'
        'ALTER COLUMN receipt_id SET OPTIONS (description = \'An unique id for the receipt\'),'
        'ALTER COLUMN data_id SET OPTIONS (description = \'An unique DATA receipt identifier\'),'
        'ALTER COLUMN originated_from_transaction_hash SET OPTIONS (description = \'The transaction hash that originated the receipt\')'
    )
query_job = bq_client.query(sql)  
query_job.result()

# COMMAND ----------

# receipt_actions
dataset_ref = bigquery.DatasetReference(project_id, "crypto_near_mainnet")
table_ref = dataset_ref.table('receipt_actions')
table = bq_client.get_table(table_ref)

table.description = "Action Receipt represents a request to apply actions on the receiver_id side. It could be derived as a result of a Transaction execution or another ACTION Receipt processing. Action kind can be: ADD_KEY, CREATE_ACCOUNT, DELEGATE_ACTION, DELETE_ACCOUNT, DELETE_KEY, DEPLOY_CONTRACT, FUNCTION_CALL, STAKE, TRANSFER"
bq_client.update_table(table, ["description"])

sql = (
        f'ALTER TABLE `{project_id}.crypto_near_mainnet.receipt_actions` '
        'ALTER COLUMN block_date SET OPTIONS (description = \'The date of the Block. Used to partition the table\'), '
        'ALTER COLUMN block_height SET OPTIONS (description = \'The height of the Block\'),'
        'ALTER COLUMN block_timestamp SET OPTIONS (description = \'The timestamp of the Block in nanoseconds\'),'
        'ALTER COLUMN block_timestamp_utc SET OPTIONS (description = \'The timestamp of the Block in UTC\'),'
        'ALTER COLUMN block_hash SET OPTIONS (description = \'The hash of the Block\'),'
        'ALTER COLUMN chunk_hash SET OPTIONS (description = \'The hash of the Chunk\'),'
        'ALTER COLUMN shard_id SET OPTIONS (description = \'The shard ID of the Chunk\'),'
        'ALTER COLUMN index_in_action_receipt SET OPTIONS (description = \'The index in the ACTION receipt\'),'
        'ALTER COLUMN receipt_id SET OPTIONS (description = \'An unique id for the receipt\'),'
        'ALTER COLUMN gas_price SET OPTIONS (description = \'The gas fee\'),'
        'ALTER COLUMN signer_account_id SET OPTIONS (description = \'An account on which behalf the origin transaction is signed\'),'
        'ALTER COLUMN signer_public_key SET OPTIONS (description = \'An access key which was used to sign the origin transaction\'),'
        'ALTER COLUMN args SET OPTIONS (description = \'Arguments\'),'
        'ALTER COLUMN receipt_predecessor_account_id SET OPTIONS (description = \'The account ID which issued a receipt. In case of a gas or deposit refund, the account ID is system\'),'
        'ALTER COLUMN action_kind SET OPTIONS (description = \'The action kind: ADD_KEY, CREATE_ACCOUNT	, DELEGATE_ACTION, DELETE_ACCOUNT, DELETE_KEY, DEPLOY_CONTRACT, FUNCTION_CALL, STAKE, TRANSFER\'),'
        'ALTER COLUMN receipt_receiver_account_id SET OPTIONS (description = \'The destination account ID\'),'
        'ALTER COLUMN is_delegate_action SET OPTIONS (description = \'Flag for delegate action\')'
    )
query_job = bq_client.query(sql)  
query_job.result()

# COMMAND ----------

# receipts view

sql = (f"""
    CREATE OR REPLACE VIEW `{project_id}.crypto_near_mainnet.receipts`
    AS 
    SELECT 
    r.*,
    o.originated_from_transaction_hash,
    t.signer_account_id as transaction_signer_account_id,
    t.signer_public_key as transaction_signer_public_key,
    t.status as transaction_status,
    eo.executed_in_block_hash as execution_outcome_executed_in_block_hash,
    eo.outcome_receipt_ids as execution_outcome_receipt_ids,
    eo.gas_burnt as execution_outcome_gas_burnt,
    eo.tokens_burnt as execution_outcome_tokens_burnt,
    eo.executor_account_id as execution_outcome_executor_account_id,
    eo.status as execution_outcome_status
    FROM `{project_id}.crypto_near_mainnet.receipt_details` r
    LEFT JOIN `{project_id}.crypto_near_mainnet.receipt_origin` o ON r.receipt_id = o.receipt_id
    LEFT JOIN `{project_id}.crypto_near_mainnet.transactions` t ON o.originated_from_transaction_hash = t.transaction_hash
    LEFT JOIN `{project_id}.crypto_near_mainnet.execution_outcomes` eo ON eo.receipt_id = r.receipt_id;  
    """)
query_job = bq_client.query(sql)  
print(query_job.result())

dataset_ref = bigquery.DatasetReference(project_id, "crypto_near_mainnet")
table_ref = dataset_ref.table('receipts')
table = bq_client.get_table(table_ref)

table.description = "It's recomended to select only the columns and partitions (block_date) needed to avoid unecessary query costs. This view join the receipt details, the transaction that originated the receipt and the receipt execution outcome. Receipt: All cross-contract (we assume that each account lives in its own shard) communication in Near happens through Receipts. Receipts are stateful in a sense that they serve not only as messages between accounts but also can be stored in the account storage to await DataReceipts. Each receipt has a predecessor_id (who sent it) and receiver_id the current account."
bq_client.update_table(table, ["description"])

sql = (
        f'ALTER VIEW `{project_id}.crypto_near_mainnet.receipts` '
        'ALTER COLUMN block_date SET OPTIONS (description = \'The date of the Block. Used to partition the table\'), '
        'ALTER COLUMN block_height SET OPTIONS (description = \'The height of the Block\'),'
        'ALTER COLUMN block_timestamp SET OPTIONS (description = \'The timestamp of the Block in nanoseconds\'),'
        'ALTER COLUMN block_timestamp_utc SET OPTIONS (description = \'The timestamp of the Block in UTC\'),'
        'ALTER COLUMN block_hash SET OPTIONS (description = \'The hash of the Block\'),'
        'ALTER COLUMN chunk_hash SET OPTIONS (description = \'The hash of the Chunk\'),'
        'ALTER COLUMN shard_id SET OPTIONS (description = \'The shard ID of the Chunk\'),'
        'ALTER COLUMN index_in_chunk SET OPTIONS (description = \'The index in the Chunk\'),'
        'ALTER COLUMN receipt_kind SET OPTIONS (description = \'There are 2 types of Receipt: ACTION and DATA. An ACTION receipt is a request to apply Actions, while a DATA receipt is a result of the application of these actions\'),'
        'ALTER COLUMN receipt_id SET OPTIONS (description = \'An unique id for the receipt\'),'
        'ALTER COLUMN data_id SET OPTIONS (description = \'An unique DATA receipt identifier\'),'
        'ALTER COLUMN predecessor_account_id SET OPTIONS (description = \'The account ID which issued a receipt. In case of a gas or deposit refund, the account ID is system\'),'
        'ALTER COLUMN receiver_account_id SET OPTIONS (description = \'The destination account ID\'),'
        'ALTER COLUMN receipt SET OPTIONS (description = \'Receipt details\'),'
        'ALTER COLUMN originated_from_transaction_hash SET OPTIONS (description = \'The transaction hash that originated the receipt\'),'
        'ALTER COLUMN transaction_signer_account_id SET OPTIONS (description = \'An account on which behalf the origin transaction is signed\'),'
        'ALTER COLUMN transaction_signer_public_key SET OPTIONS (description = \'An access key which was used to sign the origin transaction\'),'
        'ALTER COLUMN execution_outcome_executed_in_block_hash SET OPTIONS (description = \'The execution outcome Block hash\'),'
        'ALTER COLUMN execution_outcome_receipt_ids SET OPTIONS (description = \'The execution outcome Receipt IDs generated by the transaction or receipt\'),'
        'ALTER COLUMN execution_outcome_gas_burnt SET OPTIONS (description = \'The execution outcome amount of the gas burnt by the given transaction or receipt\'),'
        'ALTER COLUMN execution_outcome_tokens_burnt SET OPTIONS (description = \'The execution outcome amount of tokens burnt corresponding to the burnt gas amount. This value does not always equal to the `gas_burnt` multiplied by the gas price, because the prepaid gas price might be lower than the actual gas price and it creates a deficit\'),'
        'ALTER COLUMN execution_outcome_executor_account_id SET OPTIONS (description = \'The execution outcome id of the account on which the execution happens. For transaction this is signer_id, for receipt this is receiver_id\'),'
        'ALTER COLUMN execution_outcome_status SET OPTIONS (description = \'The execution outcome status. Contains the result in case of successful execution\')'
    )
query_job = bq_client.query(sql)  
print(query_job.result())

# COMMAND ----------

# near_social
dataset_ref = bigquery.DatasetReference(project_id, "crypto_near_mainnet")
table_ref = dataset_ref.table('near_social_transactions')
table = bq_client.get_table(table_ref)

table.description = "NEAR Social transactions for posts, comments, likes, widgets, profiles, followers, etc."
bq_client.update_table(table, ["description"])

sql = (
        f'ALTER TABLE `{project_id}.crypto_near_mainnet.near_social_transactions` '
        'ALTER COLUMN block_date SET OPTIONS (description = \'The date of the Block. Used to partition the table\'), '
        'ALTER COLUMN block_height SET OPTIONS (description = \'The height of the Block\'),'
        'ALTER COLUMN block_timestamp_utc SET OPTIONS (description = \'The timestamp of the Block in UTC\'),'
        'ALTER COLUMN signer_id SET OPTIONS (description = \'An account on which behalf the origin transaction is signed\'),'
        'ALTER COLUMN true_signer_id SET OPTIONS (description = \'An account on which behalf the origin transaction is signed in case of action being delegated to relayer\'),'
        'ALTER COLUMN predecessor_id SET OPTIONS (description = \'The account ID which issued a receipt. In case of a gas or deposit refund, the account ID is system\'),'
        'ALTER COLUMN receipt_id SET OPTIONS (description = \'An unique id for the receipt\'),'
        'ALTER COLUMN contract_id SET OPTIONS (description = \'The contract ID\'),'
        'ALTER COLUMN method_name SET OPTIONS (description = \'The method name\'),'
        'ALTER COLUMN deposit SET OPTIONS (description = \'The deposit amount\'),'
        'ALTER COLUMN gas SET OPTIONS (description = \'The gas fee\'),'
        'ALTER COLUMN account_object SET OPTIONS (description = \'The account object\'),'
        'ALTER COLUMN widget SET OPTIONS (description = \'The account object edit widget\'),'
        'ALTER COLUMN post SET OPTIONS (description = \'The account object post or comment\'),'
        'ALTER COLUMN profile SET OPTIONS (description = \'The account object edit profile\'),'
        'ALTER COLUMN graph SET OPTIONS (description = \'The account object graph follow or hide\'),'
        'ALTER COLUMN settings SET OPTIONS (description = \'The account object settings\'),'
        'ALTER COLUMN badge SET OPTIONS (description = \'The account object badge\'),'
        'ALTER COLUMN index SET OPTIONS (description = \'The account object index like, follow, poke, comment, post, notify\')'
    )
query_job = bq_client.query(sql)  
query_job.result()

# COMMAND ----------

# accounts view

sql = (f"""
    CREATE OR REPLACE VIEW `{project_id}.crypto_near_mainnet.accounts`
    AS 
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
        receipt_receiver_account_id AS account_id,
        action_kind,
        NOT (action_kind = 'DELETE_ACCOUNT') AS is_active
    FROM (
        SELECT *, 
        row_number() OVER (PARTITION BY receipt_receiver_account_id ORDER BY block_timestamp_utc DESC) AS rn
        FROM `{project_id}.crypto_near_mainnet.receipt_actions`
        WHERE action_kind IN ('CREATE_ACCOUNT', 'DELETE_ACCOUNT', 'TRANSFER')
    )
    WHERE rn = 1
    """)
query_job = bq_client.query(sql)  
print(query_job.result())

dataset_ref = bigquery.DatasetReference(project_id, "crypto_near_mainnet")
table_ref = dataset_ref.table('accounts')
table = bq_client.get_table(table_ref)

table.description = "It's recomended to select only the columns and partitions (block_date) needed to avoid unecessary query costs. This view filters the receipt_actions table to have the latest account state based on action_kind CREATE_ACCOUNT, TRANSFER, and DELETE_ACCOUNT."
bq_client.update_table(table, ["description"])

sql = (
        f'ALTER VIEW `{project_id}.crypto_near_mainnet.accounts` '
        'ALTER COLUMN block_date SET OPTIONS (description = \'The date of the Block. Used to partition the table\'), '
        'ALTER COLUMN block_height SET OPTIONS (description = \'The height of the Block\'),'
        'ALTER COLUMN block_timestamp SET OPTIONS (description = \'The timestamp of the Block in nanoseconds\'),'
        'ALTER COLUMN block_timestamp_utc SET OPTIONS (description = \'The timestamp of the Block in UTC\'),'
        'ALTER COLUMN block_hash SET OPTIONS (description = \'The hash of the Block\'),'
        'ALTER COLUMN chunk_hash SET OPTIONS (description = \'The hash of the Chunk\'),'
        'ALTER COLUMN shard_id SET OPTIONS (description = \'The shard ID of the Chunk\'),'
        'ALTER COLUMN index_in_action_receipt SET OPTIONS (description = \'The index in the ACTION receipt\'),'
        'ALTER COLUMN receipt_id SET OPTIONS (description = \'An unique id for the receipt\'),'
        'ALTER COLUMN action_kind SET OPTIONS (description = \'The action kind: CREATE_ACCOUNT, DELETE_ACCOUNT, TRANSFER\'),'
        'ALTER COLUMN is_active SET OPTIONS (description = \'Active Flag\'),'
        'ALTER COLUMN account_id SET OPTIONS (description = \'The account ID\')'
    )
query_job = bq_client.query(sql)  
print(query_job.result())

# COMMAND ----------

# ft_events
dataset_ref = bigquery.DatasetReference(project_id, "crypto_near_mainnet")
table_ref = dataset_ref.table('ft_events')
table = bq_client.get_table(table_ref)

table.description = "NEAR Fungible Tokens event logs."
bq_client.update_table(table, ["description"])

sql = (
        f'ALTER TABLE `{project_id}.crypto_near_mainnet.ft_events` '
        'ALTER COLUMN block_date SET OPTIONS (description = \'The date of the Block. Used to partition the table\'), '
        'ALTER COLUMN block_height SET OPTIONS (description = \'The height of the Block\'),'
        'ALTER COLUMN block_timestamp SET OPTIONS (description = \'The timestamp of the Block\'),'
        'ALTER COLUMN block_timestamp_utc SET OPTIONS (description = \'The timestamp of the Block in UTC\'),'
        'ALTER COLUMN block_hash SET OPTIONS (description = \'The hash of the Block\'),'
        'ALTER COLUMN chunk_hash SET OPTIONS (description = \'The hash of the Chunk\'),'
        'ALTER COLUMN shard_id SET OPTIONS (description = \'The shard ID of the Chunk\'),'
        'ALTER COLUMN standard SET OPTIONS (description = \'The fungible tokens standard. e.g. nep141. \'),'
        'ALTER COLUMN receipt_id SET OPTIONS (description = \'An unique id for the receipt\'),'
        'ALTER COLUMN contract_account_id SET OPTIONS (description = \'The contract account ID\'),'
        'ALTER COLUMN affected_account_id SET OPTIONS (description = \'Account ID affected by the change\'),'
        'ALTER COLUMN involved_account_id SET OPTIONS (description = \'Account ID involved\'),'
        'ALTER COLUMN delta_amount SET OPTIONS (description = \'The delta amount\'),'
        'ALTER COLUMN cause SET OPTIONS (description = \'The cause of the change\'),'
        'ALTER COLUMN status SET OPTIONS (description = \'Execution status. Contains the result in case of successful execution\'),'
        'ALTER COLUMN event_memo SET OPTIONS (description = \'The event log memo\'),'
        'ALTER COLUMN event_index SET OPTIONS (description = \'The event log index\'),'
        'ALTER COLUMN token_id SET OPTIONS (description = \'The token ID\')'
    )
query_job = bq_client.query(sql)  
query_job.result()

# COMMAND ----------

# ft_events_contracts_by_account
sql = (
        'CREATE MATERIALIZED VIEW pagoda-data-platform.crypto_near_mainnet.ft_events_contracts_by_account '
        'OPTIONS (enable_refresh = true, refresh_interval_minutes = 60) AS '
        'SELECT DISTINCT affected_account_id, contract_account_id '
        'FROM pagoda-data-platform.crypto_near_mainnet.ft_events'
    )
query_job = bq_client.query(sql)  
query_job.result()

dataset_ref = bigquery.DatasetReference(project_id, "crypto_near_mainnet")
table_ref = dataset_ref.table('ft_events_contracts_by_account')
table = bq_client.get_table(table_ref)

table.description = "NEAR Fungible Tokens event logs contracts by account"
bq_client.update_table(table, ["description"])

# COMMAND ----------

# nft_events
dataset_ref = bigquery.DatasetReference(project_id, "crypto_near_mainnet")
table_ref = dataset_ref.table('nft_events')
table = bq_client.get_table(table_ref)

table.description = "NEAR Non Fungible Tokens event logs."
bq_client.update_table(table, ["description"])

sql = (
        f'ALTER TABLE `{project_id}.crypto_near_mainnet.nft_events` '
        'ALTER COLUMN block_date SET OPTIONS (description = \'The date of the Block. Used to partition the table\'), '
        'ALTER COLUMN block_height SET OPTIONS (description = \'The height of the Block\'),'
        'ALTER COLUMN block_timestamp SET OPTIONS (description = \'The timestamp of the Block\'),'
        'ALTER COLUMN block_timestamp_utc SET OPTIONS (description = \'The timestamp of the Block in UTC\'),'
        'ALTER COLUMN block_hash SET OPTIONS (description = \'The hash of the Block\'),'
        'ALTER COLUMN chunk_hash SET OPTIONS (description = \'The hash of the Chunk\'),'
        'ALTER COLUMN shard_id SET OPTIONS (description = \'The shard ID of the Chunk\'),'
        'ALTER COLUMN standard SET OPTIONS (description = \'The fungible tokens standard. e.g. nep141. \'),'
        'ALTER COLUMN receipt_id SET OPTIONS (description = \'An unique id for the receipt\'),'
        'ALTER COLUMN contract_account_id SET OPTIONS (description = \'The contract account ID\'),'
        'ALTER COLUMN old_owner_account_id SET OPTIONS (description = \'Old Owner Account ID\'),'
        'ALTER COLUMN new_owner_account_id SET OPTIONS (description = \'New Owner Account ID\'),'
        'ALTER COLUMN authorized_account_id SET OPTIONS (description = \'Authorized Account ID\'),'
        'ALTER COLUMN cause SET OPTIONS (description = \'The cause of the change\'),'
        'ALTER COLUMN status SET OPTIONS (description = \'Execution status. Contains the result in case of successful execution\'),'
        'ALTER COLUMN event_memo SET OPTIONS (description = \'The event log memo\'),'
        'ALTER COLUMN event_index SET OPTIONS (description = \'The event log index\'),'
        'ALTER COLUMN token_id SET OPTIONS (description = \'The token ID\')'
    )
query_job = bq_client.query(sql)  
query_job.result()

# COMMAND ----------

# nft_events_contracts_by_account
sql = (
        'CREATE MATERIALIZED VIEW pagoda-data-platform.crypto_near_mainnet.nft_events_contracts_by_account '
        'OPTIONS (enable_refresh = true, refresh_interval_minutes = 60) AS '
        'SELECT DISTINCT old_owner_account_id, new_owner_account_id, contract_account_id '
        'FROM pagoda-data-platform.crypto_near_mainnet.nft_events'
    )
query_job = bq_client.query(sql)  
query_job.result()

dataset_ref = bigquery.DatasetReference(project_id, "crypto_near_mainnet")
table_ref = dataset_ref.table('nft_events_contracts_by_account')
table = bq_client.get_table(table_ref)

table.description = "NEAR Non Fungible Tokens event logs contracts by account"
bq_client.update_table(table, ["description"])

# COMMAND ----------

# ft_balances_daily
dataset_ref = bigquery.DatasetReference(project_id, "crypto_near_mainnet")
table_ref = dataset_ref.table('ft_balances_daily')
table = bq_client.get_table(table_ref)

table.description = "Fungible Tokens daily balances"
bq_client.update_table(table, ["description"])

sql = (
        f'ALTER TABLE `{project_id}.crypto_near_mainnet.ft_balances_daily` '
        'ALTER COLUMN epoch_date SET OPTIONS (description = \'The date of the Epoch. Used to partition the table\'), '
        'ALTER COLUMN epoch_block_height SET OPTIONS (description = \'The max height of the Block in the epoch\'),'
        'ALTER COLUMN account_id SET OPTIONS (description = \'The delegator Account ID\'),'
        'ALTER COLUMN liquid SET OPTIONS (description = \'Liquid balance\'),'
        'ALTER COLUMN storage_usage SET OPTIONS (description = \'Storage Usage\'),'
        'ALTER COLUMN unstaked_not_liquid SET OPTIONS (description = \'Unstaked balance\'),'
        'ALTER COLUMN staked SET OPTIONS (description = \'Staked balance\'),'
        'ALTER COLUMN reward SET OPTIONS (description = \'Reward\'),'
        'ALTER COLUMN lockup_account_id SET OPTIONS (description = \'The lockup delegator Account ID\'),'
        'ALTER COLUMN lockup_liquid SET OPTIONS (description = \'Lockup liquid balance\'),'
        'ALTER COLUMN lockup_unstaked_not_liquid SET OPTIONS (description = \'Lockup unstaked balance\'),'
        'ALTER COLUMN lockup_staked SET OPTIONS (description = \'Lockup staked balance\'),'
        'ALTER COLUMN lockup_reward SET OPTIONS (description = \'Lockup Reward\')'
    )
query_job = bq_client.query(sql)  
query_job.result()

# COMMAND ----------

# c
dataset_ref = bigquery.DatasetReference(project_id, "crypto_near_mainnet")
table_ref = dataset_ref.table('circulating_supply')
table = bq_client.get_table(table_ref)

table.description = "Circulating supply"
bq_client.update_table(table, ["description"])

sql = (
        f'ALTER TABLE `{project_id}.crypto_near_mainnet.circulating_supply` '
        'ALTER COLUMN block_date SET OPTIONS (description = \'Block Date\'), '
        'ALTER COLUMN computed_at_block_height SET OPTIONS (description = \'Computed at the height of the Block\'),'
        'ALTER COLUMN computed_at_block_timestamp SET OPTIONS (description = \'Computed at the timestamp of the Block\'),'
        'ALTER COLUMN computed_at_block_hash SET OPTIONS (description = \'Computed at the hash of the Block\'),'
        'ALTER COLUMN circulating_tokens_supply SET OPTIONS (description = \'Circulating supply\'),'
        'ALTER COLUMN total_tokens_supply SET OPTIONS (description = \'Total supply\')'
    )
query_job = bq_client.query(sql)  
query_job.result()

# COMMAND ----------

# Transactions
dataset_ref = bigquery.DatasetReference(project_id, "crypto_near_mainnet")
table_ref = dataset_ref.table('transaction_actions')
table = bq_client.get_table(table_ref)

table.description = "Each transaction can have one or multiple Actions, which are the actual operations to be performed on the Receiver account. Action kind can be: ADD_KEY, CREATE_ACCOUNT, DELEGATE_ACTION, DELETE_ACCOUNT, DELETE_KEY, DEPLOY_CONTRACT, FUNCTION_CALL, STAKE, TRANSFER"
bq_client.update_table(table, ["description"])

sql = (
        f'ALTER TABLE `{project_id}.crypto_near_mainnet.transaction_actions` '
        'ALTER COLUMN block_date SET OPTIONS (description = \'The date of the Block. Used to partition the table\'), '
        'ALTER COLUMN block_height SET OPTIONS (description = \'The height of the Block\'),'
        'ALTER COLUMN block_timestamp SET OPTIONS (description = \'The timestamp of the Block in nanoseconds\'),'
        'ALTER COLUMN block_timestamp_utc SET OPTIONS (description = \'The timestamp of the Block in UTC\'),'
        'ALTER COLUMN transaction_hash SET OPTIONS (description = \'The transaction hash\'),'
        'ALTER COLUMN transaction_status SET OPTIONS (description = \'Transaction status\'),'
        'ALTER COLUMN converted_into_receipt_id SET OPTIONS (description = \'Receipt ID that the transaction was converted.\'),'
        'ALTER COLUMN signer_account_id SET OPTIONS (description = \'An account on which behalf transaction is signed\'),'
        'ALTER COLUMN signer_public_key SET OPTIONS (description = \'An access key which was used to sign a transaction\'),'
        'ALTER COLUMN receiver_account_id SET OPTIONS (description = \'Receiver account for this transaction\'),'
        'ALTER COLUMN index_in_transaction SET OPTIONS (description = \'The index in the transaction actions\'),'
        'ALTER COLUMN action_kind SET OPTIONS (description = \'The action kind: ADD_KEY, CREATE_ACCOUNT	, DELEGATE_ACTION, DELETE_ACCOUNT, DELETE_KEY, DEPLOY_CONTRACT, FUNCTION_CALL, STAKE, TRANSFER\'),'
        'ALTER COLUMN args SET OPTIONS (description = \'Arguments\')'
    )
query_job = bq_client.query(sql)  
query_job.result()

# COMMAND ----------


