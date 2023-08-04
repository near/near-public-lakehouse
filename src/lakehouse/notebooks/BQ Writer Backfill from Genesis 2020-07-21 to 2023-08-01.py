# Databricks notebook source
# MAGIC %md
# MAGIC # BQ Writer Backfill from Genesis 2020-07-21 to 2023-08-01

# COMMAND ----------

def bq_writer_backfill(table_name, start_date, end_date):
    df = spark.sql(f"SELECT * FROM hive_metastore.mainnet.silver_{table_name} WHERE block_date >= '{start_date}' AND  block_date <= '{end_date}'")

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

bq_writer_backfill("blocks", "2020-01-01", "2023-08-01")

# COMMAND ----------

bq_writer_backfill("transactions", "2020-01-01", "2023-08-01")

# COMMAND ----------

bq_writer_backfill("receipts", "2020-01-01", "2023-08-01")

# COMMAND ----------

bq_writer_backfill("account_changes", "2020-01-01", "2023-08-01")

# COMMAND ----------

bq_writer_backfill("chunks", "2020-01-01", "2020-12-31")
bq_writer_backfill("chunks", "2021-01-01", "2021-12-31")
bq_writer_backfill("chunks", "2022-01-01", "2022-06-30")
bq_writer_backfill("chunks", "2022-07-01", "2022-12-31")
bq_writer_backfill("chunks", "2023-07-01", "2023-12-31")

# COMMAND ----------


