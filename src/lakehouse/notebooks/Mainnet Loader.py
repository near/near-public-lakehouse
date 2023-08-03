# Databricks notebook source
access_key = dbutils.secrets.get(scope = "aws", key = "access-key")
secret_key = dbutils.secrets.get(scope = "aws", key = "secret-access-key")
#sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
spark.conf.set("fs.s3a.access.key", access_key)
spark.conf.set("fs.s3a.secret.key", secret_key)
spark.conf.set("fs.s3a.requester-pays.enabled", "true")
spark.conf.set("fs.s3a.experimental.input.fadvise", "random")

# when using Auto Loader file notification mode to load files, provide the AWS Region ID.
aws_region = "eu-central-1"
spark.conf.set("fs.s3a.endpoint", "s3." + aws_region + ".amazonaws.com")

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(name="blocks", comment="NEAR Lake Mainnet Raw Blocks")
def mainnet_blocks():
    return (spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", '/nearlake/landingZone/mainnet_blocks/_checkpoint')
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.useIncrementalListing", "true") 
      .option("pathGlobFilter", "*block.json") 
      .load("s3a://near-lake-data-mainnet/")
#      .withColumn("date_created", current_timestamp())
           )
    
@dlt.table(name="chunks", comment="NEAR Lake Mainnet Raw Chunks")
def mainnet_chunks():
    return (spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", '/nearlake/landingZone/mainnet_chunks/_checkpoint')
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.useIncrementalListing", "true") 
      .option("pathGlobFilter", "*shard*.json") 
      .load("s3a://near-lake-data-mainnet/")
#      .withColumn("date_created", current_timestamp())
           )
