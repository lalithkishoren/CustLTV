# Databricks notebook source
# =================================================================================
# SILVER LAYER DATA QUALITY PROFILING
# =================================================================================
# CRITICAL AUTH POLICY: Authentication is handled entirely by Unity Catalog via 
# Managed Identity (Access Connector). NO fs.azure.* keys or secrets are set here.
# =================================================================================

import json
from pyspark.sql.functions import col, count, sum, when, isnull
from delta.tables import DeltaTable

dbutils.widgets.text("target_silver_path", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("storage_account", "")

target_silver_path = dbutils.widgets.get("target_silver_path")
table_name = dbutils.widgets.get("table_name")
storage_account = dbutils.widgets.get("storage_account")

silver_uri = f"abfss://silver@{storage_account}.dfs.core.windows.net/{target_silver_path.replace('silver/', '', 1)}"

print(f"Running DQ Profiling on: {silver_uri}")

if not DeltaTable.isDeltaTable(spark, silver_uri):
    dbutils.notebook.exit(json.dumps({"status": "skipped", "reason": "Table does not exist yet"}))

df = spark.read.format("delta").load(silver_uri)
total_records = df.count()

# Profile Nulls
null_counts = df.select([sum(when(isnull(c), 1).otherwise(0)).alias(c) for c in df.columns]).collect()[0].asDict()

# Profile Warnings (from _dq_warnings array)
warnings_count = 0
if "_dq_warnings" in df.columns:
    warnings_count = df.filter("size(_dq_warnings) > 0").count()

# Profile Deletes
deleted_count = 0
if "_is_deleted" in df.columns:
    deleted_count = df.filter(col("_is_deleted") == True).count()

dq_summary = {
    "table_name": table_name,
    "total_records": total_records,
    "records_with_warnings": warnings_count,
    "soft_deleted_records": deleted_count,
    "null_profiling": null_counts
}

print(json.dumps(dq_summary, indent=2))
dbutils.notebook.exit(json.dumps(dq_summary))