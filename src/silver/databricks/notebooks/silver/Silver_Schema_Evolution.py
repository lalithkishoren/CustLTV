# Databricks notebook source
# =================================================================================
# SILVER LAYER SCHEMA EVOLUTION NOTEBOOK
# =================================================================================
# (auth handled by Unity Catalog — see note above)
# Detects schema drift between Bronze and Silver. Schema evolution is INTENTIONAL.
# =================================================================================

import json

dbutils.widgets.text("source_system", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("source_bronze_table", "")
dbutils.widgets.text("target_silver_table", "")
dbutils.widgets.text("storage_account", "")

source_system = dbutils.widgets.get("source_system")
schema_name = dbutils.widgets.get("schema_name")
source_bronze_table = dbutils.widgets.get("source_bronze_table")
target_silver_table = dbutils.widgets.get("target_silver_table")
storage_account = dbutils.widgets.get("storage_account")

bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/{source_system}/{schema_name}/{source_bronze_table}/"
silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/{source_system}/{schema_name}/{target_silver_table}/"

try:
    df_bronze = spark.read.format("delta").load(bronze_path)
    df_silver = spark.read.format("delta").load(silver_path)
    
    bronze_cols = set([f.name.lower() for f in df_bronze.schema.fields])
    silver_cols = set([f.name.lower() for f in df_silver.schema.fields])
    
    # Ignore metadata columns
    metadata_cols = {"_pipeline_run_id", "_load_timestamp", "_is_deleted", "_valid_from", "_valid_to", "_is_current", "_hash_key"}
    silver_cols = silver_cols - metadata_cols
    
    new_columns = bronze_cols - silver_cols
    missing_columns = silver_cols - bronze_cols
    
    if new_columns:
        print(f"WARNING: New columns detected in Bronze not present in Silver: {new_columns}")
        print("Schema evolution is intentional. Please review and update Unity Catalog DDL explicitly.")
        
    if missing_columns:
        print(f"WARNING: Columns missing in Bronze that are expected in Silver: {missing_columns}")
        
    dbutils.notebook.exit(json.dumps({
        "new_columns": list(new_columns),
        "missing_columns": list(missing_columns)
    }))
    
except Exception as e:
    print(f"Schema check skipped or failed: {str(e)}")
    dbutils.notebook.exit(json.dumps({"status": "skipped"}))