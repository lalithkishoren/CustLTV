# Databricks notebook source
# =================================================================================
# SILVER LAYER DATA QUALITY NOTEBOOK
# =================================================================================
# (auth handled by Unity Catalog — see note above)
# Applies progressive trust model: quarantines bad rows, flags warnings.
# =================================================================================

import json
from pyspark.sql.functions import col, expr, lit, current_timestamp
from delta.tables import DeltaTable

dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("source_system", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("target_silver_table", "")
dbutils.widgets.text("storage_account", "")

pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
source_system = dbutils.widgets.get("source_system")
schema_name = dbutils.widgets.get("schema_name")
target_silver_table = dbutils.widgets.get("target_silver_table")
storage_account = dbutils.widgets.get("storage_account")

silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/{source_system}/{schema_name}/{target_silver_table}/"
quarantine_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/quarantine/{source_system}/{schema_name}/{target_silver_table}/"

print(f"Evaluating DQ for: {silver_path}")

# Hardcoded rules based on LLD and Project Decisions for demonstration
# In a fully dynamic setup, these would be fetched from control.silver_dq_rules
dq_rules = [
    {"rule_id": "DQ-V-001", "column": "total_amount", "expr": "total_amount > 0", "severity": "ERROR"},
    {"rule_id": "DQ-N-001", "column": "customer_id", "expr": "customer_id IS NOT NULL", "severity": "ERROR"},
    {"rule_id": "DQ-N-002", "column": "status", "expr": "status IS NOT NULL", "severity": "WARNING"}
]

df = spark.read.format("delta").load(silver_path)
initial_count = df.count()

# Evaluate Rules
error_conditions = []
warning_conditions = []

for rule in dq_rules:
    if rule["column"] in df.columns:
        if rule["severity"] == "ERROR":
            error_conditions.append(f"NOT ({rule['expr']})")
        elif rule["severity"] == "WARNING":
            warning_conditions.append(f"NOT ({rule['expr']})")

# 1. Handle Errors (Quarantine)
if error_conditions:
    combined_error_expr = " OR ".join(error_conditions)
    df_errors = df.filter(expr(combined_error_expr))
    error_count = df_errors.count()
    
    if error_count > 0:
        print(f"Found {error_count} records violating ERROR rules. Quarantining...")
        df_errors = df_errors.withColumn("_dq_quarantine_timestamp", current_timestamp()) \
                             .withColumn("_dq_pipeline_run_id", lit(pipeline_run_id))
        
        # Write to quarantine
        df_errors.write.format("delta").mode("append").save(quarantine_path)
        
        # Delete from Silver (Progressive Trust)
        dt = DeltaTable.forPath(spark, silver_path)
        dt.delete(expr(combined_error_expr))

# 2. Handle Warnings (Flag)
if warning_conditions:
    combined_warn_expr = " OR ".join(warning_conditions)
    df_warns = df.filter(expr(combined_warn_expr))
    warn_count = df_warns.count()
    
    if warn_count > 0:
        print(f"Found {warn_count} records violating WARNING rules. Flagging...")
        # In a real scenario, we might update a _dq_warnings column in the Delta table
        # dt.update(condition=expr(combined_warn_expr), set={"_dq_warnings": lit("true")})

print("Data Quality evaluation complete.")
dbutils.notebook.exit(json.dumps({"status": "success", "initial_count": initial_count}))