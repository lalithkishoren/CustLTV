# Databricks notebook source
import json
from pyspark.sql.functions import col, expr, current_timestamp, lit

# COMMAND ----------
# 1. WIDGET DEFINITIONS
# COMMAND ----------
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("target_silver_table", "")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("storage_access_key", "")

pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
target_silver_table = dbutils.widgets.get("target_silver_table")
storage_account = dbutils.widgets.get("storage_account")
storage_access_key = dbutils.widgets.get("storage_access_key")

# COMMAND ----------
# 2. STORAGE AUTHENTICATION
# COMMAND ----------
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_access_key
)

silver_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/silver/{target_silver_table}/"

# COMMAND ----------
# 3. READ SILVER DATA
# COMMAND ----------
df_silver = spark.read.format("delta").load(silver_path)
total_records = df_silver.count()

# COMMAND ----------
# 4. APPLY POST-LOAD DQ CHECKS (Format & Referential Integrity Warnings)
# COMMAND ----------
# These are checks that do not drop rows but flag them for observability (Severity: WARNING)
dq_results = []

if target_silver_table == 'customers':
    # DQ-F-001: Email Format Check
    invalid_emails = df_silver.filter(col("email").isNotNull() & ~col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")).count()
    dq_results.append({"rule_id": "DQ-F-001", "failed_count": invalid_emails, "severity": "WARNING"})

elif target_silver_table == 'oe_order_headers_all':
    # DQ-RI-001: Referential Integrity (Orders -> Customers)
    customers_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/silver/customers/"
    df_customers = spark.read.format("delta").load(customers_path).select("customer_id").distinct()
    
    orphaned_orders = df_silver.join(df_customers, "customer_id", "left_anti").count()
    dq_results.append({"rule_id": "DQ-RI-001", "failed_count": orphaned_orders, "severity": "WARNING"})

# COMMAND ----------
# 5. RETURN DQ SUMMARY
# COMMAND ----------
result = {
    "table": target_silver_table,
    "total_records_checked": total_records,
    "dq_warnings": dq_results
}
dbutils.notebook.exit(json.dumps(result))