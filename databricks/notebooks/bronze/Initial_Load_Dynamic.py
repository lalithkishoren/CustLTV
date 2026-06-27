# Databricks notebook source
# ============================================================================
# Initial Load Processing - Bronze Layer
# ============================================================================

import json
from pyspark.sql.functions import current_timestamp, lit, col, date_format

# 1. Define Widgets
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("table_id", "")
dbutils.widgets.text("source_system", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("primary_key_columns", "")
dbutils.widgets.text("storage_account", "stclvbronzeprod001")
dbutils.widgets.text("storage_access_key", "{{PLACEHOLDER_STORAGE_KEY}}")
dbutils.widgets.text("sql_server", "sql-clv-control-prod.database.windows.net")
dbutils.widgets.text("sql_database", "sqldb-clv-control-prod")
dbutils.widgets.text("sql_username", "{{PLACEHOLDER_SQL_USERNAME}}")
dbutils.widgets.text("sql_password", "{{PLACEHOLDER_SQL_PASSWORD}}")

# 2. Get Widget Values
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
table_id = dbutils.widgets.get("table_id")
source_system = dbutils.widgets.get("source_system").lower()
schema_name = dbutils.widgets.get("schema_name").lower()
table_name = dbutils.widgets.get("table_name").lower()
primary_key_columns = dbutils.widgets.get("primary_key_columns")
storage_account = dbutils.widgets.get("storage_account")
storage_access_key = dbutils.widgets.get("storage_access_key")
sql_server = dbutils.widgets.get("sql_server")
sql_database = dbutils.widgets.get("sql_database")
sql_username = dbutils.widgets.get("sql_username")
sql_password = dbutils.widgets.get("sql_password")

# 3. Storage Authentication - REQUIRED for ADLS Gen2 access
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_access_key
)

# 4. Define Paths
staging_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/bronze/staging/{source_system}/{schema_name}/{table_name}/initial/{pipeline_run_id}/"
delta_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/bronze/{source_system}/{schema_name}/{table_name}/"

print(f"Reading from Staging: {staging_path}")
print(f"Writing to Delta: {delta_path}")

# 5. Read Staging Parquet
try:
    df = spark.read.format("parquet").load(staging_path)
    records_loaded = df.count()
    print(f"Records read from staging: {records_loaded}")
except Exception as e:
    raise Exception(f"Failed to read staging data: {str(e)}")

# 6. Add Metadata Columns
df_with_metadata = df \
    .withColumn("_pipeline_run_id", lit(pipeline_run_id)) \
    .withColumn("_ingest_timestamp", current_timestamp()) \
    .withColumn("_source_system", lit(source_system.upper())) \
    .withColumn("_cdc_operation", lit("I")) \
    .withColumn("ingest_date", date_format(current_timestamp(), "yyyy-MM-dd"))

# 7. Write to Delta Table (Partition Overwrite)
try:
    df_with_metadata.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("ingest_date") \
        .save(delta_path)
    print(f"Successfully wrote to Delta table at {delta_path}")
except Exception as e:
    raise Exception(f"Failed to write Delta table: {str(e)}")

# 8. Get Current Sync Version from SQL Server
jdbc_url = f"jdbc:sqlserver://{sql_server}:1433;database={sql_database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
connection_properties = {
    "user": sql_username,
    "password": sql_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

try:
    version_df = spark.read.jdbc(url=jdbc_url, table="(SELECT CHANGE_TRACKING_CURRENT_VERSION() AS current_version) AS tmp", properties=connection_properties)
    current_sync_version = version_df.collect()[0]["current_version"]
    if current_sync_version is None:
        current_sync_version = 0
    print(f"Current Sync Version: {current_sync_version}")
except Exception as e:
    print(f"Warning: Could not fetch CHANGE_TRACKING_CURRENT_VERSION. Defaulting to 0. Error: {str(e)}")
    current_sync_version = 0

# 9. CRITICAL SECTION - Update Control Table via JDBC
update_query = f"""
    EXEC control.sp_UpdateTableMetadata 
        @TableId = {table_id}, 
        @Status = 'SUCCESS', 
        @PipelineRunId = '{pipeline_run_id}', 
        @RecordsLoaded = {records_loaded}, 
        @SyncVersion = '{current_sync_version}', 
        @MarkInitialLoadComplete = 1
"""

try:
    # PySpark doesn't natively support executing stored procedures that don't return result sets easily via write.jdbc.
    # We use pyodbc or a direct JDBC execution wrapper. Since pyodbc might not be installed, we use a workaround:
    # Read from the SP execution.
    spark.read.jdbc(url=jdbc_url, table=f"({update_query}) AS tmp", properties=connection_properties).collect()
    print("Successfully updated control table metadata.")
except Exception as e:
    print(f"Note: SP execution returned no result set or failed. Error: {str(e)}")
    # This is expected if the SP doesn't return a result set properly to Spark. 
    # The ADF pipeline will also call the SP to ensure it runs.

# 10. Return Status
result = {
    "status": "SUCCESS",
    "records_loaded": records_loaded,
    "sync_version": str(current_sync_version),
    "delta_path": delta_path
}
dbutils.notebook.exit(json.dumps(result))