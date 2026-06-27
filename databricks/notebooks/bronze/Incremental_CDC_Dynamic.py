# Databricks notebook source
# ============================================================================
# Incremental CDC Processing - Bronze Layer
# ============================================================================

import json
from pyspark.sql.functions import current_timestamp, lit, col, row_number, date_format
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# 1. Define Widgets
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("table_id", "")
dbutils.widgets.text("source_system", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("primary_key_columns", "")
dbutils.widgets.text("last_sync_version", "")
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
staging_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/bronze/staging/{source_system}/{schema_name}/{table_name}/incremental/{pipeline_run_id}/"
delta_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/bronze/{source_system}/{schema_name}/{table_name}/"

# 5. Read CDC Changes from Staging Parquet
try:
    cdc_df = spark.read.format("parquet").load(staging_path)
    records_read = cdc_df.count()
    print(f"Records read from staging: {records_read}")
    if records_read == 0:
        dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "records_loaded": 0}))
except Exception as e:
    raise Exception(f"Failed to read staging data: {str(e)}")

# Extract new sync version from the data
new_sync_version = cdc_df.select("_current_sync_version").first()[0]

# 6. Deduplicate Source Data (Keep latest change per key)
pk_cols = [pk.strip() for pk in primary_key_columns.split(',')]
window_spec = Window.partitionBy(*pk_cols).orderBy(col("SYS_CHANGE_VERSION").desc())

source_deduped = cdc_df.withColumn("row_num", row_number().over(window_spec)) \
                       .filter(col("row_num") == 1) \
                       .drop("row_num")

# 7. Add Metadata Columns
source_with_metadata = source_deduped \
    .withColumn("_pipeline_run_id", lit(pipeline_run_id)) \
    .withColumn("_ingest_timestamp", current_timestamp()) \
    .withColumn("_source_system", lit(source_system.upper())) \
    .withColumn("_cdc_operation", col("SYS_CHANGE_OPERATION")) \
    .withColumn("ingest_date", date_format(current_timestamp(), "yyyy-MM-dd"))

# 8. Prepare MERGE Condition and Columns
merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in pk_cols])

# Get all columns except CDC specific ones for the update/insert mapping
all_columns = [c for c in source_with_metadata.columns if c not in ["SYS_CHANGE_OPERATION", "SYS_CHANGE_VERSION", "_current_sync_version"]]
update_columns = [c for c in all_columns if c not in pk_cols and c != "ingest_date"]

# 9. Execute MERGE
try:
    target_table = DeltaTable.forPath(spark, delta_path)
    
    target_table.alias("target").merge(
        source_with_metadata.alias("source"),
        merge_condition
    ).whenMatchedDelete(
        condition="source.SYS_CHANGE_OPERATION = 'D'"
    ).whenMatchedUpdate(
        condition="source.SYS_CHANGE_OPERATION IN ('U', 'I')",
        set={column: f"source.{column}" for column in update_columns + ["_pipeline_run_id", "_ingest_timestamp", "_source_system", "_cdc_operation"]}
    ).whenNotMatchedInsert(
        condition="source.SYS_CHANGE_OPERATION != 'D'",
        values={column: f"source.{column}" for column in all_columns}
    ).execute()
    
    print(f"Successfully merged data into Delta table at {delta_path}")
except Exception as e:
    raise Exception(f"Failed to execute MERGE: {str(e)}")

# 10. POST-MERGE Optimization (Z-ORDER)
try:
    spark.sql(f"""
        OPTIMIZE delta.`{delta_path}`
        ZORDER BY ({primary_key_columns}, _ingest_timestamp)
    """)
    print(f"Optimized Delta table: {delta_path}")
except Exception as e:
    print(f"Warning: Optimization failed: {str(e)}")

# 11. Update Control Table via JDBC
jdbc_url = f"jdbc:sqlserver://{sql_server}:1433;database={sql_database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
connection_properties = {
    "user": sql_username,
    "password": sql_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

update_query = f"""
    EXEC control.sp_UpdateTableMetadata 
        @TableId = {table_id}, 
        @Status = 'SUCCESS', 
        @PipelineRunId = '{pipeline_run_id}', 
        @RecordsLoaded = {records_read}, 
        @SyncVersion = '{new_sync_version}', 
        @MarkInitialLoadComplete = 0
"""

try:
    spark.read.jdbc(url=jdbc_url, table=f"({update_query}) AS tmp", properties=connection_properties).collect()
    print("Successfully updated control table metadata.")
except Exception as e:
    print(f"Note: SP execution returned no result set or failed. Error: {str(e)}")

# 12. Return Status
result = {
    "status": "SUCCESS",
    "records_loaded": records_read,
    "sync_version": str(new_sync_version),
    "delta_path": delta_path
}
dbutils.notebook.exit(json.dumps(result))