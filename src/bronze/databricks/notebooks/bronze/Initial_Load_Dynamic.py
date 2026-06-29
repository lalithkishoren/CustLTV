# ===============================================================================
# Initial Load Processing (Parquet Staging -> Delta Bronze)
# ===============================================================================

from pyspark.sql.functions import current_timestamp, lit
import json

# 1. Define Widgets
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("table_id", "")
dbutils.widgets.text("source_system", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("primary_key_columns", "")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("sql_server", "")
dbutils.widgets.text("sql_database", "")
dbutils.widgets.text("sql_username", "")
dbutils.widgets.text("sql_password", "")

# 2. Get Widget Values
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
table_id = dbutils.widgets.get("table_id")
source_system = dbutils.widgets.get("source_system")
schema_name = dbutils.widgets.get("schema_name")
table_name = dbutils.widgets.get("table_name")
primary_key_columns = dbutils.widgets.get("primary_key_columns")
storage_account = dbutils.widgets.get("storage_account")
sql_server = dbutils.widgets.get("sql_server")
sql_database = dbutils.widgets.get("sql_database")
sql_username = dbutils.widgets.get("sql_username")
sql_password = dbutils.widgets.get("sql_password")

# ADLS auth: handled by Unity Catalog (NO account key, NO fs.azure.* configs)

# 3. Define Paths
staging_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/staging/{source_system}/{schema_name}/{table_name}/initial/{pipeline_run_id}/"
delta_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/{source_system}/{schema_name}/{table_name}/"

print(f"Reading from staging: {staging_path}")
print(f"Writing to Delta: {delta_path}")

# 4. Read Staging Parquet
df = spark.read.format("parquet").load(staging_path)
records_processed = df.count()
print(f"Records read from staging: {records_processed}")

# 5. Add Audit/Metadata Columns
df_with_metadata = df \
    .withColumn("_pipeline_run_id", lit(pipeline_run_id)) \
    .withColumn("_ingest_timestamp", current_timestamp()) \
    .withColumn("_source_system", lit(source_system)) \
    .withColumn("_cdc_operation", lit("I"))

# 6. Write to Delta Table (Overwrite for Initial Load)
df_with_metadata.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save(delta_path)

print(f"Successfully wrote {records_processed} records to Delta table.")

# 7. Update SQL Control Table via JDBC
jdbc_url = f"jdbc:sqlserver://{sql_server}:1433;database={sql_database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
connection_properties = {
    "user": sql_username,
    "password": sql_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Get current sync version from source (if available in staging data, else 0)
# For initial load, we assume the source system's current version is captured or we start at 0
current_sync_version = "0"
if "_current_sync_version" in df.columns:
    max_version_row = df.agg({"_current_sync_version": "max"}).collect()[0]
    if max_version_row[0] is not None:
        current_sync_version = str(max_version_row[0])

update_query = f"""
    EXEC control.sp_UpdateTableMetadata 
        @TableId = {table_id}, 
        @Status = 'SUCCESS', 
        @PipelineRunId = '{pipeline_run_id}', 
        @RecordsLoaded = {records_processed}, 
        @SyncVersion = '{current_sync_version}', 
        @MarkInitialLoadComplete = 1
"""

# Execute JDBC update
# Note: PySpark JDBC write is for DataFrames. For executing a stored procedure, we use pyodbc or a dummy dataframe write.
# Since pyodbc might not be installed by default, we use the Spark JDBC pushdown trick:
spark.read.jdbc(url=jdbc_url, table=f"({update_query}) AS tmp", properties=connection_properties)

print("Control table updated successfully. initial_load_completed set to 1.")

# 8. Clean up staging files
dbutils.fs.rm(staging_path, recurse=True)
print(f"Cleaned up staging path: {staging_path}")

# 9. Return Status
result = {
    "status": "SUCCESS",
    "records_processed": records_processed,
    "sync_version": current_sync_version
}
dbutils.notebook.exit(json.dumps(result))