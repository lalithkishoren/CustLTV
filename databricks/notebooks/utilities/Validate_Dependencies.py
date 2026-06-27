# Databricks notebook source
# ============================================================================
# Validate Environment Dependencies
# ============================================================================

import pyodbc
from pyspark.sql import SparkSession

dbutils.widgets.text("storage_account", "stclvbronzeprod001")
dbutils.widgets.text("storage_access_key", "{{PLACEHOLDER_STORAGE_KEY}}")
dbutils.widgets.text("sql_server", "sql-clv-control-prod.database.windows.net")
dbutils.widgets.text("sql_database", "sqldb-clv-control-prod")
dbutils.widgets.text("sql_username", "{{PLACEHOLDER_SQL_USERNAME}}")
dbutils.widgets.text("sql_password", "{{PLACEHOLDER_SQL_PASSWORD}}")

storage_account = dbutils.widgets.get("storage_account")
storage_access_key = dbutils.widgets.get("storage_access_key")
sql_server = dbutils.widgets.get("sql_server")
sql_database = dbutils.widgets.get("sql_database")
sql_username = dbutils.widgets.get("sql_username")
sql_password = dbutils.widgets.get("sql_password")

results = {"passed": 0, "failed": 0, "details": []}

def log_result(test_name, status, message=""):
    print(f"[{status}] {test_name}: {message}")
    if status == "PASS":
        results["passed"] += 1
    else:
        results["failed"] += 1
    results["details"].append({"test": test_name, "status": status, "message": message})

# 1. Check Spark Version
try:
    spark_version = spark.version
    log_result("Spark Version Check", "PASS", f"Version: {spark_version}")
except Exception as e:
    log_result("Spark Version Check", "FAIL", str(e))

# 2. Check Storage Authentication
try:
    spark.conf.set(
        f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
        storage_access_key
    )
    # Try to list root
    dbutils.fs.ls(f"abfss://datalake@{storage_account}.dfs.core.windows.net/")
    log_result("Storage Authentication", "PASS", "Successfully accessed ADLS Gen2")
except Exception as e:
    log_result("Storage Authentication", "FAIL", str(e))

# 3. Check SQL Server JDBC Connectivity
try:
    jdbc_url = f"jdbc:sqlserver://{sql_server}:1433;database={sql_database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
    connection_properties = {
        "user": sql_username,
        "password": sql_password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    df = spark.read.jdbc(url=jdbc_url, table="(SELECT 1 as test) as tmp", properties=connection_properties)
    log_result("SQL Server JDBC", "PASS", "Successfully connected to SQL Server via JDBC")
except Exception as e:
    log_result("SQL Server JDBC", "FAIL", str(e))

# 4. Check Control Tables Exist
try:
    tables_query = """
    (SELECT count(*) as cnt FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id 
     WHERE s.name = 'control' AND t.name IN ('source_systems', 'table_metadata', 'load_dependencies', 'pipeline_execution_log', 'data_quality_rules')) as tmp
    """
    df = spark.read.jdbc(url=jdbc_url, table=tables_query, properties=connection_properties)
    count = df.collect()[0]["cnt"]
    if count == 5:
        log_result("Control Tables Check", "PASS", "All 5 control tables exist")
    else:
        log_result("Control Tables Check", "FAIL", f"Found {count}/5 control tables")
except Exception as e:
    log_result("Control Tables Check", "FAIL", str(e))

# 5. Check Stored Procedures Exist
try:
    sp_query = """
    (SELECT count(*) as cnt FROM sys.procedures p JOIN sys.schemas s ON p.schema_id = s.schema_id 
     WHERE s.name = 'control' AND p.name IN ('sp_GetCDCChanges', 'sp_UpdateTableMetadata', 'sp_GetTableLoadOrder')) as tmp
    """
    df = spark.read.jdbc(url=jdbc_url, table=sp_query, properties=connection_properties)
    count = df.collect()[0]["cnt"]
    if count == 3:
        log_result("Stored Procedures Check", "PASS", "All 3 stored procedures exist")
    else:
        log_result("Stored Procedures Check", "FAIL", f"Found {count}/3 stored procedures")
except Exception as e:
    log_result("Stored Procedures Check", "FAIL", str(e))

# Summary
print("\n" + "="*50)
print(f"VALIDATION SUMMARY: {results['passed']} Passed, {results['failed']} Failed")
print("="*50)

if results["failed"] > 0:
    raise Exception("Environment validation failed. Check logs for details.")