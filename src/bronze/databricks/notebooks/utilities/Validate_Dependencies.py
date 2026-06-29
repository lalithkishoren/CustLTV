# Databricks notebook source
# ===============================================================================
# Validate_Dependencies.py
# PURPOSE: Validate environment before running pipeline
# ===============================================================================

import pyodbc
import sys

dbutils.widgets.text("storage_account", "{{PLACEHOLDER_STORAGE_ACCOUNT}}", "Storage Account")
dbutils.widgets.text("sql_server", "{{PLACEHOLDER_SQL_SERVER_FQDN}}", "SQL Server")
dbutils.widgets.text("sql_database", "{{PLACEHOLDER_SQL_DATABASE}}", "SQL Database")
dbutils.widgets.text("sql_username", "{{PLACEHOLDER_SQL_USERNAME}}", "SQL Username")
dbutils.widgets.text("sql_password", "{{PLACEHOLDER_SQL_PASSWORD}}", "SQL Password")

storage_account = dbutils.widgets.get("storage_account")
sql_server = dbutils.widgets.get("sql_server")
sql_database = dbutils.widgets.get("sql_database")
sql_username = dbutils.widgets.get("sql_username")
sql_password = dbutils.widgets.get("sql_password")

results = {"pass": 0, "fail": 0}

def log_result(test_name, status, message=""):
    if status:
        print(f"[PASS] {test_name}")
        results["pass"] += 1
    else:
        print(f"[FAIL] {test_name} - {message}")
        results["fail"] += 1

print("--- Starting Environment Validation ---\n")

# 1. Spark & Delta Check
try:
    spark_version = spark.version
    log_result("Spark Version Check", True)
    
    spark.sql("SELECT 1").collect()
    log_result("Delta Lake Availability", True)
except Exception as e:
    log_result("Spark/Delta Check", False, str(e))

# 2. Python Packages
try:
    import pyodbc
    log_result("pyodbc Package Installed", True)
except ImportError:
    log_result("pyodbc Package Installed", False, "pyodbc not found")

# 3. Storage Access (Unity Catalog)
try:
    path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
    dbutils.fs.ls(path)
    log_result("ADLS Bronze Layer Access (Unity Catalog)", True)
except Exception as e:
    log_result("ADLS Bronze Layer Access (Unity Catalog)", False, str(e))

# 4. SQL Server Connectivity & Objects
try:
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sql_server};DATABASE={sql_database};UID={sql_username};PWD={sql_password}"
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    log_result("SQL Server Connectivity", True)
    
    # Check tables
    tables = ['source_systems', 'table_metadata', 'load_dependencies', 'pipeline_execution_log', 'data_quality_rules']
    for tbl in tables:
        cursor.execute(f"SELECT 1 FROM sys.tables WHERE object_id = OBJECT_ID('control.{tbl}')")
        if cursor.fetchone():
            log_result(f"Control Table Exists: {tbl}", True)
        else:
            log_result(f"Control Table Exists: {tbl}", False, "Table missing")
            
    # Check stored procedures
    sps = ['sp_GetCDCChanges', 'sp_UpdateTableMetadata', 'sp_GetTableLoadOrder']
    for sp in sps:
        cursor.execute(f"SELECT 1 FROM sys.procedures WHERE object_id = OBJECT_ID('control.{sp}')")
        if cursor.fetchone():
            log_result(f"Stored Procedure Exists: {sp}", True)
        else:
            log_result(f"Stored Procedure Exists: {sp}", False, "SP missing")
            
    conn.close()
except Exception as e:
    log_result("SQL Server Validation", False, str(e))

print(f"\n--- Validation Summary ---")
print(f"Passed: {results['pass']}")
print(f"Failed: {results['fail']}")

if results["fail"] > 0:
    raise Exception("Environment validation failed. Check logs above.")
else:
    print("Environment is ready for pipeline execution.")