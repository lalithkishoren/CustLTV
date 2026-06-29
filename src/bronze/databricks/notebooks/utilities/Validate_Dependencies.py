# ===============================================================================
# Validate Environment Dependencies
# ===============================================================================

import sys
import importlib

print("Starting Environment Validation...\n")

# 1. Check Spark Version
spark_version = spark.version
print(f"✅ Spark Version: {spark_version}")

# 2. Check Delta Lake
try:
    from delta.tables import DeltaTable
    print("✅ Delta Lake library is available.")
except ImportError:
    print("❌ FAILED: Delta Lake library is missing.")

# 3. Check pyodbc (Optional but good for direct SQL queries if needed)
try:
    import pyodbc
    print("✅ pyodbc is available.")
except ImportError:
    print("⚠️ WARNING: pyodbc is not installed. (JDBC will be used instead).")

# 4. Check Storage Access (Unity Catalog)
dbutils.widgets.text("storage_account", "stdataplatformdevnda0jg")
storage_account = dbutils.widgets.get("storage_account")

try:
    dbutils.fs.ls(f"abfss://bronze@{storage_account}.dfs.core.windows.net/")
    print("✅ Unity Catalog Storage Access: Verified for Bronze layer.")
except Exception as e:
    print(f"❌ FAILED: Unity Catalog Storage Access failed. Error: {str(e)}")

# 5. Check SQL Server JDBC Driver
try:
    # Attempt to load the driver class via JVM
    spark._jvm.java.lang.Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
    print("✅ SQL Server JDBC Driver is available.")
except Exception as e:
    print("❌ FAILED: SQL Server JDBC Driver is missing.")

print("\nValidation Complete.")