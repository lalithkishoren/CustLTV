# Silver Layer Test Harness

This test suite validates the PySpark/Databricks notebooks that power the Silver layer of the Medallion architecture. It ensures compliance with enterprise data quality rules, idempotency, and Slowly Changing Dimensions (SCD) logic.

## Architecture
Because the transformation logic resides in Databricks Notebooks (procedural scripts rather than packaged Python modules), this test harness uses a custom `execute_notebook` fixture in `conftest.py`. 

This fixture:
1. Reads the actual `.py` notebook files.
2. Mocks `dbutils.widgets` and `dbutils.notebook.exit`.
3. Dynamically replaces hardcoded Azure Data Lake (`abfss://`) paths with local temporary directories.
4. Executes the code inside a local session-scoped Delta-configured `SparkSession`.

This guarantees we are testing the **exact code** deployed to production without modification.

## Prerequisites
Ensure you have a Java 8 or 11 runtime installed (required for local PySpark).