# Bronze Layer Testing Harness

This test suite validates the PySpark Databricks notebooks and data contracts for the Bronze layer. It uses `pytest`, `chispa`, and a local Delta Lake Spark session to execute the *actual* notebook code without requiring a live Databricks cluster or Azure SQL connection.

## Test Pyramid Coverage
- **Unit**: Tests the deduplication (window function) logic and utility functions.
- **Data Quality / Contract**: Validates that metadata columns (`_pipeline_run_id`, `_ingest_timestamp`, etc.) are appended and schema contracts are honored.
- **Idempotency**: Proves that re-running `Initial_Load_Dynamic.py` (partition overwrite) and `Incremental_CDC_Dynamic.py` (MERGE) produces identical, duplicate-free results.
- **Reconciliation**: Asserts that the `records_loaded` output matches the source staging row counts.
- **SCD**: Validates Type 1 upsert logic (Inserts, Updates, Deletes) based on `SYS_CHANGE_OPERATION`.

## Setup & Execution

1. **Install Dependencies**: