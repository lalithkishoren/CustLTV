# Troubleshooting Guide

## 1. Initial Load Runs Every Time
**Symptom**: The pipeline keeps executing `PL_Initial_Load_Single_Table` instead of CDC.
**Cause**: The `initial_load_completed` flag in `control.table_metadata` is not updating to `1`.
**Resolution**:
- Check the Databricks notebook output for `Initial_Load_Dynamic`. Ensure the JDBC update query executed successfully.
- Verify the ADF pipeline `SP_UpdateMetadata_Success` activity succeeded.
- Manually check the table: `SELECT table_name, initial_load_completed FROM control.table_metadata`.

## 2. MERGE Fails with "Multiple matches"
**Symptom**: `Incremental_CDC_Dynamic` fails during the `target_table.merge()` operation.
**Cause**: The source data contains multiple rows with the same Primary Key, causing an ambiguous merge.
**Resolution**:
- The notebook includes a deduplication step using `row_number() over (partition by PK order by SYS_CHANGE_VERSION desc)`. Ensure the `primary_key_columns` in the control table exactly match the actual unique keys of the source table.
- For composite keys, ensure they are comma-separated without spaces (e.g., `CITY,STATE`).

## 3. Change Tracking Version Invalid
**Symptom**: `sp_GetCDCChanges` fails with an error about an invalid sync version.
**Cause**: The source database purged the Change Tracking history for the requested `last_sync_version` (retention period exceeded).
**Resolution**:
- The table must be re-initialized.
- Run: `UPDATE control.table_metadata SET initial_load_completed = 0, last_sync_version = NULL WHERE table_name = 'YourTable'`
- Re-run the pipeline to trigger a fresh Initial Load.

## 4. Storage Access Denied (DELTA_INVALID_FORMAT or 403)
**Symptom**: Databricks fails to read/write to ADLS.
**Cause**: Unity Catalog External Location is misconfigured, or paths are mixed up.
**Resolution**:
- Ensure you are NOT mixing staging paths (Parquet) with Delta paths.
- Run `Verify_ADLS_Access.py` to confirm the Databricks Access Connector has `Storage Blob Data Contributor` on the storage account.

## 5. How to Add a New Table
1. Insert a new row into `control.table_metadata` with `initial_load_completed = 0`.
2. The `PL_Bronze_Orchestrator` will automatically pick it up on the next run.