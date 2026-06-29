# Troubleshooting Guide

## Common Issues & Resolutions

### 1. "Initial load runs every time"
**Symptom**: The pipeline keeps running `PL_Initial_Load_Single_Table` instead of CDC.
**Cause**: The `initial_load_completed` flag in `control.table_metadata` is still `0`.
**Resolution**: 
- Check the Databricks `Initial_Load_Dynamic` notebook logs. Ensure it successfully calls `sp_UpdateTableMetadata` with `@MarkInitialLoadComplete = 1`.
- Verify the ADF pipeline passes the correct `table_id`.

### 2. "MERGE fails with ambiguous match"
**Symptom**: Databricks CDC notebook fails during the `MERGE INTO` operation.
**Cause**: Duplicate primary keys in the source CDC data.
**Resolution**: 
- The `Incremental_CDC_Dynamic.py` notebook includes a deduplication step using `row_number() over (partition by PK order by SYS_CHANGE_VERSION desc)`. Ensure the `primary_key_columns` parameter in the control table exactly matches the source table's actual primary keys.

### 3. "Change Tracking version invalid"
**Symptom**: `sp_GetCDCChanges` fails with an error about invalid change tracking version.
**Cause**: The source database purged the change tracking history for the `last_sync_version`.
**Resolution**: 
- You must perform a full refresh.
- Run: `UPDATE control.table_metadata SET initial_load_completed = 0, last_sync_version = NULL WHERE table_name = 'YourTable';`
- Re-run the pipeline.

### 4. "DELTA_INVALID_FORMAT Incompatible format detected"
**Symptom**: Databricks fails to read the Delta table.
**Cause**: ADF wrote Parquet files directly into the Delta table path instead of the Staging path.
**Resolution**: 
- Ensure `DS_Parquet_Bronze_Staging` points to `staging/...` and NOT the root `bronze/...` path.
- Delete the corrupted folder in ADLS and reset `initial_load_completed = 0` to reload.

### 5. Connection Failures (SQL or ADLS)
**Symptom**: ADF or Databricks cannot connect to resources.
**Resolution**:
- **ADLS**: Verify Unity Catalog External Locations and Storage Credentials. Ensure the Databricks Access Connector has `Storage Blob Data Contributor` on the storage account.
- **SQL**: Verify the SQL Server firewall allows Azure Services (or specific VNet/Private Endpoints). Check credentials in Key Vault.

## Operational Tasks

### How to reset a table for re-processing