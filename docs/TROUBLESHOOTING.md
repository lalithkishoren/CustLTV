# Troubleshooting Guide

## Common Issues & Resolutions

### 1. Initial Load Runs Every Time
**Symptom**: The pipeline keeps running `PL_Initial_Load_Single_Table` instead of incremental CDC.
**Cause**: The `initial_load_completed` flag in `control.table_metadata` is not being set to 1.
**Resolution**: 
- Check the ADF pipeline run logs for the `SP_UpdateMetadata_Success` activity.
- Verify the Databricks notebook `Initial_Load_Dynamic` completed successfully.
- Manually update if necessary: `UPDATE control.table_metadata SET initial_load_completed = 1 WHERE table_name = '...'`

### 2. MERGE Fails with "Ambiguous Match"
**Symptom**: Databricks notebook `Incremental_CDC_Dynamic` fails during the MERGE operation.
**Cause**: The source data contains multiple updates for the same primary key in a single batch.
**Resolution**: 
- The notebook includes a deduplication step using `Window.partitionBy(*pk_cols).orderBy(col("SYS_CHANGE_VERSION").desc())`. Ensure the `primary_key_columns` in `control.table_metadata` are correct and comma-separated for composite keys.

### 3. Change Tracking Version Invalid
**Symptom**: `sp_GetCDCChanges` fails with an error about invalid sync version.
**Cause**: The source database purged the change tracking history for the requested version (retention period exceeded).
**Resolution**:
1. Reset the table for a full load: