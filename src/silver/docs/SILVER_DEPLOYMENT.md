# Silver Layer Deployment Guide

## Prerequisites
1. **Bronze Layer**: The Bronze layer must be fully deployed and operational.
2. **Unity Catalog**: Managed Identity (Access Connector) must be configured with `Storage Blob Data Contributor` on the ADLS Gen2 account. External Locations for `abfss://silver@...` must be registered.
3. **Azure SQL Database**: The control database must be accessible via ADF Managed Identity.

## Deployment Steps

### 1. Control Database Setup
Execute the SQL scripts in the following order against the Azure SQL Control Database:
1. `sql/silver_control_tables/01_create_silver_control_tables.sql` - Creates the schema and tables.
2. `sql/silver_control_tables/02_populate_silver_config.sql` - Populates the metadata for the 13 in-scope tables and their transformation rules.
3. `sql/silver_stored_procedures/sp_UpdateSilverTableStatus.sql` - Creates the status update SP.
4. `sql/silver_stored_procedures/sp_GetSilverTransformationRules.sql` - Creates the rule retrieval SP.

### 2. Databricks Notebooks
Upload the following notebooks to the Databricks workspace under `/Shared/silver/`:
- `Silver_Transform_Dynamic.py`
- `Silver_Data_Quality.py`
- `Silver_Schema_Evolution.py`

*Note: No secrets or storage keys need to be configured in Databricks. Unity Catalog handles all ADLS authentication natively.*

### 3. Azure Data Factory (ADF)
Deploy the ADF components in the following order:
1. **Linked Services**: `LS_AzureDataLakeStorage.json`, `LS_AzureDatabricks.json`, `LS_AzureSQL_Control.json`.
2. **Datasets**: `DS_Delta_Bronze.json`, `DS_Delta_Silver.json`, `DS_ControlDB_Silver.json`.
3. **Pipelines**: 
   - `PL_Silver_Transform_Single_Table.json`
   - `PL_Silver_Orchestrator.json`
   - `PL_Medallion_Master.json` (Replaces the Bronze-only master pipeline)

## Validation
1. Trigger `PL_Medallion_Master` with `run_bronze = false` and `run_silver = true`.
2. Verify that `control.silver_execution_log` populates with successful runs.
3. Verify that quarantined records (e.g., `TOTAL_AMOUNT <= 0`) are written to `abfss://silver@.../quarantine/`.