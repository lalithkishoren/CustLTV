# Silver Layer Deployment Guide

## Prerequisites
1. **Bronze Layer**: The Bronze layer must be fully deployed and operational.
2. **Unity Catalog**: Managed Identity access must be configured. Databricks Access Connector must have `Storage Blob Data Contributor` on the ADLS Gen2 account.
3. **Azure SQL**: The `control` schema must exist.

## Deployment Steps

### 1. Database Setup
Execute the SQL scripts in the following order against the Azure SQL Control Database:
1. `sql/silver_control_tables/01_create_silver_control_tables.sql`
2. `sql/silver_control_tables/02_populate_silver_config.sql`
3. `sql/silver_stored_procedures/sp_UpdateSilverTableStatus.sql`
4. `sql/silver_stored_procedures/sp_GetSilverTransformationRules.sql`

### 2. Databricks Setup
1. Import the notebooks from `databricks/notebooks/silver/` into your Databricks workspace under `/Shared/silver/`.
2. Ensure the cluster referenced by `{{PLACEHOLDER_CLUSTER_ID}}` has Unity Catalog enabled.

### 3. Azure Data Factory Setup
1. Deploy Linked Services (`LS_AzureDataLakeStorage`, `LS_AzureDatabricks`, `LS_AzureSQL_Control`).
2. Deploy Datasets (`DS_Delta_Bronze`, `DS_Delta_Silver`, `DS_ControlDB_Silver`).
3. Deploy Pipelines (`PL_Silver_Transform_Single_Table`, `PL_Silver_Orchestrator`, `PL_Medallion_Master`).

### 4. Validation
1. Trigger `PL_Medallion_Master` with `run_bronze = false` and `run_silver = true`.
2. Verify that data is written to `abfss://silver@<storage>.dfs.core.windows.net/`.
3. Check `control.silver_execution_log` for success statuses.