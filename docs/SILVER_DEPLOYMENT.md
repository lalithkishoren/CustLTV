# Silver Layer Deployment Guide

## Prerequisites
1. Bronze layer must be fully deployed and operational.
2. Azure SQL Database must be provisioned with the `control` schema.
3. Databricks workspace must be active with a configured cluster.
4. Azure Key Vault must contain `storage-account-key` and `databricks-pat`.

## Deployment Steps

### 1. Database Setup
Execute the SQL scripts in the following order against the Azure SQL Control Database:
1. `sql/silver_control_tables/01_create_silver_control_tables.sql`
2. `sql/silver_control_tables/02_populate_silver_config.sql`
3. `sql/silver_stored_procedures/sp_UpdateSilverTableStatus.sql`
4. `sql/silver_stored_procedures/sp_GetSilverTransformationRules.sql`

### 2. Databricks Setup
1. Import the notebooks from `databricks/notebooks/silver/` into your Databricks workspace under `/Shared/silver/`.
2. Ensure the cluster has access to the ADLS Gen2 storage account via Service Principal or Access Key (passed via ADF).

### 3. Azure Data Factory Setup
1. Deploy Linked Services (if not already existing from Bronze):
   - `LS_AzureDataLakeStorage.json`
   - `LS_AzureDatabricks.json`
   - `LS_AzureSQL_Control.json`
2. Deploy Datasets:
   - `DS_Delta_Bronze.json`
   - `DS_Delta_Silver.json`
   - `DS_ControlDB_Silver.json`
3. Deploy Pipelines:
   - `PL_Silver_Transform_Single_Table.json`
   - `PL_Silver_Orchestrator.json`
   - `PL_Medallion_Master.json`

### 4. Validation
1. Trigger `PL_Medallion_Master` with `run_bronze = false` and `run_silver = true`.
2. Monitor the execution in ADF Monitor.
3. Verify data is written to `abfss://datalake@<storage>.dfs.core.windows.net/silver/`.
4. Check `control.silver_execution_log` for success statuses and row counts.
5. Check `control.dq_exception_log` for any quarantined rows.