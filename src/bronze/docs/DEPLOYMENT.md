# Deployment Guide: Bronze Layer CDC Pipeline

## Prerequisites Checklist
- [ ] Azure SQL Database provisioned.
- [ ] ADLS Gen2 Storage Account provisioned with `bronze`, `silver`, `gold` containers.
- [ ] Databricks Workspace provisioned with Unity Catalog enabled.
- [ ] Databricks Access Connector configured with Storage Blob Data Contributor on ADLS.
- [ ] Unity Catalog External Locations created for `abfss://bronze@...`, `silver`, `gold`.
- [ ] Azure Data Factory provisioned with Managed Identity.
- [ ] Azure Key Vault provisioned (optional but recommended for SQL credentials).
- [ ] Change Tracking enabled on source SQL databases.

## Step-by-Step Deployment Order

### 1. Create Control Tables (SQL)
Execute the following scripts in your Azure SQL Control Database:
1. `sql/control_tables/01_create_control_tables.sql`
2. `sql/control_tables/02_populate_control_tables.sql`

### 2. Create Stored Procedures (SQL)
Execute the following scripts in your Azure SQL Control Database:
1. `sql/stored_procedures/sp_GetCDCChanges.sql`
2. `sql/stored_procedures/sp_UpdateTableMetadata.sql`
3. `sql/stored_procedures/sp_GetTableLoadOrder.sql`

### 3. Configure Databricks
1. Import the notebooks from `databricks/notebooks/` into your workspace under `/Shared/`.
2. Ensure your cluster has `pyodbc` installed (usually default in DBR, or add via Libraries).
3. Run `Verify_ADLS_Access.py` to confirm Unity Catalog is correctly authorizing ADLS access. **Do not mount storage.**

### 4. Deploy ADF Linked Services
Replace the `{{PLACEHOLDER_*}}` values in the JSON files with your actual resource names or Key Vault references, then deploy:
1. `adf/linkedService/LS_AzureSQL_Control.json`
2. `adf/linkedService/LS_AzureDataLakeStorage.json`
3. `adf/linkedService/LS_AzureDatabricks.json`

### 5. Deploy ADF Datasets
Deploy the datasets:
1. `adf/dataset/DS_ControlDB.json`
2. `adf/dataset/DS_AzureSQL_Source.json`
3. `adf/dataset/DS_Parquet_Bronze_Staging.json`

### 6. Deploy ADF Pipelines
Deploy the pipelines in this exact order:
1. `adf/pipeline/PL_Initial_Load_Single_Table.json`
2. `adf/pipeline/PL_Incremental_CDC_Single_Table.json`
3. `adf/pipeline/PL_Bronze_Orchestrator.json`
4. `adf/pipeline/PL_Medallion_Master.json`

### 7. Final Verification
1. Run the `Validate_Dependencies.py` notebook in Databricks.
2. Trigger `PL_Medallion_Master` in ADF.
3. Verify that `initial_load_completed` updates to `1` in `control.table_metadata`.