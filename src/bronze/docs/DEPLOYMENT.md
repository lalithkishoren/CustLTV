# Deployment Guide: Bronze Layer CDC Pipeline

## Prerequisites
1. **Azure Resources Provisioned**:
   - Azure Data Factory (ADF)
   - Azure Databricks Workspace (Unity Catalog enabled)
   - Azure Data Lake Storage Gen2 (ADLS Gen2)
   - Azure SQL Database (Control DB)
   - Azure Key Vault
2. **Permissions**:
   - ADF Managed Identity needs `Storage Blob Data Contributor` on ADLS.
   - Databricks Access Connector needs `Storage Blob Data Contributor` on ADLS.
   - ADF Managed Identity needs `db_owner` or appropriate execute/write roles on Azure SQL.
3. **Source Systems**:
   - Change Tracking must be enabled on the source SQL databases for CDC to function.

## Deployment Steps

### Step 1: Database Setup
1. Connect to the Azure SQL Control Database.
2. Execute `sql/control_tables/01_create_control_tables.sql` to create the schema and tables.
3. Execute `sql/control_tables/02_populate_control_tables.sql` to insert the 13 entities.
4. Execute all scripts in `sql/stored_procedures/` to create the required procs.

### Step 2: Databricks Setup
1. Import the notebooks from `databricks/notebooks/` into your workspace under `/Shared/`.
2. Ensure the cluster has the SQL Server JDBC driver installed (usually default in DBR 14.3+).
3. Run `databricks/notebooks/setup/Verify_ADLS_Access.py` to confirm Unity Catalog is correctly authorizing ADLS access.
4. Run `databricks/notebooks/utilities/Validate_Dependencies.py` to ensure all libraries are present.

### Step 3: ADF Linked Services & Datasets
1. Replace all `{{PLACEHOLDER_*}}` values in the JSON files with your actual resource names/URIs.
2. Deploy Linked Services:
   - `LS_AzureSQL_Control.json`
   - `LS_AzureDataLakeStorage.json`
   - `LS_AzureDatabricks.json`
3. Deploy Datasets:
   - `DS_ControlDB.json`
   - `DS_AzureSQL_Source.json`
   - `DS_Parquet_Bronze_Staging.json`

### Step 4: ADF Pipelines
1. Deploy Pipelines in this order:
   - `PL_Initial_Load_Single_Table.json`
   - `PL_Incremental_CDC_Single_Table.json`
   - `PL_Bronze_Orchestrator.json`
   - `PL_Medallion_Master.json`

### Step 5: Execution
1. Trigger `PL_Medallion_Master`.
2. The orchestrator will detect `initial_load_completed = 0` for all tables and route them to the Initial Load pipeline.
3. Upon success, the flag updates to `1`. Subsequent runs will automatically route to the Incremental CDC pipeline.