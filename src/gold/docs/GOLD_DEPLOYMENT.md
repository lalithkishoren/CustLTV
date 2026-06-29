# Gold Layer Deployment Guide

## Prerequisites
1. **Silver Layer**: Must be fully deployed and populated.
2. **Unity Catalog**: Configured with Managed Identity (Access Connector).
3. **Azure SQL Control DB**: Provisioned and accessible via Managed Identity.

## Deployment Steps

### 1. Control Database Setup
Execute the SQL scripts in the `sql/gold_control_tables/` directory against the Azure SQL Control Database:
1. Run `01_create_gold_control_tables.sql` to create the schema and tables.
2. Run `02_populate_gold_config.sql` to insert the LLD configurations.
3. Execute the scripts in `sql/gold_stored_procedures/` to create the required SPs.

### 2. Azure Data Factory Setup
1. Import the Linked Services from `adf/linkedService/`. Ensure placeholders (e.g., `stdataplatformdevnda0jg`) are replaced with actual values or Key Vault references.
2. Import the Datasets from `adf/dataset/`.
3. Import the Pipelines from `adf/pipeline/`.

### 3. Databricks Setup
1. Import the notebooks from `databricks/notebooks/gold/` into the `/Shared/gold/` workspace directory.
2. Ensure the Databricks cluster has Unity Catalog enabled. No storage keys should be configured in the cluster environment variables.

### 4. Initial Load Execution
1. Trigger `PL_Medallion_Master` in ADF with parameters:
   - `run_bronze`: false
   - `run_silver`: false
   - `run_gold`: true
2. Monitor the execution in ADF Monitor. The orchestrator will automatically build Dimensions first, then Facts, then Aggregates.