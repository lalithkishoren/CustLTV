# Gold Layer Deployment Guide

## Prerequisites
1. **Unity Catalog**: Configured with Managed Identity (Databricks Access Connector).
2. **Storage**: ADLS Gen2 account with `gold` container.
3. **Silver Layer**: Must be fully deployed and populated.
4. **Control Database**: Azure SQL Database for metadata.

## Deployment Steps

### 1. Control Database Setup
Execute the SQL scripts in the `sql/gold_control_tables/` directory against the Control DB:
1. `01_create_gold_control_tables.sql`
2. `02_populate_gold_config.sql`

Execute the stored procedure scripts in `sql/gold_stored_procedures/`:
1. `sp_UpdateGoldTableStatus.sql`
2. `sp_GetGoldAggregationRules.sql`
3. `sp_GetGoldDimensionConfig.sql`

### 2. Databricks Setup
1. Import the notebooks from `databricks/notebooks/gold/` into the Databricks workspace under `/Shared/gold/`.
2. Ensure the cluster used by ADF has Unity Catalog enabled.
3. **CRITICAL**: Do NOT configure `fs.azure.account.key` in the cluster or notebooks. Unity Catalog handles all ADLS authorization via External Locations.

### 3. Azure Data Factory Setup
1. Import Linked Services from `adf/linkedService/`. Ensure Managed Identity is granted access to ADLS, Databricks, and SQL.
2. Import Datasets from `adf/dataset/`.
3. Import Pipelines from `adf/pipeline/`.

### 4. Initial Load Execution
1. Trigger `PL_Medallion_Master` with parameters:
   - `run_bronze`: false
   - `run_silver`: false
   - `run_gold`: true
2. Monitor the execution in ADF Monitor. The orchestrator will automatically process Dimensions first, then Facts, then Aggregates.

## Security & Governance
- **Data Masking**: Apply Unity Catalog Dynamic Data Masking on `dim_customer.email` and `dim_customer.phone`.
- **Access Control**: Grant `SELECT` on `gold.marts` to BI Service Principals.