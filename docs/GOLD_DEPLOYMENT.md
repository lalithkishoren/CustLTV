# Gold Layer Deployment Guide

## Prerequisites
1. Bronze and Silver layers must be fully deployed and operational.
2. Azure SQL Control Database must be provisioned.
3. Databricks workspace must have Unity Catalog enabled.
4. Azure Key Vault must contain all required secrets (`storage-key`, `sql-password`, etc.).

## Deployment Steps

### 1. Control Database Setup
Execute the SQL scripts in the following order against the Azure SQL Control Database:
1. `sql/gold_control_tables/01_create_gold_control_tables.sql`
2. `sql/gold_control_tables/02_populate_gold_config.sql`
3. `sql/gold_stored_procedures/sp_UpdateGoldTableStatus.sql`
4. `sql/gold_stored_procedures/sp_GetGoldAggregationRules.sql`
5. `sql/gold_stored_procedures/sp_GetGoldDimensionConfig.sql`

### 2. Databricks Notebooks
Import the following notebooks into the Databricks workspace under `/Shared/gold/`:
- `Gold_Build_Dimension.py`
- `Gold_Build_Fact.py`
- `Gold_Build_Aggregate.py`
- `Gold_Data_Mart.py`

### 3. Azure Data Factory (ADF)
Deploy the ADF artifacts in the following order:
1. **Linked Services**: `LS_AzureDataLakeStorage.json`, `LS_AzureDatabricks.json`, `LS_AzureSQL_Control.json`
2. **Datasets**: `DS_Delta_Silver.json`, `DS_Delta_Gold.json`, `DS_ControlDB_Gold.json`
3. **Pipelines**: 
   - `PL_Gold_Build_Dimension.json`
   - `PL_Gold_Build_Fact.json`
   - `PL_Gold_Build_Aggregate.json`
   - `PL_Gold_Orchestrator.json`
   - `PL_Medallion_Master.json`

### 4. Initial Load Execution
1. Trigger `PL_Medallion_Master` with parameters:
   - `run_bronze`: false
   - `run_silver`: false
   - `run_gold`: true
2. Verify that `dim_date` is populated first.
3. Verify that all dimensions contain the Unknown member (`-1`).
4. Verify that `dim_campaign` contains the Organic member (`-2`).
5. Check `control.gold_execution_log` for successful completion.