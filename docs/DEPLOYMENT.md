# Deployment Guide: Bronze Layer CDC Pipeline

This guide outlines the step-by-step process to deploy the metadata-driven CDC pipeline for the Bronze layer.

## Prerequisites Checklist
- [ ] Azure SQL Database provisioned (Serverless recommended).
- [ ] Azure Data Lake Storage Gen2 provisioned with Hierarchical Namespace enabled.
- [ ] Azure Databricks Workspace provisioned.
- [ ] Azure Data Factory provisioned.
- [ ] Azure Key Vault provisioned with secrets for SQL and Storage.
- [ ] Change Tracking enabled on the source SQL Server database and all 13 source tables.

## Deployment Steps

### Phase 1: Database Setup
1. Connect to your Azure SQL Control Database.
2. Execute `sql/control_tables/01_create_control_tables.sql` to create the schema and tables.
3. Execute `sql/control_tables/02_populate_control_tables.sql` to insert the 13 entities and dependencies.
4. Execute `sql/stored_procedures/sp_GetCDCChanges.sql`.
5. Execute `sql/stored_procedures/sp_UpdateTableMetadata.sql`.
6. Execute `sql/stored_procedures/sp_GetTableLoadOrder.sql`.

### Phase 2: Databricks Setup
1. Import the notebooks from `databricks/notebooks/` into your Databricks workspace under `/Shared/`.
2. Create a cluster (Runtime 14.3 LTS or higher recommended).
3. Run the `setup/Mount_ADLS.py` notebook to ensure storage connectivity.
4. Run the `utilities/Validate_Dependencies.py` notebook to verify all connections and tables.

### Phase 3: Azure Data Factory Setup
1. **Linked Services**: 
   - Import the 3 JSON files from `adf/linkedService/`.
   - Replace `{{PLACEHOLDER_*}}` values with your actual Key Vault secret references or credentials.
2. **Datasets**:
   - Import the 3 JSON files from `adf/dataset/`.
3. **Pipelines**:
   - Import `PL_Initial_Load_Single_Table.json`.
   - Import `PL_Incremental_CDC_Single_Table.json`.
   - Import `PL_Bronze_Orchestrator.json`.
   - Import `PL_Medallion_Master.json`.

### Phase 4: Execution & Verification
1. Trigger `PL_Medallion_Master` in ADF.
2. Monitor the pipeline execution. The first run will execute `PL_Initial_Load_Single_Table` for all 13 tables.
3. Verify in the Control DB: