-- ==============================================================================
-- 02. ROLE-BASED ACCESS CONTROL (RBAC) GRANTS
-- Platform: Databricks Unity Catalog
-- Purpose: Enforces least-privilege access across the Medallion architecture.
-- ==============================================================================

USE CATALOG `myntra_prod`;

-- ------------------------------------------------------------------------------
-- A. Service Principal (ADF Orchestrator)
-- Needs full read/write to execute DLT pipelines and MERGE operations.
-- ------------------------------------------------------------------------------
GRANT USE CATALOG ON CATALOG `myntra_prod` TO `spn-adf-orchestrator`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA `bronze` TO `spn-adf-orchestrator`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA `silver` TO `spn-adf-orchestrator`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA `gold` TO `spn-adf-orchestrator`;

-- ------------------------------------------------------------------------------
-- B. Data Engineers
-- Needs ability to build, debug, and manage pipelines across all layers.
-- ------------------------------------------------------------------------------
GRANT USE CATALOG ON CATALOG `myntra_prod` TO `grp_myntra_data_engineers`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA `bronze` TO `grp_myntra_data_engineers`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA `silver` TO `grp_myntra_data_engineers`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA `gold` TO `grp_myntra_data_engineers`;

-- ------------------------------------------------------------------------------
-- C. Data Scientists
-- Needs read access to Silver (features) and Gold (aggregates) for ML models.
-- ------------------------------------------------------------------------------
GRANT USE CATALOG ON CATALOG `myntra_prod` TO `grp_myntra_data_scientists`;
GRANT USE SCHEMA, SELECT ON SCHEMA `silver` TO `grp_myntra_data_scientists`;
GRANT USE SCHEMA, SELECT ON SCHEMA `gold` TO `grp_myntra_data_scientists`;

-- ------------------------------------------------------------------------------
-- D. Data Analysts / BI Tools (Power BI)
-- Strictly limited to the Gold layer (Business-ready dimensional models).
-- ------------------------------------------------------------------------------
GRANT USE CATALOG ON CATALOG `myntra_prod` TO `grp_myntra_data_analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA `gold` TO `grp_myntra_data_analysts`;

-- ------------------------------------------------------------------------------
-- E. External Location Access (For direct file reads if necessary for ML/DLT)
-- ------------------------------------------------------------------------------
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `ext_myntra_bronze` TO `spn-adf-orchestrator`;
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `ext_myntra_silver` TO `spn-adf-orchestrator`;
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `ext_myntra_gold` TO `spn-adf-orchestrator`;

GRANT READ FILES ON EXTERNAL LOCATION `ext_myntra_silver` TO `grp_myntra_data_scientists`;