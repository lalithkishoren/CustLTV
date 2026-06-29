-- ==============================================================================
-- Unity Catalog RBAC & Role Design
-- Platform: Azure Databricks
-- Description: Creates catalogs, schemas, and applies Entra ID group grants.
-- ==============================================================================

-- 1. Create Catalogs for Medallion Architecture
CREATE CATALOG IF NOT EXISTS prd_bronze COMMENT 'Raw data landing zone with exact source fidelity';
CREATE CATALOG IF NOT EXISTS prd_silver COMMENT 'Cleansed, conformed, and standardized data with SCD tracking';
CREATE CATALOG IF NOT EXISTS prd_gold COMMENT 'Business-ready dimensional models and aggregated KPIs';
CREATE CATALOG IF NOT EXISTS prd_governance COMMENT 'Centralized governance, security policies, and audit logs';

-- 2. Create Schemas (Databases) per Domain
CREATE SCHEMA IF NOT EXISTS prd_bronze.erp;
CREATE SCHEMA IF NOT EXISTS prd_bronze.crm;
CREATE SCHEMA IF NOT EXISTS prd_bronze.marketing;

CREATE SCHEMA IF NOT EXISTS prd_silver.erp;
CREATE SCHEMA IF NOT EXISTS prd_silver.crm;
CREATE SCHEMA IF NOT EXISTS prd_silver.marketing;

CREATE SCHEMA IF NOT EXISTS prd_gold.sales;
CREATE SCHEMA IF NOT EXISTS prd_gold.customers;
CREATE SCHEMA IF NOT EXISTS prd_gold.marketing;

CREATE SCHEMA IF NOT EXISTS prd_governance.security;

-- 3. Grant Privileges to Service Principal (ADF Orchestration)
-- ADF needs to read/write across all data layers to execute pipelines
GRANT USE CATALOG ON CATALOG prd_bronze TO `sp_adf_etl`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA prd_bronze.erp TO `sp_adf_etl`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA prd_bronze.crm TO `sp_adf_etl`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA prd_bronze.marketing TO `sp_adf_etl`;

GRANT USE CATALOG ON CATALOG prd_silver TO `sp_adf_etl`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA prd_silver.erp TO `sp_adf_etl`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA prd_silver.crm TO `sp_adf_etl`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA prd_silver.marketing TO `sp_adf_etl`;

GRANT USE CATALOG ON CATALOG prd_gold TO `sp_adf_etl`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA prd_gold.sales TO `sp_adf_etl`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA prd_gold.customers TO `sp_adf_etl`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA prd_gold.marketing TO `sp_adf_etl`;

-- 4. Grant Privileges to Data Engineers (Debugging & Support)
-- Engineers get read access in Prod to debug issues, but no write access (CI/CD only)
GRANT USE CATALOG ON CATALOG prd_bronze TO `data_engineers`;
GRANT USE SCHEMA, SELECT ON CATALOG prd_bronze TO `data_engineers`;

GRANT USE CATALOG ON CATALOG prd_silver TO `data_engineers`;
GRANT USE SCHEMA, SELECT ON CATALOG prd_silver TO `data_engineers`;

GRANT USE CATALOG ON CATALOG prd_gold TO `data_engineers`;
GRANT USE SCHEMA, SELECT ON CATALOG prd_gold TO `data_engineers`;

-- 5. Grant Privileges to Data Analysts (BI & Analytics)
-- Analysts only get access to the Gold layer (Business-ready data)
GRANT USE CATALOG ON CATALOG prd_gold TO `data_analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA prd_gold.sales TO `data_analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA prd_gold.customers TO `data_analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA prd_gold.marketing TO `data_analysts`;

-- 6. Grant Privileges to Data Stewards (Governance & DQ)
GRANT USE CATALOG ON CATALOG prd_bronze TO `data_stewards`;
GRANT USE SCHEMA, SELECT ON CATALOG prd_bronze TO `data_stewards`;
GRANT USE CATALOG ON CATALOG prd_silver TO `data_stewards`;
GRANT USE SCHEMA, SELECT ON CATALOG prd_silver TO `data_stewards`;
GRANT USE CATALOG ON CATALOG prd_gold TO `data_stewards`;
GRANT USE SCHEMA, SELECT ON CATALOG prd_gold TO `data_stewards`;
GRANT USE CATALOG ON CATALOG prd_governance TO `data_stewards`;
GRANT USE SCHEMA, CREATE FUNCTION, EXECUTE ON SCHEMA prd_governance.security TO `data_stewards`;