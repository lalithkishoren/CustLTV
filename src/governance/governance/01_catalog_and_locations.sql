-- ==============================================================================
-- 01. CATALOG & EXTERNAL LOCATION REGISTRATION
-- Platform: Databricks Unity Catalog
-- Purpose: Establishes the physical storage links and logical namespaces.
-- ==============================================================================

-- 1. Create Storage Credentials (Assumes Azure Managed Identity is configured)
CREATE STORAGE CREDENTIAL IF NOT EXISTS `mi_myntra_adls_prod`
  COMMENT 'Managed Identity for Production ADLS Gen2 access';

-- 2. Create External Locations for Medallion Layers
CREATE EXTERNAL LOCATION IF NOT EXISTS `ext_myntra_bronze`
  URL 'abfss://bronze@stmyntraprod.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL `mi_myntra_adls_prod`)
  COMMENT 'Bronze landing zone';

CREATE EXTERNAL LOCATION IF NOT EXISTS `ext_myntra_silver`
  URL 'abfss://silver@stmyntraprod.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL `mi_myntra_adls_prod`)
  COMMENT 'Silver curated zone';

CREATE EXTERNAL LOCATION IF NOT EXISTS `ext_myntra_gold`
  URL 'abfss://gold@stmyntraprod.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL `mi_myntra_adls_prod`)
  COMMENT 'Gold analytics zone';

-- 3. Create Production Catalog
CREATE CATALOG IF NOT EXISTS `myntra_prod`
  COMMENT 'Production catalog for Myntra CLV Analytics Platform';

USE CATALOG `myntra_prod`;

-- 4. Create Schemas (Databases) mapped to External Locations
CREATE SCHEMA IF NOT EXISTS `bronze`
  MANAGED LOCATION 'abfss://bronze@stmyntraprod.dfs.core.windows.net/managed/'
  COMMENT 'Raw data ingested from ERP, CRM, and Marketing';

CREATE SCHEMA IF NOT EXISTS `silver`
  MANAGED LOCATION 'abfss://silver@stmyntraprod.dfs.core.windows.net/managed/'
  COMMENT 'Cleansed, conformed, and standardized data (SCD applied)';

CREATE SCHEMA IF NOT EXISTS `gold`
  MANAGED LOCATION 'abfss://gold@stmyntraprod.dfs.core.windows.net/managed/'
  COMMENT 'Dimensional models and aggregated KPIs for BI/ML';

-- 5. Transfer Ownership to Governance Admins
ALTER CATALOG `myntra_prod` OWNER TO `grp_myntra_gov_admins`;
ALTER SCHEMA `bronze` OWNER TO `grp_myntra_gov_admins`;
ALTER SCHEMA `silver` OWNER TO `grp_myntra_gov_admins`;
ALTER SCHEMA `gold` OWNER TO `grp_myntra_gov_admins`;