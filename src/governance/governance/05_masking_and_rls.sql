-- ==============================================================================
-- 05. DYNAMIC DATA MASKING & ROW-LEVEL SECURITY
-- Platform: Databricks Unity Catalog
-- Purpose: Protects PII at query time and restricts row visibility by domain.
-- ==============================================================================

USE CATALOG `myntra_prod`;

-- ------------------------------------------------------------------------------
-- A. Dynamic Data Masking (DDM) Policies
-- ------------------------------------------------------------------------------

-- Create a masking function for Email addresses
CREATE OR REPLACE FUNCTION `gold`.`mask_email`(email STRING)
  RETURN 
    CASE 
      -- Service Principals and authorized PII readers see raw data
      WHEN IS_ACCOUNT_GROUP_MEMBER('grp_myntra_pii_readers') THEN email
      WHEN IS_ACCOUNT_GROUP_MEMBER('spn-adf-orchestrator') THEN email
      -- Others see masked data (e.g., j***@domain.com)
      WHEN email IS NULL THEN NULL
      ELSE CONCAT(LEFT(email, 1), '***@', SUBSTRING(email, INSTR(email, '@') + 1))
    END;

-- Create a masking function for Phone Numbers
CREATE OR REPLACE FUNCTION `gold`.`mask_phone`(phone STRING)
  RETURN 
    CASE 
      WHEN IS_ACCOUNT_GROUP_MEMBER('grp_myntra_pii_readers') THEN phone
      WHEN IS_ACCOUNT_GROUP_MEMBER('spn-adf-orchestrator') THEN phone
      WHEN phone IS NULL THEN NULL
      -- Mask all but last 4 digits
      ELSE CONCAT('***-***-', RIGHT(phone, 4))
    END;

-- Create a generic masking function for Names/Strings
CREATE OR REPLACE FUNCTION `gold`.`mask_string`(val STRING)
  RETURN 
    CASE 
      WHEN IS_ACCOUNT_GROUP_MEMBER('grp_myntra_pii_readers') THEN val
      WHEN IS_ACCOUNT_GROUP_MEMBER('spn-adf-orchestrator') THEN val
      WHEN val IS NULL THEN NULL
      ELSE '[REDACTED]'
    END;

-- Apply Masking Policies to Silver Layer
ALTER TABLE `silver`.`customers` ALTER COLUMN `email` SET MASK `gold`.`mask_email`;
ALTER TABLE `silver`.`customers` ALTER COLUMN `phone_number` SET MASK `gold`.`mask_phone`;
ALTER TABLE `silver`.`customers` ALTER COLUMN `first_name` SET MASK `gold`.`mask_string`;
ALTER TABLE `silver`.`customers` ALTER COLUMN `last_name` SET MASK `gold`.`mask_string`;

-- Apply Masking Policies to Gold Layer
ALTER TABLE `gold`.`dim_customer` ALTER COLUMN `email` SET MASK `gold`.`mask_email`;
ALTER TABLE `gold`.`dim_customer` ALTER COLUMN `phone_number` SET MASK `gold`.`mask_phone`;

-- ------------------------------------------------------------------------------
-- B. Row-Level Security (RLS) Policies
-- ------------------------------------------------------------------------------

-- Create an RLS function to restrict sales data by region (if applicable)
-- Assuming analysts are assigned to Entra ID groups like 'grp_myntra_region_na'
CREATE OR REPLACE FUNCTION `gold`.`rls_region_filter`(region_code STRING)
  RETURN 
    -- Admins and Service Principals see all rows
    IS_ACCOUNT_GROUP_MEMBER('grp_myntra_gov_admins') 
    OR IS_ACCOUNT_GROUP_MEMBER('spn-adf-orchestrator')
    -- Global analysts see all rows
    OR IS_ACCOUNT_GROUP_MEMBER('grp_myntra_global_analysts')
    -- Regional analysts see only their region
    OR (IS_ACCOUNT_GROUP_MEMBER('grp_myntra_region_na') AND region_code = 'NA')
    OR (IS_ACCOUNT_GROUP_MEMBER('grp_myntra_region_emea') AND region_code = 'EMEA')
    OR (IS_ACCOUNT_GROUP_MEMBER('grp_myntra_region_apac') AND region_code = 'APAC');

-- Apply RLS to the Sales Fact Table (assuming it joins to a location dimension with region_code)
-- Note: In a star schema, RLS is typically applied to the dimension, and BI tools propagate the filter.
-- If applying directly to a denormalized fact or dimension:
ALTER TABLE `gold`.`dim_location` SET ROW FILTER `gold`.`rls_region_filter` ON (`region_code`);