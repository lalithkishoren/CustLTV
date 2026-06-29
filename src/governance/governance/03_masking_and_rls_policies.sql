-- ==============================================================================
-- Unity Catalog Dynamic Data Masking & Row-Level Security
-- Platform: Azure Databricks
-- Description: Defines and applies masking policies for PII and RLS for domains.
-- ==============================================================================

USE CATALOG prd_governance;
USE SCHEMA security;

-- 1. Create Dynamic Masking Function for Standard Strings (e.g., Names, Phones)
CREATE OR REPLACE FUNCTION mask_pii_string(val STRING)
RETURNS STRING
RETURN CASE 
    -- Service Principals and Data Stewards see raw data
    WHEN is_account_group_member('sp_adf_etl') THEN val
    WHEN is_account_group_member('data_stewards') THEN val
    -- Everyone else sees a masked string
    ELSE '***MASKED***'
END;

-- 2. Create Dynamic Masking Function for Emails (Preserves domain for analytics)
CREATE OR REPLACE FUNCTION mask_pii_email(email STRING)
RETURNS STRING
RETURN CASE 
    WHEN is_account_group_member('sp_adf_etl') THEN email
    WHEN is_account_group_member('data_stewards') THEN email
    -- Mask local part, keep domain (e.g., j***@gmail.com)
    ELSE concat(left(email, 1), '***@', split(email, '@')[1])
END;

-- 3. Create Row-Level Security (RLS) Function for Regional Data (Example)
-- Ensures analysts only see data for regions they are authorized for
CREATE OR REPLACE FUNCTION rls_region_filter(region STRING)
RETURNS BOOLEAN
RETURN CASE
    WHEN is_account_group_member('sp_adf_etl') THEN TRUE
    WHEN is_account_group_member('data_stewards') THEN TRUE
    WHEN is_account_group_member('data_engineers') THEN TRUE
    -- Example: Map specific groups to regions
    WHEN is_account_group_member('analysts_na') AND region = 'NA' THEN TRUE
    WHEN is_account_group_member('analysts_emea') AND region = 'EMEA' THEN TRUE
    WHEN is_account_group_member('analysts_apac') AND region = 'APAC' THEN TRUE
    ELSE FALSE
END;

-- ==============================================================================
-- Apply Masking Policies to Silver Layer (Propagates to Gold via DLT/Views)
-- ==============================================================================

-- Apply to CRM Customers
ALTER TABLE prd_silver.crm.customers 
ALTER COLUMN first_name SET MASKING POLICY prd_governance.security.mask_pii_string;

ALTER TABLE prd_silver.crm.customers 
ALTER COLUMN last_name SET MASKING POLICY prd_governance.security.mask_pii_string;

ALTER TABLE prd_silver.crm.customers 
ALTER COLUMN email SET MASKING POLICY prd_governance.security.mask_pii_email;

ALTER TABLE prd_silver.crm.customers 
ALTER COLUMN phone_number SET MASKING POLICY prd_governance.security.mask_pii_string;

-- Apply to ERP Addresses
ALTER TABLE prd_silver.erp.addresses 
ALTER COLUMN street_address SET MASKING POLICY prd_governance.security.mask_pii_string;

ALTER TABLE prd_silver.erp.addresses 
ALTER COLUMN postal_code SET MASKING POLICY prd_governance.security.mask_pii_string;

-- ==============================================================================
-- Apply Masking Policies to Gold Layer (Dimensional Model)
-- ==============================================================================

ALTER TABLE prd_gold.customers.dim_customer 
ALTER COLUMN first_name SET MASKING POLICY prd_governance.security.mask_pii_string;

ALTER TABLE prd_gold.customers.dim_customer 
ALTER COLUMN last_name SET MASKING POLICY prd_governance.security.mask_pii_string;

ALTER TABLE prd_gold.customers.dim_customer 
ALTER COLUMN email SET MASKING POLICY prd_governance.security.mask_pii_email;