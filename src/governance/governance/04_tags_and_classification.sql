-- ==============================================================================
-- 04. DATA CLASSIFICATION & TAGGING
-- Platform: Databricks Unity Catalog
-- Purpose: Applies metadata tags for discovery, audit, and compliance tracking.
-- ==============================================================================

USE CATALOG `myntra_prod`;

-- 1. Schema Level Tagging
ALTER SCHEMA `bronze` SET TAGS ('layer' = 'landing', 'trust_level' = 'low');
ALTER SCHEMA `silver` SET TAGS ('layer' = 'curated', 'trust_level' = 'medium');
ALTER SCHEMA `gold`   SET TAGS ('layer' = 'business', 'trust_level' = 'high');

-- 2. Table Level Tagging (Examples based on classification map)
ALTER TABLE `silver`.`customers` SET TAGS (
    'domain' = 'CRM',
    'contains_pii' = 'true',
    'owner' = 'crm_data_steward'
);

ALTER TABLE `gold`.`dim_customer` SET TAGS (
    'domain' = 'Conformed',
    'contains_pii' = 'true',
    'grain' = 'One row per Customer SCD2'
);

ALTER TABLE `gold`.`fact_sales` SET TAGS (
    'domain' = 'Sales',
    'contains_pii' = 'false',
    'grain' = 'One row per Order Line'
);

-- 3. Column Level Tagging (PII Identification)
ALTER TABLE `silver`.`customers` ALTER COLUMN `email` SET TAGS (
    'sensitivity' = 'Restricted',
    'pii_type' = 'Email'
);

ALTER TABLE `silver`.`customers` ALTER COLUMN `phone_number` SET TAGS (
    'sensitivity' = 'Restricted',
    'pii_type' = 'Phone'
);

ALTER TABLE `gold`.`dim_customer` ALTER COLUMN `email` SET TAGS (
    'sensitivity' = 'Restricted',
    'pii_type' = 'Email'
);

ALTER TABLE `gold`.`dim_customer` ALTER COLUMN `phone_number` SET TAGS (
    'sensitivity' = 'Restricted',
    'pii_type' = 'Phone'
);