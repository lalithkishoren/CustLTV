-- ==============================================================================
-- Unity Catalog Tagging, Semantic Registration & Lineage
-- Platform: Azure Databricks
-- Description: Applies classification tags and business metadata for Purview sync.
-- ==============================================================================

-- 1. Apply Classification Tags to Catalogs and Schemas
ALTER CATALOG prd_bronze SET TAGS ('layer' = 'bronze', 'trust_level' = 'raw');
ALTER CATALOG prd_silver SET TAGS ('layer' = 'silver', 'trust_level' = 'cleansed');
ALTER CATALOG prd_gold SET TAGS ('layer' = 'gold', 'trust_level' = 'business_ready');

ALTER SCHEMA prd_gold.sales SET TAGS ('domain' = 'sales', 'owner' = 'sales_analytics_team');
ALTER SCHEMA prd_gold.customers SET TAGS ('domain' = 'customer_success', 'owner' = 'crm_analytics_team');

-- 2. Apply Table-Level and Column-Level Tags (Data Classification Map Implementation)
-- CRM Customers
ALTER TABLE prd_silver.crm.customers SET TAGS ('contains_pii' = 'true', 'domain' = 'crm');
ALTER TABLE prd_silver.crm.customers ALTER COLUMN first_name SET TAGS ('classification' = 'Restricted_PII');
ALTER TABLE prd_silver.crm.customers ALTER COLUMN last_name SET TAGS ('classification' = 'Restricted_PII');
ALTER TABLE prd_silver.crm.customers ALTER COLUMN email SET TAGS ('classification' = 'Restricted_PII');
ALTER TABLE prd_silver.crm.customers ALTER COLUMN phone_number SET TAGS ('classification' = 'Restricted_PII');

-- ERP Orders
ALTER TABLE prd_silver.erp.oe_order_headers_all SET TAGS ('contains_pii' = 'false', 'domain' = 'erp');
ALTER TABLE prd_silver.erp.oe_order_headers_all ALTER COLUMN total_amount SET TAGS ('classification' = 'Confidential');

-- Gold KPIs
ALTER TABLE prd_gold.customers.fact_sales SET TAGS ('classification' = 'Confidential', 'grain' = 'order_line');
ALTER TABLE prd_gold.customers.agg_customer_clv SET TAGS ('classification' = 'Confidential', 'grain' = 'customer_monthly');

-- 3. Register Semantic Business Meaning (Data Dictionary / Catalog Comments)
-- These comments are automatically synced to Microsoft Purview and visible in Power BI

COMMENT ON TABLE prd_gold.customers.dim_customer IS 
'Conformed Customer Dimension. Contains SCD Type 2 history of customer attributes including churn status. Source: Oracle CRM.';

COMMENT ON COLUMN prd_gold.customers.dim_customer.customer_id IS 
'Unique surrogate key for the customer dimension.';

COMMENT ON COLUMN prd_gold.customers.dim_customer.status IS 
'Current lifecycle status of the customer (e.g., Active, Churned). Tracked via SCD2 based on event time.';

COMMENT ON TABLE prd_gold.sales.fact_sales IS 
'Core sales fact table. Grain: One row per product line item within a customer order. Excludes Cancelled/Returned orders.';

COMMENT ON TABLE prd_gold.customers.agg_customer_clv IS 
'Monthly aggregated Customer Lifetime Value metrics. Includes pre-calculated Average Order Value (AOV) and Purchase Frequency.';

COMMENT ON COLUMN prd_gold.customers.agg_customer_clv.aov IS 
'Average Order Value (AOV). Calculation: SUM(TOTAL_AMOUNT) / COUNT(DISTINCT ORDER_ID).';

COMMENT ON COLUMN prd_gold.customers.agg_customer_clv.purchase_frequency IS 
'Purchase Frequency. Calculation: COUNT(DISTINCT ORDER_ID) / (DATEDIFF(day, MIN(ORDER_DATE), MAX(ORDER_DATE)) / 30.0).';

COMMENT ON COLUMN prd_gold.marketing.agg_campaign_roi.cac IS 
'Customer Acquisition Cost (CAC). Calculation: SUM(TOTAL_SPEND) / NULLIF(SUM(CUSTOMERS_ACQUIRED), 0). Organic channels explicitly set to 0.';