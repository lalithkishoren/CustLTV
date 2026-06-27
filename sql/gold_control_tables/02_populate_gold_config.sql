-- Clear existing data for clean insert
DELETE FROM control.gold_dimension_config;
DELETE FROM control.gold_aggregation_rules;
DELETE FROM control.gold_table_config;
DBCC CHECKIDENT ('control.gold_table_config', RESEED, 0);
DBCC CHECKIDENT ('control.gold_dimension_config', RESEED, 0);
DBCC CHECKIDENT ('control.gold_aggregation_rules', RESEED, 0);
GO

-- Insert Dimensions
INSERT INTO control.gold_table_config (source_silver_tables, target_gold_table, table_type, refresh_frequency) VALUES
('silver.customers', 'dim_customer', 'DIMENSION', 'DAILY'),
('silver.mtl_system_items_b,silver.categories,silver.brands', 'dim_product', 'DIMENSION', 'DAILY'),
('silver.addresses,silver.city_tier_master', 'dim_location', 'DIMENSION', 'DAILY'),
('silver.marketing_campaigns', 'dim_campaign', 'DIMENSION', 'DAILY'),
('silver.customer_registration_source', 'dim_registration_source', 'DIMENSION', 'DAILY'),
('STATIC', 'dim_date', 'DIMENSION', 'DAILY');

-- Insert Facts
INSERT INTO control.gold_table_config (source_silver_tables, target_gold_table, table_type, refresh_frequency) VALUES
('silver.oe_order_lines_all,silver.oe_order_headers_all', 'fact_sales', 'FACT', 'DAILY'),
('silver.interactions,silver.incidents', 'fact_interactions', 'FACT', 'DAILY'),
('silver.surveys', 'fact_surveys', 'FACT', 'DAILY');

-- Insert Aggregates
INSERT INTO control.gold_table_config (source_silver_tables, target_gold_table, table_type, refresh_frequency) VALUES
('gold.fact_sales,gold.dim_campaign,silver.marketing_campaigns', 'agg_monthly_campaign_roi', 'AGGREGATE', 'DAILY'),
('gold.fact_sales,gold.dim_customer', 'agg_customer_clv_metrics', 'AGGREGATE', 'DAILY');

-- Populate Dimension Configs
INSERT INTO control.gold_dimension_config (gold_table_id, scd_type, business_key_columns, tracked_columns, surrogate_key_column)
SELECT gold_table_id, 2, 'customer_id', 'customer_type,status,marketing_opt_in', 'dim_customer_key'
FROM control.gold_table_config WHERE target_gold_table = 'dim_customer';

INSERT INTO control.gold_dimension_config (gold_table_id, scd_type, business_key_columns, tracked_columns, surrogate_key_column)
SELECT gold_table_id, 1, 'inventory_item_id', NULL, 'dim_product_key'
FROM control.gold_table_config WHERE target_gold_table = 'dim_product';

INSERT INTO control.gold_dimension_config (gold_table_id, scd_type, business_key_columns, tracked_columns, surrogate_key_column)
SELECT gold_table_id, 1, 'address_id', NULL, 'dim_location_key'
FROM control.gold_table_config WHERE target_gold_table = 'dim_location';

INSERT INTO control.gold_dimension_config (gold_table_id, scd_type, business_key_columns, tracked_columns, surrogate_key_column)
SELECT gold_table_id, 1, 'campaign_id', NULL, 'dim_campaign_key'
FROM control.gold_table_config WHERE target_gold_table = 'dim_campaign';

INSERT INTO control.gold_dimension_config (gold_table_id, scd_type, business_key_columns, tracked_columns, surrogate_key_column)
SELECT gold_table_id, 1, 'registration_source_id', NULL, 'dim_registration_source_key'
FROM control.gold_table_config WHERE target_gold_table = 'dim_registration_source';

INSERT INTO control.gold_dimension_config (gold_table_id, scd_type, business_key_columns, tracked_columns, surrogate_key_column)
SELECT gold_table_id, 1, 'full_date', NULL, 'dim_date_key'
FROM control.gold_table_config WHERE target_gold_table = 'dim_date';

-- Populate Aggregation Rules (For dynamic processing if needed, though LLD specifies exact SQL)
INSERT INTO control.gold_aggregation_rules (gold_table_id, aggregation_name, aggregation_type, source_column, target_column, group_by_columns)
SELECT gold_table_id, 'Monthly Campaign ROI', 'CUSTOM', 'line_total', 'total_revenue', 'year_month,dim_campaign_key'
FROM control.gold_table_config WHERE target_gold_table = 'agg_monthly_campaign_roi';

INSERT INTO control.gold_aggregation_rules (gold_table_id, aggregation_name, aggregation_type, source_column, target_column, group_by_columns)
SELECT gold_table_id, 'Customer CLV Metrics', 'CUSTOM', 'line_total', 'lifetime_revenue', 'dim_customer_key'
FROM control.gold_table_config WHERE target_gold_table = 'agg_customer_clv_metrics';
GO