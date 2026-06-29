-- Clear existing configurations for clean insert
DELETE FROM control.gold_dimension_config;
DELETE FROM control.gold_aggregation_rules;
DELETE FROM control.gold_table_config;

-- Insert Dimensions
INSERT INTO control.gold_table_config (source_silver_tables, target_gold_table, table_type, refresh_frequency, is_active)
VALUES 
('silver.customers', 'dim_customer', 'DIMENSION', 'DAILY', 1),
('silver.mtl_system_items_b,silver.categories,silver.brands', 'dim_product', 'DIMENSION', 'DAILY', 1),
('silver.addresses,silver.city_tier_master', 'dim_location', 'DIMENSION', 'DAILY', 1),
('silver.marketing_campaigns', 'dim_campaign', 'DIMENSION', 'DAILY', 1),
('silver.customer_registration_source', 'dim_registration_source', 'DIMENSION', 'DAILY', 1),
('none', 'dim_date', 'DIMENSION', 'DAILY', 1);

-- Insert Facts
INSERT INTO control.gold_table_config (source_silver_tables, target_gold_table, table_type, refresh_frequency, is_active)
VALUES 
('silver.oe_order_lines_all,silver.oe_order_headers_all', 'fact_sales', 'FACT', 'DAILY', 1),
('silver.interactions,silver.incidents', 'fact_interactions', 'FACT', 'DAILY', 1),
('silver.surveys', 'fact_surveys', 'FACT', 'DAILY', 1);

-- Insert Aggregates
INSERT INTO control.gold_table_config (source_silver_tables, target_gold_table, table_type, refresh_frequency, is_active)
VALUES 
('gold.fact_sales,gold.dim_campaign,gold.dim_date,silver.marketing_campaigns', 'agg_monthly_campaign_roi', 'AGGREGATE', 'DAILY', 1),
('gold.fact_sales,gold.dim_customer,gold.dim_date', 'agg_customer_clv_metrics', 'AGGREGATE', 'DAILY', 1);

-- Populate Dimension Configs
INSERT INTO control.gold_dimension_config (gold_table_id, scd_type, business_key_columns, tracked_columns)
SELECT gold_table_id, 2, 'customer_id', 'customer_type,status,marketing_opt_in'
FROM control.gold_table_config WHERE target_gold_table = 'dim_customer';

INSERT INTO control.gold_dimension_config (gold_table_id, scd_type, business_key_columns, tracked_columns)
SELECT gold_table_id, 1, 'inventory_item_id', NULL
FROM control.gold_table_config WHERE target_gold_table = 'dim_product';

INSERT INTO control.gold_dimension_config (gold_table_id, scd_type, business_key_columns, tracked_columns)
SELECT gold_table_id, 1, 'address_id', NULL
FROM control.gold_table_config WHERE target_gold_table = 'dim_location';

INSERT INTO control.gold_dimension_config (gold_table_id, scd_type, business_key_columns, tracked_columns)
SELECT gold_table_id, 1, 'campaign_id', NULL
FROM control.gold_table_config WHERE target_gold_table = 'dim_campaign';

INSERT INTO control.gold_dimension_config (gold_table_id, scd_type, business_key_columns, tracked_columns)
SELECT gold_table_id, 1, 'registration_source_id', NULL
FROM control.gold_table_config WHERE target_gold_table = 'dim_registration_source';

INSERT INTO control.gold_dimension_config (gold_table_id, scd_type, business_key_columns, tracked_columns)
SELECT gold_table_id, 1, 'dim_date_key', NULL
FROM control.gold_table_config WHERE target_gold_table = 'dim_date';

-- Populate Aggregation Rules (Custom SQL for complex LLD logic)
INSERT INTO control.gold_aggregation_rules (gold_table_id, aggregation_name, aggregation_type, source_column, target_column, group_by_columns, filter_expression)
SELECT gold_table_id, 'Monthly Campaign ROI', 'CUSTOM_SQL', NULL, 'agg_monthly_campaign_roi', 'year_month,dim_campaign_key', NULL
FROM control.gold_table_config WHERE target_gold_table = 'agg_monthly_campaign_roi';

INSERT INTO control.gold_aggregation_rules (gold_table_id, aggregation_name, aggregation_type, source_column, target_column, group_by_columns, filter_expression)
SELECT gold_table_id, 'Customer CLV Metrics', 'CUSTOM_SQL', NULL, 'agg_customer_clv_metrics', 'dim_customer_key', NULL
FROM control.gold_table_config WHERE target_gold_table = 'agg_customer_clv_metrics';