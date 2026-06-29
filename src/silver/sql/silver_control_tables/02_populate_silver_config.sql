-- Clear existing configurations for idempotency during deployment
DELETE FROM control.silver_transformation_rules;
DELETE FROM control.silver_table_config;

-- Insert Silver Table Configurations
INSERT INTO control.silver_table_config 
(source_bronze_path, target_silver_path, table_name, primary_key_columns, scd_type, partition_columns, z_order_columns, load_priority)
VALUES
-- CRM Domain
('bronze/SRC-002/CRM/CUSTOMERS', 'silver/SRC-002/CRM/CUSTOMERS', 'customers', 'customer_id', 2, NULL, 'customer_id', 10),
('bronze/SRC-002/CRM/CUSTOMER_REGISTRATION_SOURCE', 'silver/SRC-002/CRM/CUSTOMER_REGISTRATION_SOURCE', 'customer_registration_source', 'registration_source_id', 1, NULL, 'customer_id,campaign_id', 20),
('bronze/SRC-002/CRM/INCIDENTS', 'silver/SRC-002/CRM/INCIDENTS', 'incidents', 'incident_id', 1, NULL, 'customer_id,order_id', 20),
('bronze/SRC-002/CRM/INTERACTIONS', 'silver/SRC-002/CRM/INTERACTIONS', 'interactions', 'interaction_id', 1, NULL, 'incident_id', 30),
('bronze/SRC-002/CRM/SURVEYS', 'silver/SRC-002/CRM/SURVEYS', 'surveys', 'survey_id', 1, NULL, 'customer_id,order_id', 20),

-- ERP Domain
('bronze/SRC-001/ERP/OE_ORDER_HEADERS_ALL', 'silver/SRC-001/ERP/OE_ORDER_HEADERS_ALL', 'oe_order_headers_all', 'order_id', 1, '_order_year_month', 'customer_id', 20),
('bronze/SRC-001/ERP/OE_ORDER_LINES_ALL', 'silver/SRC-001/ERP/OE_ORDER_LINES_ALL', 'oe_order_lines_all', 'line_id', 1, '_order_year_month', 'order_id,product_id', 30),
('bronze/SRC-001/ERP/ADDRESSES', 'silver/SRC-001/ERP/ADDRESSES', 'addresses', 'address_id', 1, NULL, 'customer_id,city,state', 20),
('bronze/SRC-001/ERP/CITY_TIER_MASTER', 'silver/SRC-001/ERP/CITY_TIER_MASTER', 'city_tier_master', 'city,state', 1, NULL, 'city,state', 10),
('bronze/SRC-001/ERP/MTL_SYSTEM_ITEMS_B', 'silver/SRC-001/ERP/MTL_SYSTEM_ITEMS_B', 'mtl_system_items_b', 'inventory_item_id', 1, NULL, 'category_id,brand_id', 20),
('bronze/SRC-001/ERP/CATEGORIES', 'silver/SRC-001/ERP/CATEGORIES', 'categories', 'category_id', 1, NULL, 'category_id', 10),
('bronze/SRC-001/ERP/BRANDS', 'silver/SRC-001/ERP/BRANDS', 'brands', 'brand_id', 1, NULL, 'brand_id', 10),

-- Marketing Domain
('bronze/SRC-003/MARKETING/MARKETING_CAMPAIGNS', 'silver/SRC-003/MARKETING/MARKETING_CAMPAIGNS', 'marketing_campaigns', 'campaign_id', 1, NULL, 'campaign_id,channel', 10);

-- Insert Transformation Rules (Example for customers table)
DECLARE @CustomersTableId INT = (SELECT silver_table_id FROM control.silver_table_config WHERE table_name = 'customers');

INSERT INTO control.silver_transformation_rules 
(silver_table_id, rule_name, rule_type, source_column, target_column, transformation_expression, execution_order)
VALUES
(@CustomersTableId, 'Cast Customer ID', 'CAST', 'CUSTOMER_ID', 'customer_id', 'BIGINT', 10),
(@CustomersTableId, 'Standardize Email', 'TRANSFORM', 'EMAIL', 'email', 'LOWER(TRIM(EMAIL))', 20),
(@CustomersTableId, 'Standardize Status', 'TRANSFORM', 'STATUS', 'status', 'UPPER(TRIM(STATUS))', 30),
(@CustomersTableId, 'Cast Registration Date', 'CAST', 'REGISTRATION_DATE', 'registration_date', 'TIMESTAMP', 40);

-- Insert Transformation Rules (Example for oe_order_headers_all table)
DECLARE @OrdersTableId INT = (SELECT silver_table_id FROM control.silver_table_config WHERE table_name = 'oe_order_headers_all');

INSERT INTO control.silver_transformation_rules 
(silver_table_id, rule_name, rule_type, source_column, target_column, transformation_expression, execution_order)
VALUES
(@OrdersTableId, 'Cast Order ID', 'CAST', 'ORDER_ID', 'order_id', 'BIGINT', 10),
(@OrdersTableId, 'Cast Total Amount', 'CAST', 'TOTAL_AMOUNT', 'total_amount', 'DECIMAL(15,2)', 20),
(@OrdersTableId, 'Standardize Order Status', 'TRANSFORM', 'ORDER_STATUS', 'order_status', 'UPPER(TRIM(ORDER_STATUS))', 30),
(@OrdersTableId, 'Derive Partition Column', 'TRANSFORM', 'ORDER_DATE', '_order_year_month', 'DATE_FORMAT(CAST(ORDER_DATE AS TIMESTAMP), "yyyy-MM")', 40);