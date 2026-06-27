-- Clear existing configurations to ensure clean state
DELETE FROM control.silver_transformation_rules;
DELETE FROM control.silver_table_config;

-- Insert Silver Table Configurations based on LLD
INSERT INTO control.silver_table_config 
(source_bronze_table, target_silver_table, silver_schema, primary_key_columns, scd_type, track_history_columns, partition_columns, z_order_columns, load_priority)
VALUES
-- Dimensions (Priority 10)
('src-002/crm/customers', 'customers', 'silver', 'customer_id', 2, 'status,customer_type,marketing_opt_in', NULL, 'customer_id', 10),
('src-001/erp/addresses', 'addresses', 'silver', 'address_id', 1, NULL, NULL, 'customer_id,city,state', 10),
('src-001/erp/city_tier_master', 'city_tier_master', 'silver', 'city,state', 1, NULL, NULL, 'city,state', 10),
('src-001/erp/categories', 'categories', 'silver', 'category_id', 1, NULL, NULL, 'category_id', 10),
('src-001/erp/brands', 'brands', 'silver', 'brand_id', 1, NULL, NULL, 'brand_id', 10),
('src-001/erp/mtl_system_items_b', 'mtl_system_items_b', 'silver', 'inventory_item_id', 1, NULL, NULL, 'category_id,brand_id', 10),
('src-003/marketing/marketing_campaigns', 'marketing_campaigns', 'silver', 'campaign_id', 1, NULL, NULL, 'campaign_id,channel', 10),

-- Facts (Priority 20)
('src-001/erp/oe_order_headers_all', 'oe_order_headers_all', 'silver', 'order_id', 1, NULL, '_order_year_month', 'customer_id', 20),
('src-001/erp/oe_order_lines_all', 'oe_order_lines_all', 'silver', 'line_id', 1, NULL, '_order_year_month', 'order_id,product_id', 20),
('src-002/crm/customer_registration_source', 'customer_registration_source', 'silver', 'registration_source_id', 1, NULL, NULL, 'customer_id,campaign_id', 20),
('src-002/crm/incidents', 'incidents', 'silver', 'incident_id', 1, NULL, NULL, 'customer_id,order_id', 20),
('src-002/crm/interactions', 'interactions', 'silver', 'interaction_id', 1, NULL, NULL, 'incident_id', 20),
('src-002/crm/surveys', 'surveys', 'silver', 'survey_id', 1, NULL, NULL, 'customer_id,order_id', 20);

-- Insert Transformation Rules for silver.customers (Example subset to demonstrate dynamic rules)
DECLARE @CustomersTableId INT = (SELECT silver_table_id FROM control.silver_table_config WHERE target_silver_table = 'customers');

INSERT INTO control.silver_transformation_rules 
(silver_table_id, rule_name, rule_type, source_column, target_column, transformation_expression, execution_sequence)
VALUES
(@CustomersTableId, 'Cast_CustomerID', 'CAST', 'CUSTOMER_ID', 'customer_id', 'BIGINT', 10),
(@CustomersTableId, 'Cleanse_Email', 'TRANSFORM', 'EMAIL', 'email', 'LOWER(TRIM(EMAIL))', 20),
(@CustomersTableId, 'Cleanse_Status', 'TRANSFORM', 'STATUS', 'status', 'UPPER(TRIM(STATUS))', 30),
(@CustomersTableId, 'Cast_RegDate', 'CAST', 'REGISTRATION_DATE', 'registration_date', 'TIMESTAMP', 40);

-- Insert Transformation Rules for silver.oe_order_headers_all
DECLARE @OrdersTableId INT = (SELECT silver_table_id FROM control.silver_table_config WHERE target_silver_table = 'oe_order_headers_all');

INSERT INTO control.silver_transformation_rules 
(silver_table_id, rule_name, rule_type, source_column, target_column, transformation_expression, execution_sequence)
VALUES
(@OrdersTableId, 'Cast_OrderID', 'CAST', 'ORDER_ID', 'order_id', 'BIGINT', 10),
(@OrdersTableId, 'Cast_TotalAmount', 'CAST', 'TOTAL_AMOUNT', 'total_amount', 'DECIMAL(15,2)', 20),
(@OrdersTableId, 'Cleanse_OrderStatus', 'TRANSFORM', 'ORDER_STATUS', 'order_status', 'UPPER(TRIM(ORDER_STATUS))', 30),
(@OrdersTableId, 'Derive_YearMonth', 'TRANSFORM', 'ORDER_DATE', '_order_year_month', 'DATE_FORMAT(CAST(ORDER_DATE AS TIMESTAMP), ''yyyy-MM'')', 40);