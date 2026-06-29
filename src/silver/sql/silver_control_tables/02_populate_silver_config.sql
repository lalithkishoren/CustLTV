-- Clear existing configurations for idempotency during deployment
DELETE FROM [control].[silver_dq_rules];
DELETE FROM [control].[silver_transformation_rules];
DELETE FROM [control].[silver_table_config];

-- 1. Populate Table Configurations
INSERT INTO [control].[silver_table_config] 
(source_system, schema_name, source_bronze_table, target_silver_table, primary_key_columns, scd_type, track_history_columns, partition_columns, z_order_columns)
VALUES 
('SRC-002', 'crm', 'customers', 'customers', 'customer_id', 2, 'status,customer_type,marketing_opt_in', NULL, 'customer_id'),
('SRC-001', 'erp', 'oe_order_headers_all', 'oe_order_headers_all', 'order_id', 1, NULL, '_order_year_month', 'customer_id'),
('SRC-001', 'erp', 'oe_order_lines_all', 'oe_order_lines_all', 'line_id', 1, NULL, '_order_year_month', 'order_id,product_id'),
('SRC-003', 'marketing', 'marketing_campaigns', 'marketing_campaigns', 'campaign_id', 1, NULL, NULL, 'campaign_id,channel');

-- 2. Populate Transformation Rules (Sample for customers)
DECLARE @CustomersTableId INT = (SELECT silver_table_id FROM [control].[silver_table_config] WHERE target_silver_table = 'customers');

INSERT INTO [control].[silver_transformation_rules] 
(silver_table_id, rule_sequence, rule_type, source_column, target_column, transformation_expression)
VALUES 
(@CustomersTableId, 1, 'CAST', 'CUSTOMER_ID', 'customer_id', 'BIGINT'),
(@CustomersTableId, 2, 'EXPRESSION', 'EMAIL', 'email', 'LOWER(TRIM(EMAIL))'),
(@CustomersTableId, 3, 'EXPRESSION', 'FIRST_NAME', 'first_name', 'TRIM(FIRST_NAME)'),
(@CustomersTableId, 4, 'EXPRESSION', 'LAST_NAME', 'last_name', 'TRIM(LAST_NAME)'),
(@CustomersTableId, 5, 'EXPRESSION', 'STATUS', 'status', 'UPPER(TRIM(STATUS))'),
(@CustomersTableId, 6, 'CAST', 'REGISTRATION_DATE', 'registration_date', 'TIMESTAMP');

-- 3. Populate Data Quality Rules
DECLARE @OrdersTableId INT = (SELECT silver_table_id FROM [control].[silver_table_config] WHERE target_silver_table = 'oe_order_headers_all');

INSERT INTO [control].[silver_dq_rules] 
(rule_id, silver_table_id, column_name, rule_type, rule_expression, severity)
VALUES 
('DQ-N-001', @CustomersTableId, 'customer_id', 'NULL_CHECK', 'customer_id IS NOT NULL', 'ERROR'),
('DQ-F-001', @CustomersTableId, 'email', 'FORMAT_CHECK', 'email RLIKE ''^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$''', 'WARNING'),
('DQ-V-001', @OrdersTableId, 'total_amount', 'RANGE_CHECK', 'total_amount > 0', 'ERROR'),
('DQ-N-005', @OrdersTableId, 'order_id', 'NULL_CHECK', 'order_id IS NOT NULL', 'ERROR');