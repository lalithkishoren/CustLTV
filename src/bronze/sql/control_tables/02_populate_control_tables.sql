-- ===============================================================================
-- Populate Control Tables from Metadata Document
-- ===============================================================================

-- 1. Insert Source Systems
INSERT INTO control.source_systems (source_system_name, source_system_type, is_active)
VALUES 
    ('SRC-001', 'Oracle ERP', 1),
    ('SRC-002', 'Oracle CRM', 1),
    ('SRC-003', 'Marketing Platform', 1);
GO

-- 2. Insert Table Metadata
DECLARE @SRC001 INT = (SELECT source_system_id FROM control.source_systems WHERE source_system_name = 'SRC-001');
DECLARE @SRC002 INT = (SELECT source_system_id FROM control.source_systems WHERE source_system_name = 'SRC-002');
DECLARE @SRC003 INT = (SELECT source_system_id FROM control.source_systems WHERE source_system_name = 'SRC-003');

INSERT INTO control.table_metadata (source_system_id, schema_name, table_name, primary_key_columns, load_type, is_active, initial_load_completed, bronze_path, load_priority)
VALUES
    -- SRC-001 (ERP)
    (@SRC001, 'ERP', 'OE_ORDER_HEADERS_ALL', 'ORDER_ID', 'WATERMARK', 1, 0, 'bronze/src-001/erp/oe_order_headers_all/', 10),
    (@SRC001, 'ERP', 'OE_ORDER_LINES_ALL', 'LINE_ID', 'WATERMARK', 1, 0, 'bronze/src-001/erp/oe_order_lines_all/', 20),
    (@SRC001, 'ERP', 'ADDRESSES', 'ADDRESS_ID', 'WATERMARK', 1, 0, 'bronze/src-001/erp/addresses/', 30),
    (@SRC001, 'ERP', 'CITY_TIER_MASTER', 'CITY,STATE', 'FULL', 1, 0, 'bronze/src-001/erp/city_tier_master/', 40),
    (@SRC001, 'ERP', 'MTL_SYSTEM_ITEMS_B', 'INVENTORY_ITEM_ID', 'WATERMARK', 1, 0, 'bronze/src-001/erp/mtl_system_items_b/', 30),
    (@SRC001, 'ERP', 'CATEGORIES', 'CATEGORY_ID', 'FULL', 1, 0, 'bronze/src-001/erp/categories/', 40),
    (@SRC001, 'ERP', 'BRANDS', 'BRAND_ID', 'FULL', 1, 0, 'bronze/src-001/erp/brands/', 40),
    
    -- SRC-002 (CRM)
    (@SRC002, 'CRM', 'CUSTOMERS', 'CUSTOMER_ID', 'WATERMARK', 1, 0, 'bronze/src-002/crm/customers/', 10),
    (@SRC002, 'CRM', 'CUSTOMER_REGISTRATION_SOURCE', 'REGISTRATION_SOURCE_ID', 'WATERMARK', 1, 0, 'bronze/src-002/crm/customer_registration_source/', 20),
    (@SRC002, 'CRM', 'INCIDENTS', 'INCIDENT_ID', 'WATERMARK', 1, 0, 'bronze/src-002/crm/incidents/', 30),
    (@SRC002, 'CRM', 'INTERACTIONS', 'INTERACTION_ID', 'WATERMARK', 1, 0, 'bronze/src-002/crm/interactions/', 40),
    (@SRC002, 'CRM', 'SURVEYS', 'SURVEY_ID', 'WATERMARK', 1, 0, 'bronze/src-002/crm/surveys/', 50),
    
    -- SRC-003 (Marketing)
    (@SRC003, 'MARKETING', 'MARKETING_CAMPAIGNS', 'CAMPAIGN_ID', 'FULL', 1, 0, 'bronze/src-003/marketing/marketing_campaigns/', 50);
GO

-- 3. Insert Load Dependencies
INSERT INTO control.load_dependencies (table_id, depends_on_table_id, dependency_type)
SELECT t1.table_id, t2.table_id, 'FK'
FROM control.table_metadata t1
JOIN control.table_metadata t2 ON 1=1
WHERE 
    (t1.table_name = 'OE_ORDER_LINES_ALL' AND t2.table_name = 'OE_ORDER_HEADERS_ALL') OR
    (t1.table_name = 'OE_ORDER_LINES_ALL' AND t2.table_name = 'MTL_SYSTEM_ITEMS_B') OR
    (t1.table_name = 'INCIDENTS' AND t2.table_name = 'CUSTOMERS') OR
    (t1.table_name = 'INCIDENTS' AND t2.table_name = 'OE_ORDER_HEADERS_ALL') OR
    (t1.table_name = 'SURVEYS' AND t2.table_name = 'CUSTOMERS') OR
    (t1.table_name = 'SURVEYS' AND t2.table_name = 'OE_ORDER_HEADERS_ALL') OR
    (t1.table_name = 'SURVEYS' AND t2.table_name = 'INCIDENTS') OR
    (t1.table_name = 'CUSTOMER_REGISTRATION_SOURCE' AND t2.table_name = 'CUSTOMERS') OR
    (t1.table_name = 'CUSTOMER_REGISTRATION_SOURCE' AND t2.table_name = 'MARKETING_CAMPAIGNS') OR
    (t1.table_name = 'ADDRESSES' AND t2.table_name = 'CUSTOMERS') OR
    (t1.table_name = 'ADDRESSES' AND t2.table_name = 'CITY_TIER_MASTER') OR
    (t1.table_name = 'MTL_SYSTEM_ITEMS_B' AND t2.table_name = 'CATEGORIES') OR
    (t1.table_name = 'MTL_SYSTEM_ITEMS_B' AND t2.table_name = 'BRANDS');
GO