-- ============================================================================
-- Populate Control Tables with Metadata
-- ============================================================================

-- 1. Insert Source Systems
INSERT INTO control.source_systems (source_system_name, source_system_type, is_active)
VALUES 
    ('SRC-001', 'Oracle ERP', 1),
    ('SRC-002', 'Oracle CRM', 1),
    ('SRC-003', 'Marketing Platform', 1);
GO

-- 2. Insert Table Metadata
DECLARE @Src001 INT = (SELECT source_system_id FROM control.source_systems WHERE source_system_name = 'SRC-001');
DECLARE @Src002 INT = (SELECT source_system_id FROM control.source_systems WHERE source_system_name = 'SRC-002');
DECLARE @Src003 INT = (SELECT source_system_id FROM control.source_systems WHERE source_system_name = 'SRC-003');

INSERT INTO control.table_metadata 
(source_system_id, schema_name, table_name, primary_key_columns, load_type, is_active, initial_load_completed, bronze_path, load_priority)
VALUES
    -- CRM Entities (SRC-002)
    (@Src002, 'CRM', 'CUSTOMERS', 'CUSTOMER_ID', 'WATERMARK', 1, 0, 'bronze/src-002/crm/customers/', 10),
    (@Src002, 'CRM', 'CUSTOMER_REGISTRATION_SOURCE', 'REGISTRATION_SOURCE_ID', 'WATERMARK', 1, 0, 'bronze/src-002/crm/customer_registration_source/', 20),
    (@Src002, 'CRM', 'INCIDENTS', 'INCIDENT_ID', 'WATERMARK', 1, 0, 'bronze/src-002/crm/incidents/', 30),
    (@Src002, 'CRM', 'INTERACTIONS', 'INTERACTION_ID', 'WATERMARK', 1, 0, 'bronze/src-002/crm/interactions/', 40),
    (@Src002, 'CRM', 'SURVEYS', 'SURVEY_ID', 'WATERMARK', 1, 0, 'bronze/src-002/crm/surveys/', 50),
    
    -- ERP Entities (SRC-001)
    (@Src001, 'ERP', 'OE_ORDER_HEADERS_ALL', 'ORDER_ID', 'WATERMARK', 1, 0, 'bronze/src-001/erp/oe_order_headers_all/', 10),
    (@Src001, 'ERP', 'OE_ORDER_LINES_ALL', 'LINE_ID', 'WATERMARK', 1, 0, 'bronze/src-001/erp/oe_order_lines_all/', 20),
    (@Src001, 'ERP', 'ADDRESSES', 'ADDRESS_ID', 'WATERMARK', 1, 0, 'bronze/src-001/erp/addresses/', 30),
    (@Src001, 'ERP', 'CITY_TIER_MASTER', 'CITY,STATE', 'FULL', 1, 0, 'bronze/src-001/erp/city_tier_master/', 40),
    (@Src001, 'ERP', 'MTL_SYSTEM_ITEMS_B', 'INVENTORY_ITEM_ID', 'WATERMARK', 1, 0, 'bronze/src-001/erp/mtl_system_items_b/', 30),
    (@Src001, 'ERP', 'CATEGORIES', 'CATEGORY_ID', 'FULL', 1, 0, 'bronze/src-001/erp/categories/', 40),
    (@Src001, 'ERP', 'BRANDS', 'BRAND_ID', 'FULL', 1, 0, 'bronze/src-001/erp/brands/', 40),
    
    -- Marketing Entities (SRC-003)
    (@Src003, 'MARKETING', 'MARKETING_CAMPAIGNS', 'CAMPAIGN_ID', 'FULL', 1, 0, 'bronze/src-003/marketing/marketing_campaigns/', 50);
GO

-- 3. Insert Load Dependencies
DECLARE @Tbl_Customers INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'CUSTOMERS');
DECLARE @Tbl_Registration INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'CUSTOMER_REGISTRATION_SOURCE');
DECLARE @Tbl_OrderHeaders INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'OE_ORDER_HEADERS_ALL');
DECLARE @Tbl_OrderLines INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'OE_ORDER_LINES_ALL');
DECLARE @Tbl_Addresses INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'ADDRESSES');
DECLARE @Tbl_CityTier INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'CITY_TIER_MASTER');
DECLARE @Tbl_Items INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'MTL_SYSTEM_ITEMS_B');
DECLARE @Tbl_Categories INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'CATEGORIES');
DECLARE @Tbl_Brands INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'BRANDS');
DECLARE @Tbl_Campaigns INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'MARKETING_CAMPAIGNS');
DECLARE @Tbl_Incidents INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'INCIDENTS');
DECLARE @Tbl_Interactions INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'INTERACTIONS');
DECLARE @Tbl_Surveys INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'SURVEYS');

INSERT INTO control.load_dependencies (table_id, depends_on_table_id, dependency_type)
VALUES
    (@Tbl_Registration, @Tbl_Customers, 'HARD'),
    (@Tbl_Registration, @Tbl_Campaigns, 'HARD'),
    (@Tbl_Addresses, @Tbl_Customers, 'HARD'),
    (@Tbl_Addresses, @Tbl_CityTier, 'HARD'),
    (@Tbl_OrderHeaders, @Tbl_Customers, 'HARD'),
    (@Tbl_OrderHeaders, @Tbl_Addresses, 'HARD'),
    (@Tbl_OrderLines, @Tbl_OrderHeaders, 'HARD'),
    (@Tbl_OrderLines, @Tbl_Items, 'HARD'),
    (@Tbl_Items, @Tbl_Categories, 'HARD'),
    (@Tbl_Items, @Tbl_Brands, 'HARD'),
    (@Tbl_Incidents, @Tbl_Customers, 'HARD'),
    (@Tbl_Incidents, @Tbl_OrderHeaders, 'SOFT'),
    (@Tbl_Interactions, @Tbl_Incidents, 'HARD'),
    (@Tbl_Surveys, @Tbl_Customers, 'HARD'),
    (@Tbl_Surveys, @Tbl_OrderHeaders, 'SOFT'),
    (@Tbl_Surveys, @Tbl_Incidents, 'SOFT');
GO