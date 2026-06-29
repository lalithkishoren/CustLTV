-- Insert Source Systems
INSERT INTO control.source_systems (source_system_name, source_system_type, is_active)
VALUES 
    ('SRC-001', 'Oracle ERP', 1),
    ('SRC-002', 'Oracle CRM', 1),
    ('SRC-003', 'Marketing Platform', 1);
GO

-- Insert Table Metadata
DECLARE @SRC_001 INT = (SELECT source_system_id FROM control.source_systems WHERE source_system_name = 'SRC-001');
DECLARE @SRC_002 INT = (SELECT source_system_id FROM control.source_systems WHERE source_system_name = 'SRC-002');
DECLARE @SRC_003 INT = (SELECT source_system_id FROM control.source_systems WHERE source_system_name = 'SRC-003');

INSERT INTO control.table_metadata (source_system_id, schema_name, table_name, primary_key_columns, load_type, initial_load_completed, bronze_path, load_priority)
VALUES
    -- CRM Entities (SRC-002)
    (@SRC_002, 'CRM', 'CUSTOMERS', 'CUSTOMER_ID', 'WATERMARK', 0, 'bronze/src-002/crm/customers/', 10),
    (@SRC_002, 'CRM', 'CUSTOMER_REGISTRATION_SOURCE', 'REGISTRATION_SOURCE_ID', 'WATERMARK', 0, 'bronze/src-002/crm/customer_registration_source/', 20),
    (@SRC_002, 'CRM', 'INCIDENTS', 'INCIDENT_ID', 'WATERMARK', 0, 'bronze/src-002/crm/incidents/', 30),
    (@SRC_002, 'CRM', 'INTERACTIONS', 'INTERACTION_ID', 'WATERMARK', 0, 'bronze/src-002/crm/interactions/', 40),
    (@SRC_002, 'CRM', 'SURVEYS', 'SURVEY_ID', 'WATERMARK', 0, 'bronze/src-002/crm/surveys/', 50),
    
    -- ERP Entities (SRC-001)
    (@SRC_001, 'ERP', 'OE_ORDER_HEADERS_ALL', 'ORDER_ID', 'WATERMARK', 0, 'bronze/src-001/erp/oe_order_headers_all/', 10),
    (@SRC_001, 'ERP', 'OE_ORDER_LINES_ALL', 'LINE_ID', 'WATERMARK', 0, 'bronze/src-001/erp/oe_order_lines_all/', 20),
    (@SRC_001, 'ERP', 'ADDRESSES', 'ADDRESS_ID', 'WATERMARK', 0, 'bronze/src-001/erp/addresses/', 30),
    (@SRC_001, 'ERP', 'CITY_TIER_MASTER', 'CITY,STATE', 'FULL', 0, 'bronze/src-001/erp/city_tier_master/', 40),
    (@SRC_001, 'ERP', 'MTL_SYSTEM_ITEMS_B', 'INVENTORY_ITEM_ID', 'WATERMARK', 0, 'bronze/src-001/erp/mtl_system_items_b/', 30),
    (@SRC_001, 'ERP', 'CATEGORIES', 'CATEGORY_ID', 'FULL', 0, 'bronze/src-001/erp/categories/', 40),
    (@SRC_001, 'ERP', 'BRANDS', 'BRAND_ID', 'FULL', 0, 'bronze/src-001/erp/brands/', 40),
    
    -- Marketing Entities (SRC-003)
    (@SRC_003, 'MARKETING', 'MARKETING_CAMPAIGNS', 'CAMPAIGN_ID', 'FULL', 0, 'bronze/src-003/marketing/marketing_campaigns/', 50);
GO

-- Insert Load Dependencies (Example based on FKs)
DECLARE @CustID INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'CUSTOMERS');
DECLARE @RegID INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'CUSTOMER_REGISTRATION_SOURCE');
DECLARE @OrdHdrID INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'OE_ORDER_HEADERS_ALL');
DECLARE @OrdLinID INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'OE_ORDER_LINES_ALL');

INSERT INTO control.load_dependencies (table_id, depends_on_table_id, dependency_type)
VALUES
    (@RegID, @CustID, 'HARD'),
    (@OrdHdrID, @CustID, 'HARD'),
    (@OrdLinID, @OrdHdrID, 'HARD');
GO