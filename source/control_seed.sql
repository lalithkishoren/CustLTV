-- Phase 5: register the 3 synthetic sources + 13 ingestion tables (metadata-driven framework)
DELETE FROM control.table_metadata;
DELETE FROM control.source_systems;
SET IDENTITY_INSERT control.source_systems ON;
INSERT INTO control.source_systems (source_system_id, source_system_name, source_system_type) VALUES
 (1,'Oracle Fusion ERP','AzureSQL'),(2,'Oracle Service Cloud CRM','AzureSQL'),(3,'Marketing Platform','AzureSQL');
SET IDENTITY_INSERT control.source_systems OFF;
INSERT INTO control.table_metadata (source_system_id,schema_name,table_name,primary_key_columns,load_type,bronze_path,load_priority) VALUES
 (1,'ERP','CATEGORIES','CATEGORY_ID','FULL','erp/categories',10),
 (1,'ERP','BRANDS','BRAND_ID','FULL','erp/brands',10),
 (1,'ERP','CITY_TIER_MASTER','CITY_TIER_ID','FULL','erp/city_tier_master',10),
 (1,'ERP','MTL_SYSTEM_ITEMS_B','ITEM_ID','FULL','erp/mtl_system_items_b',20),
 (1,'ERP','ADDRESSES','ADDRESS_ID','FULL','erp/addresses',20),
 (1,'ERP','OE_ORDER_HEADERS_ALL','ORDER_ID','FULL','erp/oe_order_headers_all',30),
 (1,'ERP','OE_ORDER_LINES_ALL','ORDER_LINE_ID','FULL','erp/oe_order_lines_all',30),
 (2,'CRM','CUSTOMERS','CUSTOMER_ID','FULL','crm/customers',10),
 (2,'CRM','CUSTOMER_REGISTRATION_SOURCE','REGISTRATION_SOURCE_ID','FULL','crm/customer_registration_source',20),
 (2,'CRM','INCIDENTS','INCIDENT_ID','FULL','crm/incidents',30),
 (2,'CRM','INTERACTIONS','INTERACTION_ID','FULL','crm/interactions',30),
 (2,'CRM','SURVEYS','SURVEY_ID','FULL','crm/surveys',30),
 (3,'MARKETING','MARKETING_CAMPAIGNS','CAMPAIGN_ID','FULL','marketing/marketing_campaigns',10);
SELECT 'source_systems',COUNT(*) FROM control.source_systems;
SELECT 'table_metadata',COUNT(*) FROM control.table_metadata;
