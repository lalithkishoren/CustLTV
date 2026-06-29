# Bronze Layer Low-Level Design Specification

## 1. Executive Summary

### 1.1 Solution Overview
- **Business Problem**: Myntra requires a highly scalable, automated Customer Lifetime Value (CLV) analytics solution to predict future customer revenue, identify high-value RFM segments, optimize Customer Acquisition Cost (CAC), and proactively reduce churn.
- **High-Level Architecture**:
```text
[Oracle ERP] ──┐                                                               ┌── [Silver Layer]
[Oracle CRM] ──┼──► [Azure Data Factory] ──► [ADLS Gen2 (Bronze)] ──► [DLT] ───┤
[Marketing] ───┘           │                          │                        └── [Gold Layer]
                           ▼                          ▼
                  [Azure SQL Control DB]       [Unity Catalog]
```
- **Key Benefits**:
  - Exact source fidelity preservation in the Bronze layer for audit and replayability.
  - Idempotent, metadata-driven ingestion supporting both Initial Full Load and Incremental CDC.
  - Automated lineage tracking via `pipeline_run_id` and `ingest_timestamp`.
  - Progressive trust Medallion architecture ensuring data quality at scale.

### 1.2 Scope
- **In-Scope Components**:
  - Azure Data Factory (ADF) orchestration pipelines.
  - Azure SQL Database for control metadata.
  - ADLS Gen2 Bronze layer storage (Delta Lake format).
  - Ingestion of 13 identified entities across Oracle ERP, Oracle CRM, and Marketing platforms.
- **Out-of-Scope Items**:
  - Silver layer transformations and data quality rules (covered in Silver LLD).
  - Gold layer aggregations and Power BI semantic models.
  - Machine Learning predictive models for CLV.
- **Assumptions and Constraints**:
  - Source systems support Change Data Capture (CDC) via Debezium/Change Tracking or reliable `LAST_UPDATE_DATE` watermarks.
  - Network connectivity via Azure Private Link is established.

### 1.3 Technology Stack
- **Orchestration**: Azure Data Factory (ADF) - Serverless
- **Compute**: Azure Databricks - Runtime 14.3 LTS or higher
- **Storage**: Azure Data Lake Storage (ADLS) Gen2 - Zone-Redundant Storage (ZRS)
- **Metadata/Control**: Azure SQL Database - Serverless
- **Security**: Azure Key Vault, Microsoft Entra ID
- **Governance**: Unity Catalog

---

## 2. Architecture Design

### 2.1 High-Level Architecture
```text
Source Systems          Orchestration & Control           Bronze Layer (Raw)             Processing
--------------          -----------------------           ------------------             ----------
 
[SRC-001: ERP] ─(JDBC)─┐                                  [ADLS Gen2]
                       │                                  Container: bronze
[SRC-002: CRM] ─(JDBC)─┼─► [ADF Master Pipeline] ───────► /source_system/schema/table/ ──► [Databricks]
                       │          │                       /ingest_date=YYYY-MM-DD/
[SRC-003: MKT] ─(REST)─┘          ▼                               │
                          [Azure SQL Control DB]                  │
                          - source_systems                        │
                          - table_metadata                        │
                          - pipeline_execution_log                ▼
                                                          (Delta Lake Format)
```

### 2.2 Component Specifications
- **Component: Azure Data Factory (ADF)**
  - **Purpose**: Master orchestrator for data movement and pipeline triggering.
  - **Inputs**: Source system credentials (from Key Vault), Control DB metadata.
  - **Outputs**: Raw Parquet files in ADLS Gen2 (landing zone), triggered Databricks jobs.
  - **Dependencies**: Azure Key Vault, Azure SQL DB, ADLS Gen2, Databricks Workspace.
  - **Configuration**: Managed Identity authentication, parallel copy activities, dynamic batch sizing.
- **Component: Azure SQL Control Database**
  - **Purpose**: State management, watermark tracking, and pipeline metadata storage.
  - **Inputs**: ADF pipeline execution statuses, Databricks row counts.
  - **Outputs**: Active table lists, `last_sync_version`, `initial_load_completed` flags.
  - **Dependencies**: None.
  - **Configuration**: Serverless compute, auto-pause enabled, IP firewall restricted to Azure services.
- **Component: ADLS Gen2 (Bronze Storage)**
  - **Purpose**: Immutable, raw fidelity storage of source data.
  - **Inputs**: ADF Copy Activity outputs.
  - **Outputs**: Delta Lake tables read by Databricks DLT.
  - **Dependencies**: Unity Catalog external locations.
  - **Configuration**: Hierarchical namespace enabled, ZRS, lifecycle management (Cool tier after 30 days).

### 2.3 Data Flow Architecture
- **Initial Load Flow**:
  - ADF queries `control.table_metadata` where `initial_load_completed = 0`.
  - ADF extracts full table from source and lands as Parquet in ADLS Gen2.
  - Databricks reads Parquet, appends audit columns (`_pipeline_run_id`, `_ingest_timestamp`), and writes to Bronze Delta table using partition overwrite on `ingest_date`.
  - ADF updates `initial_load_completed = 1` and sets initial `last_sync_version`.
- **Incremental CDC Flow**:
  - ADF queries `control.table_metadata` where `initial_load_completed = 1`.
  - ADF extracts changes using `last_sync_version` (watermark or CDC LSN).
  - Databricks reads changes, appends audit columns, and performs a targeted `MERGE INTO` the Bronze Delta table based on primary keys.
  - ADF updates `last_sync_version` upon success.

---

## 3. Control Database Design

### 3.1 Table: control.source_systems
- **Schema Definition**:
  - **Column: source_system_id**
    - Data Type: INT
    - Constraints: PRIMARY KEY, IDENTITY(1,1)
  - **Column: source_system_name**
    - Data Type: VARCHAR(100)
    - Constraints: NOT NULL, UNIQUE
  - **Column: source_system_type**
    - Data Type: VARCHAR(50)
    - Constraints: NOT NULL (e.g., 'Oracle ERP', 'Oracle CRM', 'REST API')
  - **Column: is_active**
    - Data Type: BIT
    - Constraints: NOT NULL, DEFAULT 1
  - **Column: created_date**
    - Data Type: DATETIME2
    - Constraints: NOT NULL, DEFAULT GETUTCDATE()

### 3.2 Table: control.table_metadata
- **Schema Definition**:
  - **Column: table_id**
    - Data Type: INT
    - Constraints: PRIMARY KEY, IDENTITY(1,1)
  - **Column: source_system_id**
    - Data Type: INT
    - Constraints: FOREIGN KEY references control.source_systems(source_system_id)
  - **Column: schema_name**
    - Data Type: VARCHAR(100)
    - Constraints: NOT NULL
  - **Column: table_name**
    - Data Type: VARCHAR(100)
    - Constraints: NOT NULL
  - **Column: primary_key_columns**
    - Data Type: VARCHAR(255)
    - Constraints: NOT NULL (Comma-separated for composite keys)
  - **Column: load_type**
    - Data Type: VARCHAR(20)
    - Constraints: NOT NULL (Values: 'FULL', 'CDC', 'WATERMARK')
  - **Column: is_active**
    - Data Type: BIT
    - Constraints: NOT NULL, DEFAULT 1
  - **Column: initial_load_completed**
    - Data Type: BIT
    - Constraints: NOT NULL, DEFAULT 0
  - **Column: last_sync_version**
    - Data Type: VARCHAR(255)
    - Constraints: NULL (Stores LSN, Timestamp, or ID)
  - **Column: last_load_status**
    - Data Type: VARCHAR(50)
    - Constraints: NULL
  - **Column: last_load_timestamp**
    - Data Type: DATETIME2
    - Constraints: NULL
  - **Column: last_pipeline_run_id**
    - Data Type: UNIQUEIDENTIFIER
    - Constraints: NULL
  - **Column: records_loaded**
    - Data Type: BIGINT
    - Constraints: NULL, DEFAULT 0
  - **Column: bronze_path**
    - Data Type: VARCHAR(500)
    - Constraints: NOT NULL
  - **Column: load_priority**
    - Data Type: INT
    - Constraints: NOT NULL, DEFAULT 100
  - **Column: created_date**
    - Data Type: DATETIME2
    - Constraints: NOT NULL, DEFAULT GETUTCDATE()
  - **Column: modified_date**
    - Data Type: DATETIME2
    - Constraints: NOT NULL, DEFAULT GETUTCDATE()

### 3.3 Table: control.pipeline_execution_log
- **Schema Definition**:
  - **Column: log_id**
    - Data Type: BIGINT
    - Constraints: PRIMARY KEY, IDENTITY(1,1)
  - **Column: pipeline_run_id**
    - Data Type: UNIQUEIDENTIFIER
    - Constraints: NOT NULL
  - **Column: table_id**
    - Data Type: INT
    - Constraints: FOREIGN KEY references control.table_metadata(table_id)
  - **Column: execution_start_time**
    - Data Type: DATETIME2
    - Constraints: NOT NULL
  - **Column: execution_end_time**
    - Data Type: DATETIME2
    - Constraints: NULL
  - **Column: status**
    - Data Type: VARCHAR(50)
    - Constraints: NOT NULL (Values: 'RUNNING', 'SUCCESS', 'FAILED')
  - **Column: rows_read**
    - Data Type: BIGINT
    - Constraints: NULL
  - **Column: rows_written**
    - Data Type: BIGINT
    - Constraints: NULL
  - **Column: error_message**
    - Data Type: VARCHAR(MAX)
    - Constraints: NULL

### 3.4 Table: control.data_quality_rules
- **Schema Definition**:
  - **Column: rule_id**
    - Data Type: INT
    - Constraints: PRIMARY KEY, IDENTITY(1,1)
  - **Column: table_id**
    - Data Type: INT
    - Constraints: FOREIGN KEY references control.table_metadata(table_id)
  - **Column: column_name**
    - Data Type: VARCHAR(100)
    - Constraints: NOT NULL
  - **Column: rule_type**
    - Data Type: VARCHAR(50)
    - Constraints: NOT NULL (Values: 'NOT_NULL', 'UNIQUE', 'RANGE', 'REGEX')
  - **Column: rule_definition**
    - Data Type: VARCHAR(MAX)
    - Constraints: NOT NULL
  - **Column: action_on_failure**
    - Data Type: VARCHAR(50)
    - Constraints: NOT NULL (Values: 'WARN', 'DROP', 'FAIL')

---

## 4. CDC Design Specifications

### 4.1 CDC Method Selection
- **Oracle ERP (SRC-001)**:
  - CDC Method: Query-based Watermark (due to Oracle JDBC constraints without GoldenGate).
  - Version/Watermark Column: `LAST_UPDATE_DATE`
  - Operation Tracking: Upsert (I/U). Hard deletes are not captured via watermark; requires periodic reconciliation.
- **Oracle CRM (SRC-002)**:
  - CDC Method: Query-based Watermark.
  - Version/Watermark Column: `LAST_UPDATE_DATE`
  - Operation Tracking: Upsert (I/U).
- **Marketing Platform (SRC-003)**:
  - CDC Method: Full Refresh / Batch API Pull.
  - Version/Watermark Column: N/A (Daily snapshot).
  - Operation Tracking: Overwrite.

### 4.2 CDC Metadata Columns
- **Query-based Method (Watermark)**:
  - Version Column: `_watermark_value` (Mapped to `LAST_UPDATE_DATE`)
  - Operation Column: `_cdc_operation` (Defaulted to 'U' for incremental, 'I' for initial)
  - Timestamp Column: `_ingest_timestamp` (Platform processing time)

### 4.3 CDC Processing Logic
- **sp_GetWatermarkQuery Specification**:
  - Parameters: `@TableId INT`
  - Returns: Dynamically generated SQL string.
  - Logic: `SELECT * FROM [schema].[table] WHERE [watermark_column] > '[last_sync_version]' AND [watermark_column] <= CURRENT_TIMESTAMP`
- **sp_UpdateTableMetadata Specification**:
  - Parameters: `@TableId INT`, `@Status VARCHAR(50)`, `@PipelineRunId UNIQUEIDENTIFIER`, `@RecordsLoaded BIGINT`, `@SyncVersion VARCHAR(255)`, `@MarkInitialLoadComplete BIT`
  - Logic:
    - Update `last_load_status`, `last_pipeline_run_id`, `records_loaded`, `last_load_timestamp`.
    - If `@Status = 'SUCCESS'`, update `last_sync_version = @SyncVersion`.
    - If `@MarkInitialLoadComplete = 1 AND @Status = 'SUCCESS'`, set `initial_load_completed = 1`.

---

## 5. Pipeline Architecture Specifications

### 5.1 Master Orchestrator Pipeline
- **Name**: `PL_Master_Orchestrator`
- **Parameters**: `pipeline_run_id` (System variable: `@pipeline().RunId`)
- **Flow**:
  - **Lookup_ActiveTables**: Execute `SELECT * FROM control.table_metadata WHERE is_active = 1 ORDER BY load_priority ASC`.
  - **ForEach_Table**: Iterate over the JSON array returned by the lookup.
  - **IfCondition_LoadType**: Evaluate `@equals(item().initial_load_completed, 0)`.
    - **TRUE Path**: Execute pipeline `PL_Initial_Load_Single_Table`.
    - **FALSE Path**: Execute pipeline `PL_Incremental_CDC_Single_Table`.

### 5.2 Initial Load Pipeline
- **Name**: `PL_Initial_Load_Single_Table`
- **Parameters**: `pipeline_run_id`, `table_id`, `schema_name`, `table_name`, `primary_key_columns`
- **Activities**:
  - **Copy_FullTable_ToLanding**: Extract `SELECT * FROM schema.table` to ADLS Gen2 Parquet.
  - **Databricks_Process_InitialLoad**: Execute Databricks notebook to read Parquet, add `_pipeline_run_id`, `_ingest_timestamp`, `_source_system`, and write to Bronze Delta table partitioned by `ingest_date`.
  - **SP_UpdateMetadata**: Execute `sp_UpdateTableMetadata` with `@MarkInitialLoadComplete = 1` and `@SyncVersion = MAX(LAST_UPDATE_DATE)`.
- **Bronze Path**: `bronze/{source_system}/{schema_name}/{table_name}/ingest_date={YYYY-MM-DD}/`

### 5.3 Incremental CDC Pipeline
- **Name**: `PL_Incremental_CDC_Single_Table`
- **Parameters**: `pipeline_run_id`, `table_id`, `schema_name`, `table_name`, `primary_key_columns`, `last_sync_version`
- **Activities**:
  - **Lookup_Watermark**: Get current max watermark from source.
  - **Copy_Incremental_ToLanding**: Extract data using `sp_GetWatermarkQuery` to ADLS Gen2 Parquet.
  - **IfCondition_HasData**: Check if `rowsCopied > 0`.
    - **TRUE Path**:
      - **Databricks_Process_Merge**: Execute Databricks notebook to perform `MERGE INTO` Bronze Delta table using `primary_key_columns`.
      - **SP_UpdateMetadata**: Execute `sp_UpdateTableMetadata` with new watermark.
    - **FALSE Path**: Log successful run with 0 rows.
- **Bronze Path**: `bronze/{source_system}/{schema_name}/{table_name}/ingest_date={YYYY-MM-DD}/`

---

## 6. Entity Specifications

### Entity: CRM.CUSTOMERS
- **Source Details**:
  - Source System ID: SRC-002 (Oracle CRM)
  - Schema Name: CRM
  - Table Name: CUSTOMERS
  - Primary Key(s): CUSTOMER_ID
  - Load Type: WATERMARK
  - Load Priority: 10
- **Column Specifications**:
  - **Column: CUSTOMER_ID**
    - Data Type: BIGINT
    - Nullable: NO
    - Primary Key: YES
    - Foreign Key: NONE
    - Description: Unique system-generated customer identifier
  - **Column: EMAIL**
    - Data Type: NVARCHAR(255)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Primary email address
  - **Column: PHONE**
    - Data Type: NVARCHAR(20)
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Primary contact phone
  - **Column: FIRST_NAME**
    - Data Type: NVARCHAR(100)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Customer's given name
  - **Column: LAST_NAME**
    - Data Type: NVARCHAR(100)
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Customer's family name
  - **Column: GENDER**
    - Data Type: NVARCHAR(10)
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Customer's gender identity
  - **Column: DATE_OF_BIRTH**
    - Data Type: DATE
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Customer's birth date
  - **Column: REGISTRATION_DATE**
    - Data Type: DATETIME2
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Account creation timestamp
  - **Column: CUSTOMER_TYPE**
    - Data Type: NVARCHAR(20)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Customer classification
  - **Column: STATUS**
    - Data Type: NVARCHAR(20)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Account status
  - **Column: EMAIL_VERIFIED**
    - Data Type: BIT
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Email verification flag
  - **Column: PHONE_VERIFIED**
    - Data Type: BIT
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Phone verification flag
  - **Column: MARKETING_OPT_IN**
    - Data Type: BIT
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Marketing consent flag
  - **Column: PREFERRED_LANGUAGE**
    - Data Type: NVARCHAR(20)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Communication language preference
  - **Column: CREATED_BY**
    - Data Type: NVARCHAR(50)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Record creator
  - **Column: CREATED_DATE**
    - Data Type: DATETIME2
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Record creation timestamp
  - **Column: LAST_UPDATE_DATE**
    - Data Type: DATETIME2
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Last modification timestamp
- **CDC Configuration**:
  - Method: Watermark
  - Version/Watermark Column: LAST_UPDATE_DATE
  - Change Detection Query Pattern: `LAST_UPDATE_DATE > @last_sync_version`
- **Bronze Layer Target**:
  - Path: `bronze/src-002/crm/customers/`
  - Format: Delta Lake
  - Partitioning: By `ingest_date`
- **Metadata Columns Added**:
  - `_pipeline_run_id` (STRING): Lineage tracking
  - `_ingest_timestamp` (TIMESTAMP): Processing time
  - `_source_system` (STRING): Source identifier ('SRC-002')
  - `_cdc_operation` (STRING): I/U/D operation type
- **MERGE Configuration**:
  - Merge Key: CUSTOMER_ID
  - Match Condition: `target.CUSTOMER_ID = source.CUSTOMER_ID`
  - Delete Handling: Not applicable (Soft deletes handled via STATUS column)
  - Update Handling: Update all non-PK columns

### Entity: CRM.CUSTOMER_REGISTRATION_SOURCE
- **Source Details**:
  - Source System ID: SRC-002 (Oracle CRM)
  - Schema Name: CRM
  - Table Name: CUSTOMER_REGISTRATION_SOURCE
  - Primary Key(s): REGISTRATION_SOURCE_ID
  - Load Type: WATERMARK
  - Load Priority: 20
- **Column Specifications**:
  - **Column: REGISTRATION_SOURCE_ID**
    - Data Type: BIGINT
    - Nullable: NO
    - Primary Key: YES
    - Foreign Key: NONE
    - Description: Unique record identifier
  - **Column: CUSTOMER_ID**
    - Data Type: BIGINT
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: CRM.CUSTOMERS.CUSTOMER_ID
    - Description: Link to customer master
  - **Column: CHANNEL**
    - Data Type: NVARCHAR(50)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Acquisition channel
  - **Column: CAMPAIGN_ID**
    - Data Type: INT
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: MARKETING.MARKETING_CAMPAIGNS.CAMPAIGN_ID
    - Description: Associated marketing campaign
  - **Column: UTM_SOURCE**
    - Data Type: NVARCHAR(100)
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: UTM source parameter
  - **Column: UTM_MEDIUM**
    - Data Type: NVARCHAR(100)
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: UTM medium parameter
  - **Column: UTM_CAMPAIGN**
    - Data Type: NVARCHAR(200)
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: UTM campaign parameter
  - **Column: UTM_CONTENT**
    - Data Type: NVARCHAR(200)
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: UTM content parameter
  - **Column: REFERRER_URL**
    - Data Type: NVARCHAR(500)
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: HTTP referrer URL
  - **Column: LANDING_PAGE**
    - Data Type: NVARCHAR(500)
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: First page visited
  - **Column: DEVICE_TYPE**
    - Data Type: NVARCHAR(20)
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Registration device
  - **Column: REGISTRATION_DATE**
    - Data Type: DATETIME2
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Registration timestamp
  - **Column: CREATED_DATE**
    - Data Type: DATETIME2
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Record creation timestamp
- **CDC Configuration**:
  - Method: Watermark
  - Version/Watermark Column: CREATED_DATE (Append only table)
  - Change Detection Query Pattern: `CREATED_DATE > @last_sync_version`
- **Bronze Layer Target**:
  - Path: `bronze/src-002/crm/customer_registration_source/`
  - Format: Delta Lake
  - Partitioning: By `ingest_date`
- **Metadata Columns Added**:
  - `_pipeline_run_id` (STRING): Lineage tracking
  - `_ingest_timestamp` (TIMESTAMP): Processing time
  - `_source_system` (STRING): Source identifier ('SRC-002')
  - `_cdc_operation` (STRING): I/U/D operation type
- **MERGE Configuration**:
  - Merge Key: REGISTRATION_SOURCE_ID
  - Match Condition: `target.REGISTRATION_SOURCE_ID = source.REGISTRATION_SOURCE_ID`
  - Delete Handling: N/A
  - Update Handling: Update all non-PK columns

### Entity: ERP.OE_ORDER_HEADERS_ALL
- **Source Details**:
  - Source System ID: SRC-001 (Oracle ERP)
  - Schema Name: ERP
  - Table Name: OE_ORDER_HEADERS_ALL
  - Primary Key(s): ORDER_ID
  - Load Type: WATERMARK
  - Load Priority: 10
- **Column Specifications**:
  - **Column: ORDER_ID**
    - Data Type: BIGINT
    - Nullable: NO
    - Primary Key: YES
    - Foreign Key: NONE
    - Description: Unique system order identifier
  - **Column: ORDER_NUMBER**
    - Data Type: NVARCHAR(50)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Customer-facing order reference
  - **Column: CUSTOMER_ID**
    - Data Type: BIGINT
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: CRM.CUSTOMERS.CUSTOMER_ID
    - Description: Customer who placed order
  - **Column: ORDER_DATE**
    - Data Type: DATETIME2
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Order placement timestamp
  - **Column: ORDER_STATUS**
    - Data Type: NVARCHAR(30)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Current order lifecycle status
  - **Column: PAYMENT_METHOD**
    - Data Type: NVARCHAR(30)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Payment instrument used
  - **Column: PAYMENT_STATUS**
    - Data Type: NVARCHAR(20)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Payment processing status
  - **Column: SUBTOTAL_AMOUNT**
    - Data Type: DECIMAL(15,2)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Order subtotal before discounts
  - **Column: DISCOUNT_AMOUNT**
    - Data Type: DECIMAL(15,2)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Total discount applied
  - **Column: TAX_AMOUNT**
    - Data Type: DECIMAL(15,2)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Tax amount (GST)
  - **Column: SHIPPING_AMOUNT**
    - Data Type: DECIMAL(15,2)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Shipping charges
  - **Column: TOTAL_AMOUNT**
    - Data Type: DECIMAL(15,2)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Final order total
  - **Column: CURRENCY_CODE**
    - Data Type: NVARCHAR(3)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Transaction currency
  - **Column: SHIPPING_ADDRESS_ID**
    - Data Type: BIGINT
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: ERP.ADDRESSES.ADDRESS_ID
    - Description: Delivery address
  - **Column: BILLING_ADDRESS_ID**
    - Data Type: BIGINT
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: ERP.ADDRESSES.ADDRESS_ID
    - Description: Billing address
  - **Column: PROMISED_DATE**
    - Data Type: DATE
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Promised delivery date
  - **Column: SHIPPED_DATE**
    - Data Type: DATETIME2
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Actual ship timestamp
  - **Column: DELIVERED_DATE**
    - Data Type: DATETIME2
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Actual delivery timestamp
  - **Column: CANCELLATION_REASON**
    - Data Type: NVARCHAR(200)
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Reason if cancelled
  - **Column: ORDER_SOURCE**
    - Data Type: NVARCHAR(20)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Order channel
  - **Column: ORG_ID**
    - Data Type: INT
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Organization identifier
  - **Column: CREATED_BY**
    - Data Type: NVARCHAR(50)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: System/user that created
  - **Column: CREATED_DATE**
    - Data Type: DATETIME2
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Record creation timestamp
  - **Column: LAST_UPDATE_DATE**
    - Data Type: DATETIME2
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Last modification timestamp
- **CDC Configuration**:
  - Method: Watermark
  - Version/Watermark Column: LAST_UPDATE_DATE
  - Change Detection Query Pattern: `LAST_UPDATE_DATE > @last_sync_version`
- **Bronze Layer Target**:
  - Path: `bronze/src-001/erp/oe_order_headers_all/`
  - Format: Delta Lake
  - Partitioning: By `ingest_date`
- **Metadata Columns Added**:
  - `_pipeline_run_id` (STRING): Lineage tracking
  - `_ingest_timestamp` (TIMESTAMP): Processing time
  - `_source_system` (STRING): Source identifier ('SRC-001')
  - `_cdc_operation` (STRING): I/U/D operation type
- **MERGE Configuration**:
  - Merge Key: ORDER_ID
  - Match Condition: `target.ORDER_ID = source.ORDER_ID`
  - Delete Handling: N/A
  - Update Handling: Update all non-PK columns

### Entity: ERP.OE_ORDER_LINES_ALL
- **Source Details**:
  - Source System ID: SRC-001 (Oracle ERP)
  - Schema Name: ERP
  - Table Name: OE_ORDER_LINES_ALL
  - Primary Key(s): LINE_ID
  - Load Type: WATERMARK
  - Load Priority: 20
- **Column Specifications**:
  - **Column: LINE_ID**
    - Data Type: BIGINT
    - Nullable: NO
    - Primary Key: YES
    - Foreign Key: NONE
    - Description: Unique order line identifier
  - **Column: ORDER_ID**
    - Data Type: BIGINT
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: ERP.OE_ORDER_HEADERS_ALL.ORDER_ID
    - Description: Parent order identifier
  - **Column: PRODUCT_ID**
    - Data Type: BIGINT
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: ERP.MTL_SYSTEM_ITEMS_B.INVENTORY_ITEM_ID
    - Description: Product identifier
  - **Column: QUANTITY**
    - Data Type: INT
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Quantity ordered
  - **Column: UNIT_PRICE**
    - Data Type: DECIMAL(15,2)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Price per unit
  - **Column: LINE_TOTAL**
    - Data Type: DECIMAL(15,2)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Total line amount
  - **Column: CREATED_DATE**
    - Data Type: DATETIME2
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Record creation timestamp
  - **Column: LAST_UPDATE_DATE**
    - Data Type: DATETIME2
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Last modification timestamp
- **CDC Configuration**:
  - Method: Watermark
  - Version/Watermark Column: LAST_UPDATE_DATE
  - Change Detection Query Pattern: `LAST_UPDATE_DATE > @last_sync_version`
- **Bronze Layer Target**:
  - Path: `bronze/src-001/erp/oe_order_lines_all/`
  - Format: Delta Lake
  - Partitioning: By `ingest_date`
- **Metadata Columns Added**:
  - `_pipeline_run_id` (STRING): Lineage tracking
  - `_ingest_timestamp` (TIMESTAMP): Processing time
  - `_source_system` (STRING): Source identifier ('SRC-001')
  - `_cdc_operation` (STRING): I/U/D operation type
- **MERGE Configuration**:
  - Merge Key: LINE_ID
  - Match Condition: `target.LINE_ID = source.LINE_ID`
  - Delete Handling: N/A
  - Update Handling: Update all non-PK columns

### Entity: ERP.ADDRESSES
- **Source Details**:
  - Source System ID: SRC-001 (Oracle ERP)
  - Schema Name: ERP
  - Table Name: ADDRESSES
  - Primary Key(s): ADDRESS_ID
  - Load Type: WATERMARK
  - Load Priority: 30
- **Column Specifications**:
  - **Column: ADDRESS_ID**
    - Data Type: BIGINT
    - Nullable: NO
    - Primary Key: YES
    - Foreign Key: NONE
    - Description: Unique address identifier
  - **Column: CUSTOMER_ID**
    - Data Type: BIGINT
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: CRM.CUSTOMERS.CUSTOMER_ID
    - Description: Associated customer
  - **Column: ADDRESS_LINE1**
    - Data Type: NVARCHAR(255)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Primary address line
  - **Column: ADDRESS_LINE2**
    - Data Type: NVARCHAR(255)
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Secondary address line
  - **Column: CITY**
    - Data Type: NVARCHAR(100)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: City name
  - **Column: STATE**
    - Data Type: NVARCHAR(100)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: State or province
  - **Column: POSTAL_CODE**
    - Data Type: NVARCHAR(20)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: ZIP or postal code
  - **Column: COUNTRY**
    - Data Type: NVARCHAR(100)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Country name
  - **Column: CREATED_DATE**
    - Data Type: DATETIME2
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Record creation timestamp
  - **Column: LAST_UPDATE_DATE**
    - Data Type: DATETIME2
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Last modification timestamp
- **CDC Configuration**:
  - Method: Watermark
  - Version/Watermark Column: LAST_UPDATE_DATE
  - Change Detection Query Pattern: `LAST_UPDATE_DATE > @last_sync_version`
- **Bronze Layer Target**:
  - Path: `bronze/src-001/erp/addresses/`
  - Format: Delta Lake
  - Partitioning: By `ingest_date`
- **Metadata Columns Added**:
  - `_pipeline_run_id` (STRING): Lineage tracking
  - `_ingest_timestamp` (TIMESTAMP): Processing time
  - `_source_system` (STRING): Source identifier ('SRC-001')
  - `_cdc_operation` (STRING): I/U/D operation type
- **MERGE Configuration**:
  - Merge Key: ADDRESS_ID
  - Match Condition: `target.ADDRESS_ID = source.ADDRESS_ID`
  - Delete Handling: N/A
  - Update Handling: Update all non-PK columns

### Entity: ERP.CITY_TIER_MASTER
- **Source Details**:
  - Source System ID: SRC-001 (Oracle ERP)
  - Schema Name: ERP
  - Table Name: CITY_TIER_MASTER
  - Primary Key(s): CITY, STATE
  - Load Type: FULL
  - Load Priority: 40
- **Column Specifications**:
  - **Column: CITY**
    - Data Type: NVARCHAR(100)
    - Nullable: NO
    - Primary Key: YES
    - Foreign Key: NONE
    - Description: City name
  - **Column: STATE**
    - Data Type: NVARCHAR(100)
    - Nullable: NO
    - Primary Key: YES
    - Foreign Key: NONE
    - Description: State name
  - **Column: TIER**
    - Data Type: NVARCHAR(20)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Geographic tier classification
  - **Column: CREATED_DATE**
    - Data Type: DATETIME2
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Record creation timestamp
- **CDC Configuration**:
  - Method: Full Refresh
  - Version/Watermark Column: N/A
  - Change Detection Query Pattern: N/A
- **Bronze Layer Target**:
  - Path: `bronze/src-001/erp/city_tier_master/`
  - Format: Delta Lake
  - Partitioning: By `ingest_date`
- **Metadata Columns Added**:
  - `_pipeline_run_id` (STRING): Lineage tracking
  - `_ingest_timestamp` (TIMESTAMP): Processing time
  - `_source_system` (STRING): Source identifier ('SRC-001')
  - `_cdc_operation` (STRING): I/U/D operation type
- **MERGE Configuration**:
  - Merge Key: CITY, STATE
  - Match Condition: `target.CITY = source.CITY AND target.STATE = source.STATE`
  - Delete Handling: N/A
  - Update Handling: Overwrite partition

### Entity: ERP.MTL_SYSTEM_ITEMS_B
- **Source Details**:
  - Source System ID: SRC-001 (Oracle ERP)
  - Schema Name: ERP
  - Table Name: MTL_SYSTEM_ITEMS_B
  - Primary Key(s): INVENTORY_ITEM_ID
  - Load Type: WATERMARK
  - Load Priority: 30
- **Column Specifications**:
  - **Column: INVENTORY_ITEM_ID**
    - Data Type: BIGINT
    - Nullable: NO
    - Primary Key: YES
    - Foreign Key: NONE
    - Description: Unique product identifier
  - **Column: CATEGORY_ID**
    - Data Type: BIGINT
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: ERP.CATEGORIES.CATEGORY_ID
    - Description: Product category
  - **Column: BRAND_ID**
    - Data Type: BIGINT
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: ERP.BRANDS.BRAND_ID
    - Description: Product brand
  - **Column: SKU**
    - Data Type: NVARCHAR(100)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Stock keeping unit
  - **Column: PRODUCT_NAME**
    - Data Type: NVARCHAR(255)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Name of the product
  - **Column: DESCRIPTION**
    - Data Type: NVARCHAR(MAX)
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Product description
  - **Column: UNIT_COST**
    - Data Type: DECIMAL(15,2)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Cost per unit
  - **Column: CREATED_DATE**
    - Data Type: DATETIME2
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Record creation timestamp
  - **Column: LAST_UPDATE_DATE**
    - Data Type: DATETIME2
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Last modification timestamp
- **CDC Configuration**:
  - Method: Watermark
  - Version/Watermark Column: LAST_UPDATE_DATE
  - Change Detection Query Pattern: `LAST_UPDATE_DATE > @last_sync_version`
- **Bronze Layer Target**:
  - Path: `bronze/src-001/erp/mtl_system_items_b/`
  - Format: Delta Lake
  - Partitioning: By `ingest_date`
- **Metadata Columns Added**:
  - `_pipeline_run_id` (STRING): Lineage tracking
  - `_ingest_timestamp` (TIMESTAMP): Processing time
  - `_source_system` (STRING): Source identifier ('SRC-001')
  - `_cdc_operation` (STRING): I/U/D operation type
- **MERGE Configuration**:
  - Merge Key: INVENTORY_ITEM_ID
  - Match Condition: `target.INVENTORY_ITEM_ID = source.INVENTORY_ITEM_ID`
  - Delete Handling: N/A
  - Update Handling: Update all non-PK columns

### Entity: ERP.CATEGORIES
- **Source Details**:
  - Source System ID: SRC-001 (Oracle ERP)
  - Schema Name: ERP
  - Table Name: CATEGORIES
  - Primary Key(s): CATEGORY_ID
  - Load Type: FULL
  - Load Priority: 40
- **Column Specifications**:
  - **Column: CATEGORY_ID**
    - Data Type: BIGINT
    - Nullable: NO
    - Primary Key: YES
    - Foreign Key: NONE
    - Description: Unique category identifier
  - **Column: CATEGORY_NAME**
    - Data Type: NVARCHAR(100)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Name of the category
  - **Column: PARENT_CATEGORY_ID**
    - Data Type: BIGINT
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: ERP.CATEGORIES.CATEGORY_ID
    - Description: Parent category for hierarchy
  - **Column: CREATED_DATE**
    - Data Type: DATETIME2
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Record creation timestamp
- **CDC Configuration**:
  - Method: Full Refresh
  - Version/Watermark Column: N/A
  - Change Detection Query Pattern: N/A
- **Bronze Layer Target**:
  - Path: `bronze/src-001/erp/categories/`
  - Format: Delta Lake
  - Partitioning: By `ingest_date`
- **Metadata Columns Added**:
  - `_pipeline_run_id` (STRING): Lineage tracking
  - `_ingest_timestamp` (TIMESTAMP): Processing time
  - `_source_system` (STRING): Source identifier ('SRC-001')
  - `_cdc_operation` (STRING): I/U/D operation type
- **MERGE Configuration**:
  - Merge Key: CATEGORY_ID
  - Match Condition: `target.CATEGORY_ID = source.CATEGORY_ID`
  - Delete Handling: N/A
  - Update Handling: Overwrite partition

### Entity: ERP.BRANDS
- **Source Details**:
  - Source System ID: SRC-001 (Oracle ERP)
  - Schema Name: ERP
  - Table Name: BRANDS
  - Primary Key(s): BRAND_ID
  - Load Type: FULL
  - Load Priority: 40
- **Column Specifications**:
  - **Column: BRAND_ID**
    - Data Type: BIGINT
    - Nullable: NO
    - Primary Key: YES
    - Foreign Key: NONE
    - Description: Unique brand identifier
  - **Column: BRAND_NAME**
    - Data Type: NVARCHAR(100)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Name of the brand
  - **Column: CREATED_DATE**
    - Data Type: DATETIME2
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Record creation timestamp
- **CDC Configuration**:
  - Method: Full Refresh
  - Version/Watermark Column: N/A
  - Change Detection Query Pattern: N/A
- **Bronze Layer Target**:
  - Path: `bronze/src-001/erp/brands/`
  - Format: Delta Lake
  - Partitioning: By `ingest_date`
- **Metadata Columns Added**:
  - `_pipeline_run_id` (STRING): Lineage tracking
  - `_ingest_timestamp` (TIMESTAMP): Processing time
  - `_source_system` (STRING): Source identifier ('SRC-001')
  - `_cdc_operation` (STRING): I/U/D operation type
- **MERGE Configuration**:
  - Merge Key: BRAND_ID
  - Match Condition: `target.BRAND_ID = source.BRAND_ID`
  - Delete Handling: N/A
  - Update Handling: Overwrite partition

### Entity: MARKETING.MARKETING_CAMPAIGNS
- **Source Details**:
  - Source System ID: SRC-003 (Marketing Platform)
  - Schema Name: MARKETING
  - Table Name: MARKETING_CAMPAIGNS
  - Primary Key(s): CAMPAIGN_ID
  - Load Type: FULL
  - Load Priority: 50
- **Column Specifications**:
  - **Column: CAMPAIGN_ID**
    - Data Type: INT
    - Nullable: NO
    - Primary Key: YES
    - Foreign Key: NONE
    - Description: Unique campaign identifier
  - **Column: CAMPAIGN_NAME**
    - Data Type: NVARCHAR(200)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Campaign display name
  - **Column: CAMPAIGN_CODE**
    - Data Type: NVARCHAR(50)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Unique campaign code
  - **Column: CHANNEL**
    - Data Type: NVARCHAR(50)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Marketing channel
  - **Column: SUB_CHANNEL**
    - Data Type: NVARCHAR(50)
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Channel subdivision
  - **Column: START_DATE**
    - Data Type: DATE
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Campaign start date
  - **Column: END_DATE**
    - Data Type: DATE
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Campaign end date
  - **Column: TOTAL_SPEND**
    - Data Type: DECIMAL(15,2)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Total campaign expenditure
  - **Column: CUSTOMERS_ACQUIRED**
    - Data Type: INT
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Customers attributed
  - **Column: STATUS**
    - Data Type: NVARCHAR(20)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Campaign status
  - **Column: CREATED_DATE**
    - Data Type: DATETIME2
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Record creation timestamp
  - **Column: LAST_UPDATE_DATE**
    - Data Type: DATETIME2
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Last modification timestamp
- **CDC Configuration**:
  - Method: Full Refresh (Daily Batch API Pull)
  - Version/Watermark Column: N/A
  - Change Detection Query Pattern: N/A
- **Bronze Layer Target**:
  - Path: `bronze/src-003/marketing/marketing_campaigns/`
  - Format: Delta Lake
  - Partitioning: By `ingest_date`
- **Metadata Columns Added**:
  - `_pipeline_run_id` (STRING): Lineage tracking
  - `_ingest_timestamp` (TIMESTAMP): Processing time
  - `_source_system` (STRING): Source identifier ('SRC-003')
  - `_cdc_operation` (STRING): I/U/D operation type
- **MERGE Configuration**:
  - Merge Key: CAMPAIGN_ID
  - Match Condition: `target.CAMPAIGN_ID = source.CAMPAIGN_ID`
  - Delete Handling: N/A
  - Update Handling: Overwrite partition

### Entity: CRM.INCIDENTS
- **Source Details**:
  - Source System ID: SRC-002 (Oracle CRM)
  - Schema Name: CRM
  - Table Name: INCIDENTS
  - Primary Key(s): INCIDENT_ID
  - Load Type: WATERMARK
  - Load Priority: 30
- **Column Specifications**:
  - **Column: INCIDENT_ID**
    - Data Type: BIGINT
    - Nullable: NO
    - Primary Key: YES
    - Foreign Key: NONE
    - Description: Unique support ticket identifier
  - **Column: CUSTOMER_ID**
    - Data Type: BIGINT
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: CRM.CUSTOMERS.CUSTOMER_ID
    - Description: Customer raising the incident
  - **Column: ORDER_ID**
    - Data Type: BIGINT
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: ERP.OE_ORDER_HEADERS_ALL.ORDER_ID
    - Description: Associated order
  - **Column: STATUS**
    - Data Type: NVARCHAR(50)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Incident status
  - **Column: PRIORITY**
    - Data Type: NVARCHAR(20)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Incident priority
  - **Column: CREATED_DATE**
    - Data Type: DATETIME2
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Record creation timestamp
  - **Column: LAST_UPDATE_DATE**
    - Data Type: DATETIME2
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Last modification timestamp
- **CDC Configuration**:
  - Method: Watermark
  - Version/Watermark Column: LAST_UPDATE_DATE
  - Change Detection Query Pattern: `LAST_UPDATE_DATE > @last_sync_version`
- **Bronze Layer Target**:
  - Path: `bronze/src-002/crm/incidents/`
  - Format: Delta Lake
  - Partitioning: By `ingest_date`
- **Metadata Columns Added**:
  - `_pipeline_run_id` (STRING): Lineage tracking
  - `_ingest_timestamp` (TIMESTAMP): Processing time
  - `_source_system` (STRING): Source identifier ('SRC-002')
  - `_cdc_operation` (STRING): I/U/D operation type
- **MERGE Configuration**:
  - Merge Key: INCIDENT_ID
  - Match Condition: `target.INCIDENT_ID = source.INCIDENT_ID`
  - Delete Handling: N/A
  - Update Handling: Update all non-PK columns

### Entity: CRM.INTERACTIONS
- **Source Details**:
  - Source System ID: SRC-002 (Oracle CRM)
  - Schema Name: CRM
  - Table Name: INTERACTIONS
  - Primary Key(s): INTERACTION_ID
  - Load Type: WATERMARK
  - Load Priority: 40
- **Column Specifications**:
  - **Column: INTERACTION_ID**
    - Data Type: BIGINT
    - Nullable: NO
    - Primary Key: YES
    - Foreign Key: NONE
    - Description: Unique interaction identifier
  - **Column: INCIDENT_ID**
    - Data Type: BIGINT
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: CRM.INCIDENTS.INCIDENT_ID
    - Description: Associated incident
  - **Column: INTERACTION_TYPE**
    - Data Type: NVARCHAR(50)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Type of interaction (Email, Call, Chat)
  - **Column: INTERACTION_DATE**
    - Data Type: DATETIME2
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Timestamp of interaction
  - **Column: NOTES**
    - Data Type: NVARCHAR(MAX)
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Interaction notes
- **CDC Configuration**:
  - Method: Watermark
  - Version/Watermark Column: INTERACTION_DATE (Append only)
  - Change Detection Query Pattern: `INTERACTION_DATE > @last_sync_version`
- **Bronze Layer Target**:
  - Path: `bronze/src-002/crm/interactions/`
  - Format: Delta Lake
  - Partitioning: By `ingest_date`
- **Metadata Columns Added**:
  - `_pipeline_run_id` (STRING): Lineage tracking
  - `_ingest_timestamp` (TIMESTAMP): Processing time
  - `_source_system` (STRING): Source identifier ('SRC-002')
  - `_cdc_operation` (STRING): I/U/D operation type
- **MERGE Configuration**:
  - Merge Key: INTERACTION_ID
  - Match Condition: `target.INTERACTION_ID = source.INTERACTION_ID`
  - Delete Handling: N/A
  - Update Handling: Update all non-PK columns

### Entity: CRM.SURVEYS
- **Source Details**:
  - Source System ID: SRC-002 (Oracle CRM)
  - Schema Name: CRM
  - Table Name: SURVEYS
  - Primary Key(s): SURVEY_ID
  - Load Type: WATERMARK
  - Load Priority: 50
- **Column Specifications**:
  - **Column: SURVEY_ID**
    - Data Type: BIGINT
    - Nullable: NO
    - Primary Key: YES
    - Foreign Key: NONE
    - Description: Unique survey response ID
  - **Column: CUSTOMER_ID**
    - Data Type: BIGINT
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: CRM.CUSTOMERS.CUSTOMER_ID
    - Description: Responding customer
  - **Column: ORDER_ID**
    - Data Type: BIGINT
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: ERP.OE_ORDER_HEADERS_ALL.ORDER_ID
    - Description: Associated order
  - **Column: INCIDENT_ID**
    - Data Type: BIGINT
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: CRM.INCIDENTS.INCIDENT_ID
    - Description: Associated support ticket
  - **Column: SURVEY_TYPE**
    - Data Type: NVARCHAR(20)
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Type of survey
  - **Column: NPS_SCORE**
    - Data Type: TINYINT
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Net Promoter Score (0-10)
  - **Column: CSAT_SCORE**
    - Data Type: TINYINT
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Customer Satisfaction (1-5)
  - **Column: NPS_CATEGORY**
    - Data Type: NVARCHAR(20)
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: NPS classification
  - **Column: FEEDBACK_TEXT**
    - Data Type: NVARCHAR(MAX)
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Open-ended feedback
  - **Column: FEEDBACK_CATEGORY**
    - Data Type: NVARCHAR(50)
    - Nullable: YES
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Feedback topic
  - **Column: RESPONSE_DATE**
    - Data Type: DATETIME2
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Survey completion timestamp
  - **Column: SURVEY_SENT_DATE**
    - Data Type: DATETIME2
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Survey sent timestamp
  - **Column: CREATED_DATE**
    - Data Type: DATETIME2
    - Nullable: NO
    - Primary Key: NO
    - Foreign Key: NONE
    - Description: Record creation timestamp
- **CDC Configuration**:
  - Method: Watermark
  - Version/Watermark Column: CREATED_DATE (Append only)
  - Change Detection Query Pattern: `CREATED_DATE > @last_sync_version`
- **Bronze Layer Target**:
  - Path: `bronze/src-002/crm/surveys/`
  - Format: Delta Lake
  - Partitioning: By `ingest_date`
- **Metadata Columns Added**:
  - `_pipeline_run_id` (STRING): Lineage tracking
  - `_ingest_timestamp` (TIMESTAMP): Processing time
  - `_source_system` (STRING): Source identifier ('SRC-002')
  - `_cdc_operation` (STRING): I/U/D operation type
- **MERGE Configuration**:
  - Merge Key: SURVEY_ID
  - Match Condition: `target.SURVEY_ID = source.SURVEY_ID`
  - Delete Handling: N/A
  - Update Handling: Update all non-PK columns

---

## 7. Data Flow Diagrams

### 7.1 Initial Load Flow
```text
Source Table (e.g., CRM.CUSTOMERS)
    │
    │ SELECT * FROM schema.table
    ▼
[ADF Copy Activity]
    │
    │ Parquet files
    ▼
Bronze Layer (ADLS Gen2)
    Path: bronze/src-002/crm/customers/ingest_date=2025-11-01/
    │
    │ Databricks Processing Notebook
    ▼
[Add Metadata Columns]
    - _pipeline_run_id = 'abc-123'
    - _ingest_timestamp = CURRENT_TIMESTAMP
    - _source_system = 'SRC-002'
    - _cdc_operation = 'I'
    │
    ▼
Bronze Layer (Delta Lake)
    Path: bronze/src-002/crm/customers/
    (Partition Overwrite on ingest_date)
    │
    ▼
[Update Control Table]
    SET initial_load_completed = 1
    SET last_sync_version = MAX(LAST_UPDATE_DATE)
```

### 7.2 Incremental CDC Flow
```text
Source Table (e.g., ERP.OE_ORDER_HEADERS_ALL)
    │
    │ SELECT * FROM schema.table WHERE LAST_UPDATE_DATE > '2025-11-01T00:00:00'
    ▼
[ADF Copy Activity]
    │
    │ Parquet files
    ▼
Bronze Layer (ADLS Gen2)
    Path: bronze/src-001/erp/oe_order_headers_all/ingest_date=2025-11-02/
    │
    │ Databricks Processing Notebook
    ▼
[Delta MERGE Processing]
    │
    ├─ Match on ORDER_ID
    ├─ UPDATE (U) → Update all columns + audit columns
    └─ INSERT (I) → Insert new record + audit columns
    │
    ▼
Bronze Layer (Delta Lake)
    Path: bronze/src-001/erp/oe_order_headers_all/
    │
    ▼
[Update Control Table]
    SET last_sync_version = MAX(LAST_UPDATE_DATE)
```

---

## 8. Error Handling & Recovery

### 8.1 Error Categories
- **Transient Errors**: Network timeouts, API rate limits, database connection drops.
  - Action: ADF automated retry with exponential backoff.
- **Data Quality Errors**: Schema drift, type mismatch during Databricks read.
  - Action: Fail pipeline, log to `pipeline_execution_log`, trigger Azure Monitor alert. Bronze layer preserves raw data; fix schema and replay.
- **Configuration Errors**: Missing table metadata, invalid Key Vault secret.
  - Action: Immediate pipeline failure, trigger alert.
- **CDC Version Expired**: Source system purged old watermark data.
  - Action: Reset `initial_load_completed = 0` in control table, trigger full refresh.

### 8.2 Retry Configuration
- **Max Retries**: 3
- **Base Delay**: 300 seconds (5 minutes)
- **Backoff**: Exponential

### 8.3 Recovery Procedures
- **Initial Load Failed**: Re-run pipeline. `initial_load_completed` remains 0 until successful completion. Partition overwrite ensures idempotency.
- **Incremental Failed**: Re-run pipeline. `last_sync_version` remains unchanged. Delta MERGE ensures idempotency (no duplicates).
- **Corrupted Bronze Table**: Utilize Delta Lake Time Travel (`RESTORE TABLE ... TO TIMESTAMP`) to revert to the last known good state.

---

## 9. State Management

### 9.1 Checkpoint Strategy
- **Location**: `control.table_metadata` and `control.pipeline_execution_log` in Azure SQL.
- **Frequency**: Updated synchronously at the end of each successful table load.
- **Contents**: `last_sync_version`, `pipeline_run_id`, `records_loaded`, `last_load_status`.

### 9.2 Version Tracking
- **Query-based (Watermark)**: Store `MAX(LAST_UPDATE_DATE)` or `MAX(CREATED_DATE)` in `last_sync_version`.
- **Validation**: Before execution, ADF validates that `last_sync_version` is not null if `initial_load_completed = 1`.

---

## 10. Storage Design

### 10.1 Bronze Layer Storage
- **Container/Path**: `bronze/`
- **Structure**: `bronze/{source_system}/{schema}/{table}/ingest_date={YYYY-MM-DD}/`
- **Format**: Delta Lake (Parquet underlying)
- **Retention**: 30 days in Hot tier, transition to Cool tier via ADLS Lifecycle Management.

### 10.2 Delta Lake Configuration
- **Auto Optimize**: Enabled (`spark.databricks.delta.optimizeWrite.enabled = true`)
- **Auto Compaction**: Enabled (`spark.databricks.delta.autoCompact.enabled = true`)
- **VACUUM Retention**: 168 hours (7 days) to support Time Travel and recovery.
- **Z-Ordering**: Not applied at Bronze layer (reserved for Silver/Gold).

---

## 11. Security Specifications

### 11.1 Authentication Methods
- **Source Databases**: SQL Authentication via JDBC. Credentials stored in Azure Key Vault.
- **Storage (ADLS Gen2)**: Microsoft Entra ID Managed Identity / Service Principal.
- **Processing (Databricks)**: Service Principal for ADLS access, Unity Catalog for table access.

### 11.2 Network Security
- **VNet Integration**: Databricks workspace deployed with VNet injection.
- **Private Endpoints**: Configured for ADLS Gen2, Azure SQL Database, and Azure Key Vault.
- **Public Access**: Disabled on all storage and database resources.

### 11.3 Data Security
- **Encryption at Rest**: AES-256 (Platform default SSE).
- **Encryption in Transit**: TLS 1.2+ enforced.
- **PII Columns**: Preserved in Bronze, masked dynamically in Unity Catalog at the Silver/Gold layers.

---

## 12. Monitoring & Alerting

### 12.1 Key Metrics
- **Pipeline Duration**: Tracked in Azure Monitor vs historical baseline.
- **Records Processed**: Logged to `control.pipeline_execution_log`.
- **Error Rate**: Percentage of failed pipeline runs per day.
- **CDC Lag**: Time difference between `CURRENT_TIMESTAMP` and `last_sync_version`.

### 12.2 Alerting Rules
- **Pipeline Failure**: Immediate email/Teams alert via Azure Logic Apps.
- **No Data for 24h**: Warning alert if `records_loaded = 0` for critical tables (e.g., `OE_ORDER_HEADERS_ALL`) for 24 consecutive hours.
- **CDC Lag > 24 hours**: Warning alert indicating potential source system extraction issues.

---

## 13. Deployment Specifications

### 13.1 Deployment Order
- Deploy Azure SQL Database and execute DDL scripts for control schema.
- Deploy Azure Key Vault and populate source system credentials.
- Deploy ADLS Gen2 and configure Private Endpoints.
- Deploy Databricks Workspace and configure Unity Catalog external locations.
- Deploy ADF Linked Services and Datasets.
- Deploy Databricks processing notebooks.
- Deploy ADF Pipelines (Master, Initial, Incremental).
- Execute initial load for all tables.

### 13.2 Validation Checklist
- [ ] Control tables created and populated with 13 entities.
- [ ] Stored procedures deployed to Azure SQL.
- [ ] Key Vault secrets accessible by ADF Managed Identity.
- [ ] Storage connectivity verified via Databricks notebook.
- [ ] Processing cluster configured with correct Spark properties.
- [ ] Initial load tested and verified for `CRM.CUSTOMERS`.
- [ ] `initial_load_completed` flag successfully updates to 1.

---

## 14. Appendix

### 14.1 Consolidated Artifact Summary
- **Control Tables**: `source_systems`, `table_metadata`, `pipeline_execution_log`, `data_quality_rules`
- **Stored Procedures**: `sp_GetWatermarkQuery`, `sp_UpdateTableMetadata`
- **Pipelines**: `PL_Master_Orchestrator`, `PL_Initial_Load_Single_Table`, `PL_Incremental_CDC_Single_Table`
- **Processing Notebooks**: `NB_Process_Initial_Load`, `NB_Process_Incremental_Merge`

### 14.2 Entity Cross-Reference
- SRC-001 (ERP): 7 Entities (Orders, Lines, Addresses, City Tier, Items, Categories, Brands)
- SRC-002 (CRM): 5 Entities (Customers, Registration, Incidents, Interactions, Surveys)
- SRC-003 (Marketing): 1 Entity (Campaigns)
- Total Entities: 13

### 14.3 Glossary
- **CDC**: Change Data Capture
- **DLQ**: Dead Letter Queue
- **LLD**: Low-Level Design
- **LSN**: Log Sequence Number
- **PK**: Primary Key
- **ZRS**: Zone-Redundant Storage