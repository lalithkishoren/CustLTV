# Silver Layer Low-Level Design Specification

## 1. Executive Summary

### 1.1 Solution Overview
- **Business Problem**: The Silver layer bridges the gap between raw, immutable Bronze data and business-ready Gold aggregations. It cleanses, standardizes, and conforms data from Oracle ERP, Oracle CRM, and Marketing platforms to enable accurate Customer Lifetime Value (CLV) predictions, RFM segmentation, and churn analysis.
- **High-Level Architecture**:
```text
[Bronze Layer (ADLS Gen2)]
    │
    │ Raw Parquet files with CDC metadata (_cdc_operation, _ingest_timestamp)
    ▼
[Azure Data Factory Orchestrator]
    │
    ▼
[Databricks DLT / Spark Processing]
    ├── 1. Schema & Type Validation
    ├── 2. Data Quality Enforcement (Expectations & DLQ Routing)
    ├── 3. Business Transformations (Standardization, Cleansing)
    └── 4. Idempotent MERGE (SCD Type 1 & Type 2)
    │
    ▼
[Silver Layer (Delta Lake on ADLS Gen2)] ──► [Unity Catalog Governance]
```
- **Key Transformation Capabilities**:
  - Enforcement of enterprise data quality rules (e.g., dropping orders with negative amounts).
  - Implementation of Slowly Changing Dimensions (SCD Type 2) to track customer churn status over time.
  - Standardization of dates, strings, and monetary values across disparate source systems.
  - Resolution of late-arriving data and out-of-order CDC events via Delta Lake MERGE operations.
- **Data Quality Improvements**:
  - Progressive trust model: Invalid records are quarantined to a Dead Letter Queue (DLQ) without failing the entire batch, ensuring continuous pipeline operation while preserving data integrity.

### 1.2 Scope
- **In-Scope Silver Tables**:
  - ERP Domain: `silver.oe_order_headers_all`, `silver.oe_order_lines_all`, `silver.addresses`, `silver.city_tier_master`, `silver.mtl_system_items_b`, `silver.categories`, `silver.brands`
  - CRM Domain: `silver.customers`, `silver.customer_registration_source`, `silver.incidents`, `silver.interactions`, `silver.surveys`
  - Marketing Domain: `silver.marketing_campaigns`
- **Out-of-Scope Items**:
  - Gold layer aggregations (AOV, Purchase Frequency, CAC).
  - Machine Learning predictive models.
  - Source system extraction logic (handled in Bronze).
- **Assumptions and Constraints**:
  - Bronze layer provides reliable `_cdc_operation` flags ('I', 'U', 'D').
  - Unity Catalog is configured with appropriate dynamic data masking policies for PII.

### 1.3 Technology Stack
- **Orchestration**: Azure Data Factory (ADF) - Triggers Databricks jobs and manages control metadata.
- **Compute**: Azure Databricks (Delta Live Tables / Spark Structured Streaming) - Executes transformations and MERGE operations.
- **Storage**: ADLS Gen2 - Hosts Silver Delta tables.
- **Governance**: Unity Catalog - Enforces RBAC, table metadata, and dynamic data masking.
- **Security**: Azure Key Vault - Manages Service Principal credentials for ADLS access.

---

## 2. Architecture Design

### 2.1 High-Level Architecture
```text
Bronze Layer (Raw)
    │
    │ Parquet files with CDC metadata
    ▼
[Silver Orchestrator Pipeline (ADF)]
    │
    ├── Data Quality Validation (Databricks)
    │   ├── PASS → Continue to Transform
    │   └── FAIL → Route to DLQ (silver.dq_exception_log)
    │
    ├── Transformation Engine (Databricks)
    │   ├── Type conversions & Null handling
    │   ├── String & Date Standardization
    │   └── PII Tagging for Unity Catalog
    │
    └── MERGE Processing (Databricks)
        ├── SCD Type 1 (Overwrite non-keys)
        └── SCD Type 2 (Track history via _valid_from, _valid_to)
    │
    ▼
Silver Layer (Delta Lake)
    │
    ▼
Control Database (Azure SQL - Metadata Updates)
```

### 2.2 Component Specifications
- **Component: Silver Orchestrator (ADF)**
  - **Purpose**: Master control flow for Silver layer processing.
  - **Technology**: Azure Data Factory.
  - **Inputs**: Bronze load completion status from `control.table_metadata`.
  - **Outputs**: Triggered Databricks notebooks, updated Silver metadata.
  - **Dependencies**: Bronze layer completion, Azure SQL Control DB.
  - **Configuration**: Sequential execution based on foreign key dependencies (Dimensions before Facts).
- **Component: Transformation Engine (Databricks)**
  - **Purpose**: Applies business rules and data quality checks.
  - **Technology**: Azure Databricks (Runtime 14.3 LTS).
  - **Inputs**: Bronze Parquet files.
  - **Outputs**: Cleansed DataFrames ready for MERGE, DLQ records.
  - **Dependencies**: Unity Catalog for schema validation.
  - **Configuration**: Auto-scaling compute, Delta Cache enabled.
- **Component: Silver Storage (ADLS Gen2 / Delta Lake)**
  - **Purpose**: Persistent, query-optimized storage for cleansed data.
  - **Technology**: Delta Lake on ADLS Gen2.
  - **Inputs**: Processed DataFrames.
  - **Outputs**: Delta tables registered in Unity Catalog.
  - **Dependencies**: None.
  - **Configuration**: Z-Ordering enabled, Auto-compaction enabled, 7-day VACUUM retention.

### 2.3 Data Flow Architecture
- **Initial Load Flow**:
  - ADF identifies tables where `silver_initial_load_completed = 0`.
  - Databricks reads the full Bronze initial load path.
  - Applies transformations and DQ rules; drops invalid rows to DLQ.
  - Writes to Silver Delta table using `INSERT OVERWRITE` or bulk `MERGE`.
  - ADF updates `silver_initial_load_completed = 1`.
- **Incremental CDC Flow**:
  - ADF identifies tables with new Bronze data (`last_sync_version` > `silver_last_sync_version`).
  - Databricks reads incremental Bronze path.
  - Applies transformations and DQ rules.
  - Executes `MERGE INTO` Silver table applying SCD Type 1 or Type 2 logic based on entity configuration.
  - ADF updates `silver_last_sync_version`.

---

## 3. Control Database Extensions

### 3.1 Extended Table: control.table_metadata
- **Column: silver_table_name**
  - Data Type: VARCHAR(255)
  - Description: Target Silver table name (e.g., 'customers')
- **Column: silver_schema**
  - Data Type: VARCHAR(100)
  - Description: Target schema in Unity Catalog (e.g., 'silver')
- **Column: silver_path**
  - Data Type: VARCHAR(500)
  - Description: ADLS path for Silver Delta table
- **Column: silver_initial_load_completed**
  - Data Type: BIT
  - Description: Flag indicating if Silver initial load is done (Default 0)
- **Column: silver_last_load_status**
  - Data Type: VARCHAR(50)
  - Description: Status of last Silver load (SUCCESS/FAILED)
- **Column: silver_last_load_timestamp**
  - Data Type: DATETIME2
  - Description: Timestamp of last Silver load
- **Column: silver_records_loaded**
  - Data Type: BIGINT
  - Description: Number of records successfully merged into Silver
- **Column: scd_type**
  - Data Type: INT
  - Description: SCD Strategy (1 = Overwrite, 2 = History)
- **Column: z_order_columns**
  - Data Type: VARCHAR(500)
  - Description: Comma-separated columns for Delta Z-Ordering
- **Column: partition_columns**
  - Data Type: VARCHAR(500)
  - Description: Comma-separated columns for Delta partitioning

### 3.2 New Table: control.dq_exception_log
- **Column: exception_id**
  - Data Type: BIGINT (PRIMARY KEY, IDENTITY)
- **Column: table_id**
  - Data Type: INT (FOREIGN KEY to table_metadata)
- **Column: pipeline_run_id**
  - Data Type: VARCHAR(100)
- **Column: rule_id**
  - Data Type: VARCHAR(50)
- **Column: rule_type**
  - Data Type: VARCHAR(50)
- **Column: column_name**
  - Data Type: VARCHAR(255)
- **Column: error_message**
  - Data Type: VARCHAR(MAX)
- **Column: row_data**
  - Data Type: NVARCHAR(MAX) (JSON representation of failed row)
- **Column: exception_timestamp**
  - Data Type: DATETIME2
- **Column: severity**
  - Data Type: VARCHAR(20) (ERROR, WARNING, SKIP_ROW)
- **Column: is_resolved**
  - Data Type: BIT (Default 0)

### 3.3 New Table: control.silver_lineage
- **Column: lineage_id**
  - Data Type: BIGINT (PRIMARY KEY, IDENTITY)
- **Column: table_id**
  - Data Type: INT (FOREIGN KEY to table_metadata)
- **Column: pipeline_run_id**
  - Data Type: VARCHAR(100)
- **Column: source_column**
  - Data Type: VARCHAR(255)
- **Column: target_column**
  - Data Type: VARCHAR(255)
- **Column: transformation_rule**
  - Data Type: VARCHAR(500)
- **Column: created_timestamp**
  - Data Type: DATETIME2

### 3.4 Extended Table: control.data_quality_rules
- **Column: rule_id**
  - Data Type: VARCHAR(50) (PRIMARY KEY)
- **Column: table_id**
  - Data Type: INT (FOREIGN KEY to table_metadata)
- **Column: rule_type**
  - Data Type: VARCHAR(50) (NULL_CHECK, RANGE_CHECK, FORMAT_CHECK, REFERENTIAL_INTEGRITY, DUPLICATE_CHECK, BUSINESS_RULE)
- **Column: column_name**
  - Data Type: VARCHAR(255)
- **Column: rule_expression**
  - Data Type: VARCHAR(MAX) (Exact SQL expression)
- **Column: severity**
  - Data Type: VARCHAR(20) (ERROR, WARNING, SKIP_ROW)
- **Column: is_active**
  - Data Type: BIT (Default 1)

---

## 4. Transformation Design Specifications

### 4.1 Standard Transformation Patterns
- **Type Conversion**: Explicit `CAST()` for all columns. No implicit conversions.
- **Null Handling**: `COALESCE()` applied to numeric metrics where NULL implies zero (e.g., `DISCOUNT_AMOUNT`).
- **String Standardization**: `TRIM()` applied to all VARCHAR/NVARCHAR fields. `UPPER()` applied to codes (e.g., `CURRENCY_CODE`, `STATUS`).
- **Date Standardization**: All dates cast to `DATE` or `TIMESTAMP`. Timezones normalized to UTC.
- **Numeric Precision**: Monetary values cast to `DECIMAL(15,2)`.

### 4.2 Data Quality Rules - Complete SQL Specifications
*(Note: These are extracted directly from the Blueprint and Metadata constraints)*

- **DQ Rule: DQ-V-001**
  - **Rule Type**: RANGE_CHECK
  - **Entity**: silver.oe_order_headers_all
  - **Column(s)**: total_amount
  - **Business Requirement**: DLT expectation ensures TOTAL_AMOUNT > 0. Violations are routed to a quarantine table.
  - **Severity**: SKIP_ROW
  - **Valid Row SQL Filter**: `total_amount > 0`
  - **Invalid Row SQL Check**: `total_amount <= 0 OR total_amount IS NULL`

- **DQ Rule: DQ-BR-001**
  - **Rule Type**: NULL_CHECK
  - **Entity**: silver.customers
  - **Column(s)**: customer_id
  - **Business Requirement**: Circuit breaker; if CRM.CUSTOMERS receives a payload with 100% null CUSTOMER_IDs, pipeline fails. At row level, drop nulls.
  - **Severity**: ERROR (Fail batch if 100%, otherwise SKIP_ROW)
  - **Valid Row SQL Filter**: `customer_id IS NOT NULL`
  - **Invalid Row SQL Check**: `customer_id IS NULL`

- **DQ Rule: DQ-F-001**
  - **Rule Type**: FORMAT_CHECK
  - **Entity**: silver.customers
  - **Column(s)**: email
  - **Business Requirement**: Email must follow standard format (derived from metadata).
  - **Severity**: WARNING
  - **Valid Row SQL Filter**: `email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'`
  - **Invalid Row SQL Check**: `email NOT RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'`

- **DQ Rule: DQ-R-001**
  - **Rule Type**: RANGE_CHECK
  - **Entity**: silver.surveys
  - **Column(s)**: nps_score
  - **Business Requirement**: Net Promoter Score must be between 0 and 10 (derived from metadata).
  - **Severity**: SKIP_ROW
  - **Valid Row SQL Filter**: `nps_score BETWEEN 0 AND 10 OR nps_score IS NULL`
  - **Invalid Row SQL Check**: `nps_score < 0 OR nps_score > 10`

- **DQ Rule: DQ-R-002**
  - **Rule Type**: RANGE_CHECK
  - **Entity**: silver.surveys
  - **Column(s)**: csat_score
  - **Business Requirement**: Customer Satisfaction must be between 1 and 5 (derived from metadata).
  - **Severity**: SKIP_ROW
  - **Valid Row SQL Filter**: `csat_score BETWEEN 1 AND 5 OR csat_score IS NULL`
  - **Invalid Row SQL Check**: `csat_score < 1 OR csat_score > 5`

- **DQ Rule: DQ-RI-001**
  - **Rule Type**: REFERENTIAL_INTEGRITY
  - **Entity**: silver.oe_order_headers_all
  - **Column(s)**: customer_id
  - **Business Requirement**: Orders must belong to a valid customer.
  - **Severity**: WARNING (Log to DLQ, but load to Silver to avoid dropping revenue data; Gold will handle orphans).
  - **Valid Row SQL Filter**: `EXISTS (SELECT 1 FROM silver.customers c WHERE c.customer_id = source.customer_id)`
  - **Invalid Row SQL Check**: `NOT EXISTS (SELECT 1 FROM silver.customers c WHERE c.customer_id = source.customer_id)`

### 4.3 Exception Handling Flow
- **PASS**: Row meets all `Valid Row SQL Filters`. Proceeds to transformation and MERGE.
- **FAIL (WARNING)**: Row fails a WARNING rule. Row is tagged with `_dq_warning = true`, logged to `control.dq_exception_log`, and proceeds to MERGE.
- **FAIL (SKIP_ROW)**: Row fails a SKIP_ROW rule. Row is written to `control.dq_exception_log` (DLQ) and dropped from the current processing DataFrame.
- **FAIL (ERROR)**: Row fails an ERROR rule. If error threshold (e.g., 5%) is exceeded, the entire Databricks job fails, triggering an ADF alert.

---

## 5. SCD (Slowly Changing Dimension) Design

### 5.1 SCD Type Selection per Entity
- **SCD Type 2 (History Tracking)**:
  - `silver.customers`: Required by Blueprint to track `STATUS` changes to accurately capture the exact timestamp when a customer transitions to 'Churned'.
- **SCD Type 1 (Overwrite)**:
  - `silver.marketing_campaigns`: Blueprint specifies standard SCD Type 1 (upsert) overwrites the `TOTAL_SPEND` as campaign costs are updated daily.
  - All other entities (Orders, Lines, Addresses, Items, etc.) use SCD Type 1 as historical tracking of attributes is not explicitly required by the CLV/RFM business rules.

### 5.2 SCD Type 1 Specifications (Overwrite)
- **MERGE Condition**: Match on Primary Key(s).
- **Update Action**: Overwrite all non-key columns.
- **Metadata Columns Updated**: `_last_modified_date` = CURRENT_TIMESTAMP, `_pipeline_run_id` = current run ID.

### 5.3 SCD Type 2 Specifications (History Tracking)
- **Entity**: `silver.customers`
- **Business Key**: `customer_id`
- **Tracked Columns**: `status`, `customer_type`, `marketing_opt_in`
- **SCD Metadata Columns**:
  - `_valid_from` (TIMESTAMP): Record effective start time.
  - `_valid_to` (TIMESTAMP): Record effective end time (NULL = current).
  - `_is_current` (BOOLEAN): True if current active record.
  - `_version` (INT): Incremental version number.
  - `_hash_key` (STRING): MD5 hash of tracked columns for change detection.

### 5.4 SCD Type 2 Processing Logic
- **Incoming CDC Record**:
  - Hash the tracked columns to generate `new_hash`.
  - Check if `customer_id` exists in target where `_is_current = true`.
  - **NOT EXISTS**: Insert new row (`_valid_from` = `last_update_date`, `_valid_to` = NULL, `_is_current` = true, `_version` = 1).
  - **EXISTS & Hash Matches**: Ignore (no tracked changes).
  - **EXISTS & Hash Differs**:
    - UPDATE existing row: SET `_valid_to` = `last_update_date`, `_is_current` = false.
    - INSERT new row: SET `_valid_from` = `last_update_date`, `_valid_to` = NULL, `_is_current` = true, `_version` = old_version + 1.

---

## 6. MERGE Design Specifications

### 6.1 MERGE Statement Structure
- **WHEN MATCHED AND source._cdc_operation = 'D'**:
  - Action: Soft delete.
  - Update: SET `_is_deleted` = true, `_last_modified_date` = CURRENT_TIMESTAMP.
- **WHEN MATCHED AND source._cdc_operation IN ('U', 'I')**:
  - Action: Update record.
  - Update: SET all mapped columns, `_last_modified_date` = CURRENT_TIMESTAMP, `_is_deleted` = false.
- **WHEN NOT MATCHED AND source._cdc_operation IN ('I', 'U')**:
  - Action: Insert record.
  - Insert: All mapped columns, `_created_date` = CURRENT_TIMESTAMP, `_last_modified_date` = CURRENT_TIMESTAMP, `_is_deleted` = false.

### 6.2 MERGE Key Configuration per Entity
*(Detailed within Section 8 Entity Specifications)*

---

## 7. Pipeline Architecture Specifications

### 7.1 Silver Orchestrator Pipeline
- **Name**: `PL_Silver_Orchestrator`
- **Parameters**: `pipeline_run_id`
- **Flow**:
  - **Lookup_SilverPendingTables**: `SELECT * FROM control.table_metadata WHERE silver_initial_load_completed = 0 ORDER BY load_priority ASC`
  - **ForEach_PendingTable**: Execute `PL_Silver_Initial_Load_Single_Table`
  - **Lookup_ActiveTablesForCDC**: `SELECT * FROM control.table_metadata WHERE silver_initial_load_completed = 1 ORDER BY load_priority ASC`
  - **ForEach_ActiveTable**: Execute `PL_Silver_Incremental_CDC_Single_Table`

### 7.2 Silver Initial Load Pipeline
- **Name**: `PL_Silver_Initial_Load_Single_Table`
- **Parameters**: `pipeline_run_id`, `table_id`, `schema_name`, `table_name`, `primary_key_columns`, `silver_table_name`, `silver_schema`
- **Activities**:
  - **Lookup_BronzeInitialLoadPath**: Get Bronze ADLS path.
  - **Notebook_Silver_Initial_Load**: Execute Databricks notebook passing parameters.
  - **SP_Update_Silver_Metadata**: Update `silver_initial_load_completed = 1`, `silver_records_loaded`.
- **Bronze Path**: `bronze/{source}/{schema}/{table}/`
- **Silver Path**: `silver/{source}/{schema}/{table}/`

### 7.3 Silver Incremental CDC Pipeline
- **Name**: `PL_Silver_Incremental_CDC_Single_Table`
- **Parameters**: `pipeline_run_id`, `table_id`, `schema_name`, `table_name`, `primary_key_columns`, `last_sync_version`
- **Activities**:
  - **Lookup_BronzeIncrementalPath**: Get Bronze ADLS path for new partitions.
  - **Notebook_Silver_Incremental_CDC**: Execute Databricks notebook to perform MERGE.
  - **SP_Update_Silver_Metadata**: Update `silver_last_load_timestamp`, `silver_records_loaded`.

---

## 8. Entity Specifications

### Entity: silver.customers
- **Source Details**:
  - Bronze Table: `bronze/src-002/crm/customers/`
  - Source System ID: SRC-002
  - Primary Key(s): `customer_id`
  - SCD Type: 2
- **Column Transformation Specifications**:
  - **customer_id**
    - Source: `bronze.CUSTOMER_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(CUSTOMER_ID AS BIGINT)`
  - **email**
    - Source: `bronze.EMAIL`
    - Target Type: STRING
    - Transformation: `LOWER(TRIM(EMAIL))`
  - **phone**
    - Source: `bronze.PHONE`
    - Target Type: STRING
    - Transformation: `TRIM(PHONE)`
  - **first_name**
    - Source: `bronze.FIRST_NAME`
    - Target Type: STRING
    - Transformation: `TRIM(FIRST_NAME)`
  - **last_name**
    - Source: `bronze.LAST_NAME`
    - Target Type: STRING
    - Transformation: `TRIM(LAST_NAME)`
  - **gender**
    - Source: `bronze.GENDER`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(GENDER))`
  - **date_of_birth**
    - Source: `bronze.DATE_OF_BIRTH`
    - Target Type: DATE
    - Transformation: `CAST(DATE_OF_BIRTH AS DATE)`
  - **registration_date**
    - Source: `bronze.REGISTRATION_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(REGISTRATION_DATE AS TIMESTAMP)`
  - **customer_type**
    - Source: `bronze.CUSTOMER_TYPE`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(CUSTOMER_TYPE))`
  - **status**
    - Source: `bronze.STATUS`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(STATUS))`
  - **email_verified**
    - Source: `bronze.EMAIL_VERIFIED`
    - Target Type: BOOLEAN
    - Transformation: `CAST(EMAIL_VERIFIED AS BOOLEAN)`
  - **phone_verified**
    - Source: `bronze.PHONE_VERIFIED`
    - Target Type: BOOLEAN
    - Transformation: `CAST(PHONE_VERIFIED AS BOOLEAN)`
  - **marketing_opt_in**
    - Source: `bronze.MARKETING_OPT_IN`
    - Target Type: BOOLEAN
    - Transformation: `CAST(MARKETING_OPT_IN AS BOOLEAN)`
  - **preferred_language**
    - Source: `bronze.PREFERRED_LANGUAGE`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(PREFERRED_LANGUAGE))`
  - **created_by**
    - Source: `bronze.CREATED_BY`
    - Target Type: STRING
    - Transformation: `TRIM(CREATED_BY)`
  - **created_date**
    - Source: `bronze.CREATED_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(CREATED_DATE AS TIMESTAMP)`
  - **last_update_date**
    - Source: `bronze.LAST_UPDATE_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(LAST_UPDATE_DATE AS TIMESTAMP)`
- **Metadata Columns Added**:
  - `_pipeline_run_id` (STRING)
  - `_load_timestamp` (TIMESTAMP)
  - `_is_deleted` (BOOLEAN)
  - `_is_current` (BOOLEAN)
  - `_valid_from` (TIMESTAMP)
  - `_valid_to` (TIMESTAMP)
  - `_hash_key` (STRING)
- **Data Quality Rules**:
  - Rule ID: DQ-N-001 | Type: NULL_CHECK | Column: customer_id | Expression: `customer_id IS NOT NULL` | Severity: ERROR
  - Rule ID: DQ-N-002 | Type: NULL_CHECK | Column: email | Expression: `email IS NOT NULL` | Severity: SKIP_ROW
  - Rule ID: DQ-F-001 | Type: FORMAT_CHECK | Column: email | Expression: `email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'` | Severity: WARNING
- **MERGE Configuration**:
  - Merge Key: `customer_id`
  - Match Condition: `target.customer_id = source.customer_id`
  - Delete Handling: Soft delete (`_is_deleted = true`)
  - SCD Columns Tracked: `status`, `customer_type`, `marketing_opt_in`
- **Silver Layer Target**:
  - Path: `silver/src-002/crm/customers/`
  - Partition Column(s): None (Dimension table)
  - Z-Order Column(s): `customer_id`

### Entity: silver.customer_registration_source
- **Source Details**:
  - Bronze Table: `bronze/src-002/crm/customer_registration_source/`
  - Source System ID: SRC-002
  - Primary Key(s): `registration_source_id`
  - SCD Type: 1
- **Column Transformation Specifications**:
  - **registration_source_id**
    - Source: `bronze.REGISTRATION_SOURCE_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(REGISTRATION_SOURCE_ID AS BIGINT)`
  - **customer_id**
    - Source: `bronze.CUSTOMER_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(CUSTOMER_ID AS BIGINT)`
  - **channel**
    - Source: `bronze.CHANNEL`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(CHANNEL))`
  - **campaign_id**
    - Source: `bronze.CAMPAIGN_ID`
    - Target Type: INT
    - Transformation: `CAST(CAMPAIGN_ID AS INT)`
  - **utm_source**
    - Source: `bronze.UTM_SOURCE`
    - Target Type: STRING
    - Transformation: `LOWER(TRIM(UTM_SOURCE))`
  - **utm_medium**
    - Source: `bronze.UTM_MEDIUM`
    - Target Type: STRING
    - Transformation: `LOWER(TRIM(UTM_MEDIUM))`
  - **utm_campaign**
    - Source: `bronze.UTM_CAMPAIGN`
    - Target Type: STRING
    - Transformation: `LOWER(TRIM(UTM_CAMPAIGN))`
  - **utm_content**
    - Source: `bronze.UTM_CONTENT`
    - Target Type: STRING
    - Transformation: `LOWER(TRIM(UTM_CONTENT))`
  - **referrer_url**
    - Source: `bronze.REFERRER_URL`
    - Target Type: STRING
    - Transformation: `TRIM(REFERRER_URL)`
  - **landing_page**
    - Source: `bronze.LANDING_PAGE`
    - Target Type: STRING
    - Transformation: `TRIM(LANDING_PAGE)`
  - **device_type**
    - Source: `bronze.DEVICE_TYPE`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(DEVICE_TYPE))`
  - **registration_date**
    - Source: `bronze.REGISTRATION_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(REGISTRATION_DATE AS TIMESTAMP)`
  - **created_date**
    - Source: `bronze.CREATED_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(CREATED_DATE AS TIMESTAMP)`
- **Metadata Columns Added**:
  - `_pipeline_run_id` (STRING)
  - `_load_timestamp` (TIMESTAMP)
  - `_is_deleted` (BOOLEAN)
- **Data Quality Rules**:
  - Rule ID: DQ-N-003 | Type: NULL_CHECK | Column: registration_source_id | Expression: `registration_source_id IS NOT NULL` | Severity: SKIP_ROW
  - Rule ID: DQ-N-004 | Type: NULL_CHECK | Column: customer_id | Expression: `customer_id IS NOT NULL` | Severity: SKIP_ROW
  - Rule ID: DQ-RI-002 | Type: REFERENTIAL_INTEGRITY | Column: customer_id | Expression: `EXISTS (SELECT 1 FROM silver.customers c WHERE c.customer_id = source.customer_id)` | Severity: WARNING
- **MERGE Configuration**:
  - Merge Key: `registration_source_id`
  - Match Condition: `target.registration_source_id = source.registration_source_id`
  - Delete Handling: Soft delete
- **Silver Layer Target**:
  - Path: `silver/src-002/crm/customer_registration_source/`
  - Partition Column(s): None
  - Z-Order Column(s): `customer_id`, `campaign_id`

### Entity: silver.oe_order_headers_all
- **Source Details**:
  - Bronze Table: `bronze/src-001/erp/oe_order_headers_all/`
  - Source System ID: SRC-001
  - Primary Key(s): `order_id`
  - SCD Type: 1
- **Column Transformation Specifications**:
  - **order_id**
    - Source: `bronze.ORDER_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(ORDER_ID AS BIGINT)`
  - **order_number**
    - Source: `bronze.ORDER_NUMBER`
    - Target Type: STRING
    - Transformation: `TRIM(ORDER_NUMBER)`
  - **customer_id**
    - Source: `bronze.CUSTOMER_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(CUSTOMER_ID AS BIGINT)`
  - **order_date**
    - Source: `bronze.ORDER_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(ORDER_DATE AS TIMESTAMP)`
  - **order_status**
    - Source: `bronze.ORDER_STATUS`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(ORDER_STATUS))`
  - **payment_method**
    - Source: `bronze.PAYMENT_METHOD`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(PAYMENT_METHOD))`
  - **payment_status**
    - Source: `bronze.PAYMENT_STATUS`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(PAYMENT_STATUS))`
  - **subtotal_amount**
    - Source: `bronze.SUBTOTAL_AMOUNT`
    - Target Type: DECIMAL(15,2)
    - Transformation: `CAST(SUBTOTAL_AMOUNT AS DECIMAL(15,2))`
  - **discount_amount**
    - Source: `bronze.DISCOUNT_AMOUNT`
    - Target Type: DECIMAL(15,2)
    - Transformation: `COALESCE(CAST(DISCOUNT_AMOUNT AS DECIMAL(15,2)), 0.00)`
  - **tax_amount**
    - Source: `bronze.TAX_AMOUNT`
    - Target Type: DECIMAL(15,2)
    - Transformation: `COALESCE(CAST(TAX_AMOUNT AS DECIMAL(15,2)), 0.00)`
  - **shipping_amount**
    - Source: `bronze.SHIPPING_AMOUNT`
    - Target Type: DECIMAL(15,2)
    - Transformation: `COALESCE(CAST(SHIPPING_AMOUNT AS DECIMAL(15,2)), 0.00)`
  - **total_amount**
    - Source: `bronze.TOTAL_AMOUNT`
    - Target Type: DECIMAL(15,2)
    - Transformation: `CAST(TOTAL_AMOUNT AS DECIMAL(15,2))`
  - **currency_code**
    - Source: `bronze.CURRENCY_CODE`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(CURRENCY_CODE))`
  - **shipping_address_id**
    - Source: `bronze.SHIPPING_ADDRESS_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(SHIPPING_ADDRESS_ID AS BIGINT)`
  - **billing_address_id**
    - Source: `bronze.BILLING_ADDRESS_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(BILLING_ADDRESS_ID AS BIGINT)`
  - **promised_date**
    - Source: `bronze.PROMISED_DATE`
    - Target Type: DATE
    - Transformation: `CAST(PROMISED_DATE AS DATE)`
  - **shipped_date**
    - Source: `bronze.SHIPPED_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(SHIPPED_DATE AS TIMESTAMP)`
  - **delivered_date**
    - Source: `bronze.DELIVERED_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(DELIVERED_DATE AS TIMESTAMP)`
  - **cancellation_reason**
    - Source: `bronze.CANCELLATION_REASON`
    - Target Type: STRING
    - Transformation: `TRIM(CANCELLATION_REASON)`
  - **order_source**
    - Source: `bronze.ORDER_SOURCE`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(ORDER_SOURCE))`
  - **org_id**
    - Source: `bronze.ORG_ID`
    - Target Type: INT
    - Transformation: `CAST(ORG_ID AS INT)`
  - **created_by**
    - Source: `bronze.CREATED_BY`
    - Target Type: STRING
    - Transformation: `TRIM(CREATED_BY)`
  - **created_date**
    - Source: `bronze.CREATED_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(CREATED_DATE AS TIMESTAMP)`
  - **last_update_date**
    - Source: `bronze.LAST_UPDATE_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(LAST_UPDATE_DATE AS TIMESTAMP)`
- **Metadata Columns Added**:
  - `_pipeline_run_id` (STRING)
  - `_load_timestamp` (TIMESTAMP)
  - `_is_deleted` (BOOLEAN)
  - `_order_year_month` (STRING) - Derived for partitioning: `DATE_FORMAT(ORDER_DATE, 'yyyy-MM')`
- **Data Quality Rules**:
  - Rule ID: DQ-N-005 | Type: NULL_CHECK | Column: order_id | Expression: `order_id IS NOT NULL` | Severity: SKIP_ROW
  - Rule ID: DQ-N-006 | Type: NULL_CHECK | Column: customer_id | Expression: `customer_id IS NOT NULL` | Severity: SKIP_ROW
  - Rule ID: DQ-V-001 | Type: RANGE_CHECK | Column: total_amount | Expression: `total_amount > 0` | Severity: SKIP_ROW
  - Rule ID: DQ-RI-001 | Type: REFERENTIAL_INTEGRITY | Column: customer_id | Expression: `EXISTS (SELECT 1 FROM silver.customers c WHERE c.customer_id = source.customer_id)` | Severity: WARNING
- **MERGE Configuration**:
  - Merge Key: `order_id`
  - Match Condition: `target.order_id = source.order_id`
  - Delete Handling: Soft delete
- **Silver Layer Target**:
  - Path: `silver/src-001/erp/oe_order_headers_all/`
  - Partition Column(s): `_order_year_month` (Per Blueprint requirement for large transaction tables)
  - Z-Order Column(s): `customer_id`

### Entity: silver.oe_order_lines_all
- **Source Details**:
  - Bronze Table: `bronze/src-001/erp/oe_order_lines_all/`
  - Source System ID: SRC-001
  - Primary Key(s): `line_id`
  - SCD Type: 1
- **Column Transformation Specifications**:
  - **line_id**
    - Source: `bronze.LINE_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(LINE_ID AS BIGINT)`
  - **order_id**
    - Source: `bronze.ORDER_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(ORDER_ID AS BIGINT)`
  - **product_id**
    - Source: `bronze.PRODUCT_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(PRODUCT_ID AS BIGINT)`
  - **quantity**
    - Source: `bronze.QUANTITY`
    - Target Type: INT
    - Transformation: `CAST(QUANTITY AS INT)`
  - **unit_price**
    - Source: `bronze.UNIT_PRICE`
    - Target Type: DECIMAL(15,2)
    - Transformation: `CAST(UNIT_PRICE AS DECIMAL(15,2))`
  - **line_total**
    - Source: `bronze.LINE_TOTAL`
    - Target Type: DECIMAL(15,2)
    - Transformation: `CAST(LINE_TOTAL AS DECIMAL(15,2))`
  - **created_date**
    - Source: `bronze.CREATED_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(CREATED_DATE AS TIMESTAMP)`
  - **last_update_date**
    - Source: `bronze.LAST_UPDATE_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(LAST_UPDATE_DATE AS TIMESTAMP)`
- **Metadata Columns Added**:
  - `_pipeline_run_id` (STRING)
  - `_load_timestamp` (TIMESTAMP)
  - `_is_deleted` (BOOLEAN)
  - `_order_year_month` (STRING) - Derived via join to headers or passed from Bronze if available.
- **Data Quality Rules**:
  - Rule ID: DQ-N-007 | Type: NULL_CHECK | Column: line_id | Expression: `line_id IS NOT NULL` | Severity: SKIP_ROW
  - Rule ID: DQ-N-008 | Type: NULL_CHECK | Column: order_id | Expression: `order_id IS NOT NULL` | Severity: SKIP_ROW
  - Rule ID: DQ-R-003 | Type: RANGE_CHECK | Column: quantity | Expression: `quantity > 0` | Severity: SKIP_ROW
  - Rule ID: DQ-R-004 | Type: RANGE_CHECK | Column: unit_price | Expression: `unit_price >= 0` | Severity: SKIP_ROW
  - Rule ID: DQ-RI-003 | Type: REFERENTIAL_INTEGRITY | Column: order_id | Expression: `EXISTS (SELECT 1 FROM silver.oe_order_headers_all h WHERE h.order_id = source.order_id)` | Severity: WARNING
- **MERGE Configuration**:
  - Merge Key: `line_id`
  - Match Condition: `target.line_id = source.line_id`
  - Delete Handling: Soft delete
- **Silver Layer Target**:
  - Path: `silver/src-001/erp/oe_order_lines_all/`
  - Partition Column(s): `_order_year_month`
  - Z-Order Column(s): `order_id`, `product_id`

### Entity: silver.addresses
- **Source Details**:
  - Bronze Table: `bronze/src-001/erp/addresses/`
  - Source System ID: SRC-001
  - Primary Key(s): `address_id`
  - SCD Type: 1
- **Column Transformation Specifications**:
  - **address_id**
    - Source: `bronze.ADDRESS_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(ADDRESS_ID AS BIGINT)`
  - **customer_id**
    - Source: `bronze.CUSTOMER_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(CUSTOMER_ID AS BIGINT)`
  - **address_line1**
    - Source: `bronze.ADDRESS_LINE1`
    - Target Type: STRING
    - Transformation: `TRIM(ADDRESS_LINE1)`
  - **address_line2**
    - Source: `bronze.ADDRESS_LINE2`
    - Target Type: STRING
    - Transformation: `TRIM(ADDRESS_LINE2)`
  - **city**
    - Source: `bronze.CITY`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(CITY))`
  - **state**
    - Source: `bronze.STATE`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(STATE))`
  - **postal_code**
    - Source: `bronze.POSTAL_CODE`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(POSTAL_CODE))`
  - **country**
    - Source: `bronze.COUNTRY`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(COUNTRY))`
  - **created_date**
    - Source: `bronze.CREATED_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(CREATED_DATE AS TIMESTAMP)`
  - **last_update_date**
    - Source: `bronze.LAST_UPDATE_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(LAST_UPDATE_DATE AS TIMESTAMP)`
- **Metadata Columns Added**:
  - `_pipeline_run_id` (STRING)
  - `_load_timestamp` (TIMESTAMP)
  - `_is_deleted` (BOOLEAN)
- **Data Quality Rules**:
  - Rule ID: DQ-N-009 | Type: NULL_CHECK | Column: address_id | Expression: `address_id IS NOT NULL` | Severity: SKIP_ROW
  - Rule ID: DQ-N-010 | Type: NULL_CHECK | Column: customer_id | Expression: `customer_id IS NOT NULL` | Severity: SKIP_ROW
- **MERGE Configuration**:
  - Merge Key: `address_id`
  - Match Condition: `target.address_id = source.address_id`
  - Delete Handling: Soft delete
- **Silver Layer Target**:
  - Path: `silver/src-001/erp/addresses/`
  - Partition Column(s): None
  - Z-Order Column(s): `customer_id`, `city`, `state`

### Entity: silver.city_tier_master
- **Source Details**:
  - Bronze Table: `bronze/src-001/erp/city_tier_master/`
  - Source System ID: SRC-001
  - Primary Key(s): `city`, `state`
  - SCD Type: 1
- **Column Transformation Specifications**:
  - **city**
    - Source: `bronze.CITY`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(CITY))`
  - **state**
    - Source: `bronze.STATE`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(STATE))`
  - **tier**
    - Source: `bronze.TIER`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(TIER))`
  - **created_date**
    - Source: `bronze.CREATED_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(CREATED_DATE AS TIMESTAMP)`
- **Metadata Columns Added**:
  - `_pipeline_run_id` (STRING)
  - `_load_timestamp` (TIMESTAMP)
  - `_is_deleted` (BOOLEAN)
- **Data Quality Rules**:
  - Rule ID: DQ-N-011 | Type: NULL_CHECK | Column: city | Expression: `city IS NOT NULL` | Severity: SKIP_ROW
  - Rule ID: DQ-N-012 | Type: NULL_CHECK | Column: state | Expression: `state IS NOT NULL` | Severity: SKIP_ROW
- **MERGE Configuration**:
  - Merge Key: `city`, `state`
  - Match Condition: `target.city = source.city AND target.state = source.state`
  - Delete Handling: Soft delete
- **Silver Layer Target**:
  - Path: `silver/src-001/erp/city_tier_master/`
  - Partition Column(s): None
  - Z-Order Column(s): `city`, `state`

### Entity: silver.mtl_system_items_b
- **Source Details**:
  - Bronze Table: `bronze/src-001/erp/mtl_system_items_b/`
  - Source System ID: SRC-001
  - Primary Key(s): `inventory_item_id`
  - SCD Type: 1
- **Column Transformation Specifications**:
  - **inventory_item_id**
    - Source: `bronze.INVENTORY_ITEM_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(INVENTORY_ITEM_ID AS BIGINT)`
  - **category_id**
    - Source: `bronze.CATEGORY_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(CATEGORY_ID AS BIGINT)`
  - **brand_id**
    - Source: `bronze.BRAND_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(BRAND_ID AS BIGINT)`
  - **sku**
    - Source: `bronze.SKU`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(SKU))`
  - **product_name**
    - Source: `bronze.PRODUCT_NAME`
    - Target Type: STRING
    - Transformation: `TRIM(PRODUCT_NAME)`
  - **description**
    - Source: `bronze.DESCRIPTION`
    - Target Type: STRING
    - Transformation: `TRIM(DESCRIPTION)`
  - **unit_cost**
    - Source: `bronze.UNIT_COST`
    - Target Type: DECIMAL(15,2)
    - Transformation: `CAST(UNIT_COST AS DECIMAL(15,2))`
  - **created_date**
    - Source: `bronze.CREATED_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(CREATED_DATE AS TIMESTAMP)`
  - **last_update_date**
    - Source: `bronze.LAST_UPDATE_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(LAST_UPDATE_DATE AS TIMESTAMP)`
- **Metadata Columns Added**:
  - `_pipeline_run_id` (STRING)
  - `_load_timestamp` (TIMESTAMP)
  - `_is_deleted` (BOOLEAN)
- **Data Quality Rules**:
  - Rule ID: DQ-N-013 | Type: NULL_CHECK | Column: inventory_item_id | Expression: `inventory_item_id IS NOT NULL` | Severity: SKIP_ROW
  - Rule ID: DQ-R-005 | Type: RANGE_CHECK | Column: unit_cost | Expression: `unit_cost >= 0` | Severity: SKIP_ROW
- **MERGE Configuration**:
  - Merge Key: `inventory_item_id`
  - Match Condition: `target.inventory_item_id = source.inventory_item_id`
  - Delete Handling: Soft delete
- **Silver Layer Target**:
  - Path: `silver/src-001/erp/mtl_system_items_b/`
  - Partition Column(s): None
  - Z-Order Column(s): `category_id`, `brand_id`

### Entity: silver.categories
- **Source Details**:
  - Bronze Table: `bronze/src-001/erp/categories/`
  - Source System ID: SRC-001
  - Primary Key(s): `category_id`
  - SCD Type: 1
- **Column Transformation Specifications**:
  - **category_id**
    - Source: `bronze.CATEGORY_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(CATEGORY_ID AS BIGINT)`
  - **category_name**
    - Source: `bronze.CATEGORY_NAME`
    - Target Type: STRING
    - Transformation: `TRIM(CATEGORY_NAME)`
  - **parent_category_id**
    - Source: `bronze.PARENT_CATEGORY_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(PARENT_CATEGORY_ID AS BIGINT)`
  - **created_date**
    - Source: `bronze.CREATED_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(CREATED_DATE AS TIMESTAMP)`
- **Metadata Columns Added**:
  - `_pipeline_run_id` (STRING)
  - `_load_timestamp` (TIMESTAMP)
  - `_is_deleted` (BOOLEAN)
- **Data Quality Rules**:
  - Rule ID: DQ-N-014 | Type: NULL_CHECK | Column: category_id | Expression: `category_id IS NOT NULL` | Severity: SKIP_ROW
- **MERGE Configuration**:
  - Merge Key: `category_id`
  - Match Condition: `target.category_id = source.category_id`
  - Delete Handling: Soft delete
- **Silver Layer Target**:
  - Path: `silver/src-001/erp/categories/`
  - Partition Column(s): None
  - Z-Order Column(s): `category_id`

### Entity: silver.brands
- **Source Details**:
  - Bronze Table: `bronze/src-001/erp/brands/`
  - Source System ID: SRC-001
  - Primary Key(s): `brand_id`
  - SCD Type: 1
- **Column Transformation Specifications**:
  - **brand_id**
    - Source: `bronze.BRAND_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(BRAND_ID AS BIGINT)`
  - **brand_name**
    - Source: `bronze.BRAND_NAME`
    - Target Type: STRING
    - Transformation: `TRIM(BRAND_NAME)`
  - **created_date**
    - Source: `bronze.CREATED_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(CREATED_DATE AS TIMESTAMP)`
- **Metadata Columns Added**:
  - `_pipeline_run_id` (STRING)
  - `_load_timestamp` (TIMESTAMP)
  - `_is_deleted` (BOOLEAN)
- **Data Quality Rules**:
  - Rule ID: DQ-N-015 | Type: NULL_CHECK | Column: brand_id | Expression: `brand_id IS NOT NULL` | Severity: SKIP_ROW
- **MERGE Configuration**:
  - Merge Key: `brand_id`
  - Match Condition: `target.brand_id = source.brand_id`
  - Delete Handling: Soft delete
- **Silver Layer Target**:
  - Path: `silver/src-001/erp/brands/`
  - Partition Column(s): None
  - Z-Order Column(s): `brand_id`

### Entity: silver.marketing_campaigns
- **Source Details**:
  - Bronze Table: `bronze/src-003/marketing/marketing_campaigns/`
  - Source System ID: SRC-003
  - Primary Key(s): `campaign_id`
  - SCD Type: 1
- **Column Transformation Specifications**:
  - **campaign_id**
    - Source: `bronze.CAMPAIGN_ID`
    - Target Type: INT
    - Transformation: `CAST(CAMPAIGN_ID AS INT)`
  - **campaign_name**
    - Source: `bronze.CAMPAIGN_NAME`
    - Target Type: STRING
    - Transformation: `TRIM(CAMPAIGN_NAME)`
  - **campaign_code**
    - Source: `bronze.CAMPAIGN_CODE`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(CAMPAIGN_CODE))`
  - **channel**
    - Source: `bronze.CHANNEL`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(CHANNEL))`
  - **sub_channel**
    - Source: `bronze.SUB_CHANNEL`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(SUB_CHANNEL))`
  - **start_date**
    - Source: `bronze.START_DATE`
    - Target Type: DATE
    - Transformation: `CAST(START_DATE AS DATE)`
  - **end_date**
    - Source: `bronze.END_DATE`
    - Target Type: DATE
    - Transformation: `CAST(END_DATE AS DATE)`
  - **total_spend**
    - Source: `bronze.TOTAL_SPEND`
    - Target Type: DECIMAL(15,2)
    - Transformation: `CAST(TOTAL_SPEND AS DECIMAL(15,2))`
  - **customers_acquired**
    - Source: `bronze.CUSTOMERS_ACQUIRED`
    - Target Type: INT
    - Transformation: `CAST(CUSTOMERS_ACQUIRED AS INT)`
  - **status**
    - Source: `bronze.STATUS`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(STATUS))`
  - **created_date**
    - Source: `bronze.CREATED_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(CREATED_DATE AS TIMESTAMP)`
  - **last_update_date**
    - Source: `bronze.LAST_UPDATE_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(LAST_UPDATE_DATE AS TIMESTAMP)`
- **Metadata Columns Added**:
  - `_pipeline_run_id` (STRING)
  - `_load_timestamp` (TIMESTAMP)
  - `_is_deleted` (BOOLEAN)
- **Data Quality Rules**:
  - Rule ID: DQ-N-016 | Type: NULL_CHECK | Column: campaign_id | Expression: `campaign_id IS NOT NULL` | Severity: SKIP_ROW
  - Rule ID: DQ-R-006 | Type: RANGE_CHECK | Column: total_spend | Expression: `total_spend >= 0` | Severity: SKIP_ROW
- **MERGE Configuration**:
  - Merge Key: `campaign_id`
  - Match Condition: `target.campaign_id = source.campaign_id`
  - Delete Handling: Soft delete
- **Silver Layer Target**:
  - Path: `silver/src-003/marketing/marketing_campaigns/`
  - Partition Column(s): None
  - Z-Order Column(s): `campaign_id`, `channel`

### Entity: silver.incidents
- **Source Details**:
  - Bronze Table: `bronze/src-002/crm/incidents/`
  - Source System ID: SRC-002
  - Primary Key(s): `incident_id`
  - SCD Type: 1
- **Column Transformation Specifications**:
  - **incident_id**
    - Source: `bronze.INCIDENT_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(INCIDENT_ID AS BIGINT)`
  - **customer_id**
    - Source: `bronze.CUSTOMER_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(CUSTOMER_ID AS BIGINT)`
  - **order_id**
    - Source: `bronze.ORDER_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(ORDER_ID AS BIGINT)`
  - **status**
    - Source: `bronze.STATUS`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(STATUS))`
  - **priority**
    - Source: `bronze.PRIORITY`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(PRIORITY))`
  - **created_date**
    - Source: `bronze.CREATED_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(CREATED_DATE AS TIMESTAMP)`
  - **last_update_date**
    - Source: `bronze.LAST_UPDATE_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(LAST_UPDATE_DATE AS TIMESTAMP)`
- **Metadata Columns Added**:
  - `_pipeline_run_id` (STRING)
  - `_load_timestamp` (TIMESTAMP)
  - `_is_deleted` (BOOLEAN)
- **Data Quality Rules**:
  - Rule ID: DQ-N-017 | Type: NULL_CHECK | Column: incident_id | Expression: `incident_id IS NOT NULL` | Severity: SKIP_ROW
  - Rule ID: DQ-N-018 | Type: NULL_CHECK | Column: customer_id | Expression: `customer_id IS NOT NULL` | Severity: SKIP_ROW
- **MERGE Configuration**:
  - Merge Key: `incident_id`
  - Match Condition: `target.incident_id = source.incident_id`
  - Delete Handling: Soft delete
- **Silver Layer Target**:
  - Path: `silver/src-002/crm/incidents/`
  - Partition Column(s): None
  - Z-Order Column(s): `customer_id`, `order_id`

### Entity: silver.interactions
- **Source Details**:
  - Bronze Table: `bronze/src-002/crm/interactions/`
  - Source System ID: SRC-002
  - Primary Key(s): `interaction_id`
  - SCD Type: 1
- **Column Transformation Specifications**:
  - **interaction_id**
    - Source: `bronze.INTERACTION_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(INTERACTION_ID AS BIGINT)`
  - **incident_id**
    - Source: `bronze.INCIDENT_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(INCIDENT_ID AS BIGINT)`
  - **interaction_type**
    - Source: `bronze.INTERACTION_TYPE`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(INTERACTION_TYPE))`
  - **interaction_date**
    - Source: `bronze.INTERACTION_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(INTERACTION_DATE AS TIMESTAMP)`
  - **notes**
    - Source: `bronze.NOTES`
    - Target Type: STRING
    - Transformation: `TRIM(NOTES)`
- **Metadata Columns Added**:
  - `_pipeline_run_id` (STRING)
  - `_load_timestamp` (TIMESTAMP)
  - `_is_deleted` (BOOLEAN)
- **Data Quality Rules**:
  - Rule ID: DQ-N-019 | Type: NULL_CHECK | Column: interaction_id | Expression: `interaction_id IS NOT NULL` | Severity: SKIP_ROW
  - Rule ID: DQ-N-020 | Type: NULL_CHECK | Column: incident_id | Expression: `incident_id IS NOT NULL` | Severity: SKIP_ROW
- **MERGE Configuration**:
  - Merge Key: `interaction_id`
  - Match Condition: `target.interaction_id = source.interaction_id`
  - Delete Handling: Soft delete
- **Silver Layer Target**:
  - Path: `silver/src-002/crm/interactions/`
  - Partition Column(s): None
  - Z-Order Column(s): `incident_id`

### Entity: silver.surveys
- **Source Details**:
  - Bronze Table: `bronze/src-002/crm/surveys/`
  - Source System ID: SRC-002
  - Primary Key(s): `survey_id`
  - SCD Type: 1
- **Column Transformation Specifications**:
  - **survey_id**
    - Source: `bronze.SURVEY_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(SURVEY_ID AS BIGINT)`
  - **customer_id**
    - Source: `bronze.CUSTOMER_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(CUSTOMER_ID AS BIGINT)`
  - **order_id**
    - Source: `bronze.ORDER_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(ORDER_ID AS BIGINT)`
  - **incident_id**
    - Source: `bronze.INCIDENT_ID`
    - Target Type: BIGINT
    - Transformation: `CAST(INCIDENT_ID AS BIGINT)`
  - **survey_type**
    - Source: `bronze.SURVEY_TYPE`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(SURVEY_TYPE))`
  - **nps_score**
    - Source: `bronze.NPS_SCORE`
    - Target Type: INT
    - Transformation: `CAST(NPS_SCORE AS INT)`
  - **csat_score**
    - Source: `bronze.CSAT_SCORE`
    - Target Type: INT
    - Transformation: `CAST(CSAT_SCORE AS INT)`
  - **nps_category**
    - Source: `bronze.NPS_CATEGORY`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(NPS_CATEGORY))`
  - **feedback_text**
    - Source: `bronze.FEEDBACK_TEXT`
    - Target Type: STRING
    - Transformation: `TRIM(FEEDBACK_TEXT)`
  - **feedback_category**
    - Source: `bronze.FEEDBACK_CATEGORY`
    - Target Type: STRING
    - Transformation: `UPPER(TRIM(FEEDBACK_CATEGORY))`
  - **response_date**
    - Source: `bronze.RESPONSE_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(RESPONSE_DATE AS TIMESTAMP)`
  - **survey_sent_date**
    - Source: `bronze.SURVEY_SENT_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(SURVEY_SENT_DATE AS TIMESTAMP)`
  - **created_date**
    - Source: `bronze.CREATED_DATE`
    - Target Type: TIMESTAMP
    - Transformation: `CAST(CREATED_DATE AS TIMESTAMP)`
- **Metadata Columns Added**:
  - `_pipeline_run_id` (STRING)
  - `_load_timestamp` (TIMESTAMP)
  - `_is_deleted` (BOOLEAN)
- **Data Quality Rules**:
  - Rule ID: DQ-N-021 | Type: NULL_CHECK | Column: survey_id | Expression: `survey_id IS NOT NULL` | Severity: SKIP_ROW
  - Rule ID: DQ-N-022 | Type: NULL_CHECK | Column: customer_id | Expression: `customer_id IS NOT NULL` | Severity: SKIP_ROW
  - Rule ID: DQ-R-001 | Type: RANGE_CHECK | Column: nps_score | Expression: `nps_score BETWEEN 0 AND 10 OR nps_score IS NULL` | Severity: SKIP_ROW
  - Rule ID: DQ-R-002 | Type: RANGE_CHECK | Column: csat_score | Expression: `csat_score BETWEEN 1 AND 5 OR csat_score IS NULL` | Severity: SKIP_ROW
- **MERGE Configuration**:
  - Merge Key: `survey_id`
  - Match Condition: `target.survey_id = source.survey_id`
  - Delete Handling: Soft delete
- **Silver Layer Target**:
  - Path: `silver/src-002/crm/surveys/`
  - Partition Column(s): None
  - Z-Order Column(s): `customer_id`, `order_id`

---

## 9. Data Flow Diagrams

### 9.1 Silver Initial Load Flow
```text
Bronze Initial Load (Parquet)
    Path: bronze/.../initial/{run_id}/
    │
    │ Read Parquet files
    ▼
[Schema Validation]
    │
    │ Check columns, types against Unity Catalog
    ▼
[Data Quality Validation]
    │
    ├── Valid rows → Continue
    └── Invalid rows → Write to control.dq_exception_log (DLQ)
    │
    ▼
[Apply Transformations]
    - Type conversions (CAST)
    - Business rules (COALESCE, TRIM, UPPER)
    - Standardization
    │
    ▼
[Add Metadata Columns]
    - _pipeline_run_id = @pipeline_run_id
    - _load_timestamp = CURRENT_TIMESTAMP
    - _is_deleted = false
    - _is_current = true (for SCD2)
    │
    ▼
[MERGE INTO Silver Delta Table]
    │
    ▼
[Post-Processing]
    - OPTIMIZE with Z-Order
    - ANALYZE TABLE COMPUTE STATISTICS
    │
    ▼
[Update Control Table]
    SET silver_initial_load_completed = 1
```

### 9.2 Silver Incremental CDC Flow
```text
Bronze CDC Changes (Parquet)
    Path: bronze/.../incremental/{run_id}/
    Columns: _cdc_operation (I/U/D), _cdc_version
    │
    │ Read CDC batch
    ▼
[Parse CDC Operations]
    - Deduplicate source batch keeping latest _cdc_version per PK
    │
    ▼
[Data Quality Validation]
    │
    ├── Valid → Continue
    └── Invalid → DLQ
    │
    ▼
[Apply Transformations]
    │
    ▼
[Execute MERGE with SCD Logic]
    │
    ├── DELETE (D) → SET _is_deleted = true
    │
    ├── UPDATE (U)
    │   ├── SCD Type 1 → Overwrite non-key columns
    │   └── SCD Type 2 → Close old (_valid_to = NOW), Insert new (_valid_from = NOW)
    │
    └── INSERT (I) → Insert new record
    │
    ▼
[Update Control Table]
    SET silver_last_load_timestamp = NOW()
```

---

## 10. Error Handling & Recovery

### 10.1 Error Categories
- **Transient Errors**: Network timeout, Databricks cluster provisioning delay.
  - Action: ADF automated retry with exponential backoff.
- **Data Quality Errors**: Validation failure (e.g., `TOTAL_AMOUNT` < 0).
  - Action: Row routed to DLQ (`control.dq_exception_log`). Pipeline continues unless error threshold (5%) is breached.
- **Schema Evolution**: New or removed columns in Bronze.
  - Action: Pipeline fails. Schema evolution is intentional; requires manual review and explicit DDL update in Unity Catalog.
- **MERGE Conflict**: Concurrent updates to the same Delta table.
  - Action: Databricks native retry with isolation level guarantees.

### 10.2 Retry Configuration
- **Max Retries**: 3
- **Base Delay**: 300 seconds (5 minutes)
- **Backoff**: Exponential
- **Idempotency**: Ensured via `MERGE` semantics and `pipeline_run_id`. Re-running a failed job will not duplicate data.

### 10.3 Recovery Procedures
- **Initial Load Failed**: Re-run pipeline. `silver_initial_load_completed` remains 0.
- **Incremental Failed**: Re-run pipeline. Delta MERGE ensures no duplicates.
- **DQ Exceptions**: Data Engineering team reviews `control.dq_exception_log`, fixes source system data, and re-ingests via Bronze.

---

## 11. Storage Design

### 11.1 Silver Layer Storage
- **Container/Path**: `silver/`
- **Structure**: `silver/{source_system}/{schema}/{table}/`
- **Format**: Delta Lake
- **Catalog**: Unity Catalog (`catalog.silver.table_name`)

### 11.2 Delta Lake Configuration
- **Auto Optimize**: Enabled (`spark.databricks.delta.optimizeWrite.enabled = true`)
- **Auto Compaction**: Enabled (`spark.databricks.delta.autoCompact.enabled = true`)
- **VACUUM Retention**: 168 hours (7 days)
- **Change Data Feed (CDF)**: Enabled for `silver.customers` and `silver.oe_order_headers_all` to support downstream Gold layer incremental processing.

### 11.3 Partitioning Strategy
- **silver.oe_order_headers_all**: Partitioned by `_order_year_month` (Derived from `ORDER_DATE`). Rationale: Large transaction volume, downstream queries frequently filter by month/year.
- **silver.oe_order_lines_all**: Partitioned by `_order_year_month`. Rationale: Aligns with headers for efficient join pruning.
- **All other tables**: Unpartitioned. Rationale: Dimension tables and smaller fact tables do not benefit from partitioning and would suffer from the small-file problem.

### 11.4 Z-Ordering Strategy
- Applied to high-cardinality foreign keys used heavily in downstream Gold layer joins (e.g., `customer_id`, `campaign_id`, `product_id`). Detailed per entity in Section 8.

---

## 12. Security Specifications

### 12.1 Authentication
- **Compute to Storage**: Databricks accesses ADLS Gen2 via Azure Service Principal.
- **Orchestration to Compute**: ADF triggers Databricks jobs via Managed Identity.

### 12.2 Data Security
- **Dynamic Data Masking**: Configured in Unity Catalog for `silver.customers`.
  - Masked Columns: `email`, `phone`, `first_name`, `last_name`.
  - Policy: Only users in the `HR_Support` Entra ID group can view unmasked data. Analytics users see obfuscated values (e.g., `***@***.com`).
- **Encryption**: Data encrypted at rest using ADLS Gen2 AES-256.

---

## 13. Monitoring & Alerting

### 13.1 Key Metrics
- **Transform Duration**: Tracked vs historical baseline.
- **Records Processed**: Insert/Update/Delete counts logged to `control.table_metadata`.
- **DQ Exception Rate**: Percentage of rows routed to DLQ per batch.
- **Delta Table Size**: Monitored to trigger manual OPTIMIZE if auto-compaction falls behind.

### 13.2 Alerting Rules
- **Pipeline Failure**: Immediate email/Teams alert via Azure Logic Apps.
- **DQ Exception Rate > 5%**: Warning alert to Data Stewards.
- **100% Null Circuit Breaker**: Critical alert; pipeline halted.
- **No Data Processed for 24h**: Warning alert for critical tables (`oe_order_headers_all`).

---

## 14. Deployment Specifications

### 14.1 Deployment Order
- Execute DDL scripts to extend `control.table_metadata` and create `control.dq_exception_log`.
- Deploy Unity Catalog schemas and table definitions (with masking policies).
- Deploy Databricks processing notebooks (`NB_Silver_Initial_Load`, `NB_Silver_Incremental_CDC`).
- Deploy ADF Pipelines (`PL_Silver_Orchestrator`, etc.).
- Execute initial load for all Dimension tables.
- Execute initial load for all Fact tables.

### 14.2 Validation Checklist
- [ ] Control tables extended with `silver_*` columns.
- [ ] DQ exception table created and accessible.
- [ ] Unity Catalog masking policies applied to `silver.customers`.
- [ ] Databricks notebooks deployed and linked to ADF.
- [ ] Initial load tested for `silver.customers` (SCD2 validation).
- [ ] CDC processing tested with simulated updates and deletes.

---

## 15. Appendix

### 15.1 Bronze → Silver Column Mapping Summary
- **Entity: silver.customers**
  - **customer_id**
    - Source: bronze.CUSTOMER_ID
    - Transformation: CAST
    - Data Type: BIGINT
  - **email**
    - Source: bronze.EMAIL
    - Transformation: LOWER, TRIM
    - Data Type: STRING
  - **status**
    - Source: bronze.STATUS
    - Transformation: UPPER, TRIM
    - Data Type: STRING
- **Entity: silver.oe_order_headers_all**
  - **order_id**
    - Source: bronze.ORDER_ID
    - Transformation: CAST
    - Data Type: BIGINT
  - **total_amount**
    - Source: bronze.TOTAL_AMOUNT
    - Transformation: CAST
    - Data Type: DECIMAL(15,2)
  - **order_status**
    - Source: bronze.ORDER_STATUS
    - Transformation: UPPER, TRIM
    - Data Type: STRING
*(Note: Full column mappings are detailed exhaustively in Section 8 Entity Specifications)*

### 15.2 Data Quality Rules Summary (COMPLETE - PRODUCTION READY)
- **Rule ID**: DQ-N-001
  - **Entity**: silver.customers
  - **Column**: customer_id
  - **Rule Type**: NULL_CHECK
  - **Expression**: `customer_id IS NOT NULL`
  - **Severity**: ERROR
  - **Source**: Blueprint Circuit Breaker Requirement
- **Rule ID**: DQ-N-002
  - **Entity**: silver.customers
  - **Column**: email
  - **Rule Type**: NULL_CHECK
  - **Expression**: `email IS NOT NULL`
  - **Severity**: SKIP_ROW
  - **Source**: Metadata Required Field
- **Rule ID**: DQ-F-001
  - **Entity**: silver.customers
  - **Column**: email
  - **Rule Type**: FORMAT_CHECK
  - **Expression**: `email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'`
  - **Severity**: WARNING
  - **Source**: Blueprint PII/Format Requirement
- **Rule ID**: DQ-N-003
  - **Entity**: silver.customer_registration_source
  - **Column**: registration_source_id
  - **Rule Type**: NULL_CHECK
  - **Expression**: `registration_source_id IS NOT NULL`
  - **Severity**: SKIP_ROW
  - **Source**: Metadata PK Constraint
- **Rule ID**: DQ-N-004
  - **Entity**: silver.customer_registration_source
  - **Column**: customer_id
  - **Rule Type**: NULL_CHECK
  - **Expression**: `customer_id IS NOT NULL`
  - **Severity**: SKIP_ROW
  - **Source**: Metadata FK Constraint
- **Rule ID**: DQ-RI-002
  - **Entity**: silver.customer_registration_source
  - **Column**: customer_id
  - **Rule Type**: REFERENTIAL_INTEGRITY
  - **Expression**: `EXISTS (SELECT 1 FROM silver.customers c WHERE c.customer_id = source.customer_id)`
  - **Severity**: WARNING
  - **Source**: Metadata FK Constraint
- **Rule ID**: DQ-N-005
  - **Entity**: silver.oe_order_headers_all
  - **Column**: order_id
  - **Rule Type**: NULL_CHECK
  - **Expression**: `order_id IS NOT NULL`
  - **Severity**: SKIP_ROW
  - **Source**: Metadata PK Constraint
- **Rule ID**: DQ-N-006
  - **Entity**: silver.oe_order_headers_all
  - **Column**: customer_id
  - **Rule Type**: NULL_CHECK
  - **Expression**: `customer_id IS NOT NULL`
  - **Severity**: SKIP_ROW
  - **Source**: Metadata FK Constraint
- **Rule ID**: DQ-V-001
  - **Entity**: silver.oe_order_headers_all
  - **Column**: total_amount
  - **Rule Type**: RANGE_CHECK
  - **Expression**: `total_amount > 0`
  - **Severity**: SKIP_ROW
  - **Source**: Blueprint Rule DQ-V-001
- **Rule ID**: DQ-RI-001
  - **Entity**: silver.oe_order_headers_all
  - **Column**: customer_id
  - **Rule Type**: REFERENTIAL_INTEGRITY
  - **Expression**: `EXISTS (SELECT 1 FROM silver.customers c WHERE c.customer_id = source.customer_id)`
  - **Severity**: WARNING
  - **Source**: Metadata FK Constraint
- **Rule ID**: DQ-N-007
  - **Entity**: silver.oe_order_lines_all
  - **Column**: line_id
  - **Rule Type**: NULL_CHECK
  - **Expression**: `line_id IS NOT NULL`
  - **Severity**: SKIP_ROW
  - **Source**: Metadata PK Constraint
- **Rule ID**: DQ-N-008
  - **Entity**: silver.oe_order_lines_all
  - **Column**: order_id
  - **Rule Type**: NULL_CHECK
  - **Expression**: `order_id IS NOT NULL`
  - **Severity**: SKIP_ROW
  - **Source**: Metadata FK Constraint
- **Rule ID**: DQ-R-003
  - **Entity**: silver.oe_order_lines_all
  - **Column**: quantity
  - **Rule Type**: RANGE_CHECK
  - **Expression**: `quantity > 0`
  - **Severity**: SKIP_ROW
  - **Source**: Blueprint Business Logic
- **Rule ID**: DQ-R-004
  - **Entity**: silver.oe_order_lines_all
  - **Column**: unit_price
  - **Rule Type**: RANGE_CHECK
  - **Expression**: `unit_price >= 0`
  - **Severity**: SKIP_ROW
  - **Source**: Blueprint Business Logic
- **Rule ID**: DQ-RI-003
  - **Entity**: silver.oe_order_lines_all
  - **Column**: order_id
  - **Rule Type**: REFERENTIAL_INTEGRITY
  - **Expression**: `EXISTS (SELECT 1 FROM silver.oe_order_headers_all h WHERE h.order_id = source.order_id)`
  - **Severity**: WARNING
  - **Source**: Metadata FK Constraint
- **Rule ID**: DQ-N-009
  - **Entity**: silver.addresses
  - **Column**: address_id
  - **Rule Type**: NULL_CHECK
  - **Expression**: `address_id IS NOT NULL`
  - **Severity**: SKIP_ROW
  - **Source**: Metadata PK Constraint
- **Rule ID**: DQ-N-010
  - **Entity**: silver.addresses
  - **Column**: customer_id
  - **Rule Type**: NULL_CHECK
  - **Expression**: `customer_id IS NOT NULL`
  - **Severity**: SKIP_ROW
  - **Source**: Metadata FK Constraint
- **Rule ID**: DQ-N-011
  - **Entity**: silver.city_tier_master
  - **Column**: city
  - **Rule Type**: NULL_CHECK
  - **Expression**: `city IS NOT NULL`
  - **Severity**: SKIP_ROW
  - **Source**: Metadata PK Constraint
- **Rule ID**: DQ-N-012
  - **Entity**: silver.city_tier_master
  - **Column**: state
  - **Rule Type**: NULL_CHECK
  - **Expression**: `state IS NOT NULL`
  - **Severity**: SKIP_ROW
  - **Source**: Metadata PK Constraint
- **Rule ID**: DQ-N-013
  - **Entity**: silver.mtl_system_items_b
  - **Column**: inventory_item_id
  - **Rule Type**: NULL_CHECK
  - **Expression**: `inventory_item_id IS NOT NULL`
  - **Severity**: SKIP_ROW
  - **Source**: Metadata PK Constraint
- **Rule ID**: DQ-R-005
  - **Entity**: silver.mtl_system_items_b
  - **Column**: unit_cost
  - **Rule Type**: RANGE_CHECK
  - **Expression**: `unit_cost >= 0`
  - **Severity**: SKIP_ROW
  - **Source**: Blueprint Business Logic
- **Rule ID**: DQ-N-014
  - **Entity**: silver.categories
  - **Column**: category_id
  - **Rule Type**: NULL_CHECK
  - **Expression**: `category_id IS NOT NULL`
  - **Severity**: SKIP_ROW
  - **Source**: Metadata PK Constraint
- **Rule ID**: DQ-N-015
  - **Entity**: silver.brands
  - **Column**: brand_id
  - **Rule Type**: NULL_CHECK
  - **Expression**: `brand_id IS NOT NULL`
  - **Severity**: SKIP_ROW
  - **Source**: Metadata PK Constraint
- **Rule ID**: DQ-N-016
  - **Entity**: silver.marketing_campaigns
  - **Column**: campaign_id
  - **Rule Type**: NULL_CHECK
  - **Expression**: `campaign_id IS NOT NULL`
  - **Severity**: SKIP_ROW
  - **Source**: Metadata PK Constraint
- **Rule ID**: DQ-R-006
  - **Entity**: silver.marketing_campaigns
  - **Column**: total_spend
  - **Rule Type**: RANGE_CHECK
  - **Expression**: `total_spend >= 0`
  - **Severity**: SKIP_ROW
  - **Source**: Blueprint Business Logic
- **Rule ID**: DQ-N-017
  - **Entity**: silver.incidents
  - **Column**: incident_id
  - **Rule Type**: NULL_CHECK
  - **Expression**: `incident_id IS NOT NULL`
  - **Severity**: SKIP_ROW
  - **Source**: Metadata PK Constraint
- **Rule ID**: DQ-N-018
  - **Entity**: silver.incidents
  - **Column**: customer_id
  - **Rule Type**: NULL_CHECK
  - **Expression**: `customer_id IS NOT NULL`
  - **Severity**: SKIP_ROW
  - **Source**: Metadata FK Constraint
- **Rule ID**: DQ-N-019
  - **Entity**: silver.interactions
  - **Column**: interaction_id
  - **Rule Type**: NULL_CHECK
  - **Expression**: `interaction_id IS NOT NULL`
  - **Severity**: SKIP_ROW
  - **Source**: Metadata PK Constraint
- **Rule ID**: DQ-N-020
  - **Entity**: silver.interactions
  - **Column**: incident_id
  - **Rule Type**: NULL_CHECK
  - **Expression**: `incident_id IS NOT NULL`
  - **Severity**: SKIP_ROW
  - **Source**: Metadata FK Constraint
- **Rule ID**: DQ-N-021
  - **Entity**: silver.surveys
  - **Column**: survey_id
  - **Rule Type**: NULL_CHECK
  - **Expression**: `survey_id IS NOT NULL`
  - **Severity**: SKIP_ROW
  - **Source**: Metadata PK Constraint
- **Rule ID**: DQ-N-022
  - **Entity**: silver.surveys
  - **Column**: customer_id
  - **Rule Type**: NULL_CHECK
  - **Expression**: `customer_id IS NOT NULL`
  - **Severity**: SKIP_ROW
  - **Source**: Metadata FK Constraint
- **Rule ID**: DQ-R-001
  - **Entity**: silver.surveys
  - **Column**: nps_score
  - **Rule Type**: RANGE_CHECK
  - **Expression**: `nps_score BETWEEN 0 AND 10 OR nps_score IS NULL`
  - **Severity**: SKIP_ROW
  - **Source**: Blueprint Business Logic
- **Rule ID**: DQ-R-002
  - **Entity**: silver.surveys
  - **Column**: csat_score
  - **Rule Type**: RANGE_CHECK
  - **Expression**: `csat_score BETWEEN 1 AND 5 OR csat_score IS NULL`
  - **Severity**: SKIP_ROW
  - **Source**: Blueprint Business Logic

### 15.3 SCD Configuration Summary
- **Entity: silver.customers**
  - SCD Type: 2 (History)
  - Business Key: `customer_id`
  - Tracked Columns: `status`, `customer_type`, `marketing_opt_in`
  - Hash Columns: `status`, `customer_type`, `marketing_opt_in`
- **Entity: silver.customer_registration_source**
  - SCD Type: 1 (Overwrite)
  - Business Key: `registration_source_id`
- **Entity: silver.oe_order_headers_all**
  - SCD Type: 1 (Overwrite)
  - Business Key: `order_id`
- **Entity: silver.oe_order_lines_all**
  - SCD Type: 1 (Overwrite)
  - Business Key: `line_id`
- **Entity: silver.addresses**
  - SCD Type: 1 (Overwrite)
  - Business Key: `address_id`
- **Entity: silver.city_tier_master**
  - SCD Type: 1 (Overwrite)
  - Business Key: `city`, `state`
- **Entity: silver.mtl_system_items_b**
  - SCD Type: 1 (Overwrite)
  - Business Key: `inventory_item_id`
- **Entity: silver.categories**
  - SCD Type: 1 (Overwrite)
  - Business Key: `category_id`
- **Entity: silver.brands**
  - SCD Type: 1 (Overwrite)
  - Business Key: `brand_id`
- **Entity: silver.marketing_campaigns**
  - SCD Type: 1 (Overwrite)
  - Business Key: `campaign_id`
- **Entity: silver.incidents**
  - SCD Type: 1 (Overwrite)
  - Business Key: `incident_id`
- **Entity: silver.interactions**
  - SCD Type: 1 (Overwrite)
  - Business Key: `interaction_id`
- **Entity: silver.surveys**
  - SCD Type: 1 (Overwrite)
  - Business Key: `survey_id`

### 15.4 Glossary
- **CDC**: Change Data Capture
- **CDF**: Change Data Feed (Delta Lake feature)
- **DLQ**: Dead Letter Queue
- **DQ**: Data Quality
- **MERGE**: Delta Lake upsert operation ensuring idempotency
- **SCD**: Slowly Changing Dimension