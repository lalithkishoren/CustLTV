-- Create schema if not exists
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'control')
BEGIN
    EXEC('CREATE SCHEMA [control]');
END
GO

-- 1. Gold Table Configuration
CREATE TABLE control.gold_table_config (
    gold_table_id INT IDENTITY(1,1) PRIMARY KEY,
    source_silver_tables VARCHAR(500) NOT NULL,
    target_gold_table VARCHAR(255) NOT NULL,
    table_type VARCHAR(50) NOT NULL CHECK (table_type IN ('FACT', 'DIMENSION', 'AGGREGATE', 'KPI')),
    refresh_frequency VARCHAR(50) NOT NULL CHECK (refresh_frequency IN ('DAILY', 'HOURLY', 'REAL_TIME', 'WEEKLY')),
    is_active BIT DEFAULT 1,
    last_refresh_timestamp DATETIME2 NULL,
    last_refresh_status VARCHAR(50) NULL,
    created_date DATETIME2 DEFAULT GETUTCDATE(),
    modified_date DATETIME2 DEFAULT GETUTCDATE()
);
GO

-- 2. Gold Aggregation Rules
CREATE TABLE control.gold_aggregation_rules (
    rule_id INT IDENTITY(1,1) PRIMARY KEY,
    gold_table_id INT NOT NULL FOREIGN KEY REFERENCES control.gold_table_config(gold_table_id),
    aggregation_name VARCHAR(255) NOT NULL,
    aggregation_type VARCHAR(50) NOT NULL CHECK (aggregation_type IN ('SUM', 'COUNT', 'AVG', 'MIN', 'MAX', 'DISTINCT_COUNT', 'CUSTOM')),
    source_column VARCHAR(255) NULL,
    target_column VARCHAR(255) NOT NULL,
    group_by_columns VARCHAR(500) NULL,
    filter_expression VARCHAR(1000) NULL,
    is_active BIT DEFAULT 1
);
GO

-- 3. Gold Dimension Configuration
CREATE TABLE control.gold_dimension_config (
    dimension_id INT IDENTITY(1,1) PRIMARY KEY,
    gold_table_id INT NOT NULL FOREIGN KEY REFERENCES control.gold_table_config(gold_table_id),
    scd_type INT NOT NULL CHECK (scd_type IN (1, 2, 3)),
    business_key_columns VARCHAR(500) NOT NULL,
    tracked_columns VARCHAR(MAX) NULL,
    surrogate_key_column VARCHAR(255) NOT NULL,
    unknown_member_key INT DEFAULT -1
);
GO

-- 4. Gold Execution Log
CREATE TABLE control.gold_execution_log (
    log_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    pipeline_run_id VARCHAR(100) NOT NULL,
    gold_table_id INT NOT NULL FOREIGN KEY REFERENCES control.gold_table_config(gold_table_id),
    records_processed BIGINT DEFAULT 0,
    execution_status VARCHAR(50) NOT NULL,
    start_time DATETIME2 NOT NULL,
    end_time DATETIME2 NOT NULL,
    error_message NVARCHAR(MAX) NULL
);
GO