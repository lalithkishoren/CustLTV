-- Create schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'control')
BEGIN
    EXEC('CREATE SCHEMA [control]');
END
GO

-- 1. Silver Table Configuration
IF NOT EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID('control.silver_table_config'))
BEGIN
    CREATE TABLE control.silver_table_config (
        silver_table_id INT IDENTITY(1,1) PRIMARY KEY,
        source_bronze_path VARCHAR(500) NOT NULL,
        target_silver_path VARCHAR(500) NOT NULL,
        table_name VARCHAR(255) NOT NULL,
        primary_key_columns VARCHAR(255) NOT NULL,
        scd_type INT NOT NULL DEFAULT 1,
        partition_columns VARCHAR(500) NULL,
        z_order_columns VARCHAR(500) NULL,
        transformation_type VARCHAR(50) NOT NULL DEFAULT 'TRANSFORM',
        is_active BIT NOT NULL DEFAULT 1,
        load_priority INT NOT NULL DEFAULT 100,
        last_processed_version BIGINT NULL,
        last_load_status VARCHAR(50) NULL,
        last_load_timestamp DATETIME2 NULL,
        last_pipeline_run_id VARCHAR(100) NULL,
        created_date DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        modified_date DATETIME2 NOT NULL DEFAULT GETUTCDATE()
    );
END
GO

-- 2. Silver Transformation Rules
IF NOT EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID('control.silver_transformation_rules'))
BEGIN
    CREATE TABLE control.silver_transformation_rules (
        rule_id INT IDENTITY(1,1) PRIMARY KEY,
        silver_table_id INT NOT NULL,
        rule_name VARCHAR(100) NOT NULL,
        rule_type VARCHAR(50) NOT NULL, -- 'FILTER', 'TRANSFORM', 'RENAME', 'CAST', 'DEDUPE'
        source_column VARCHAR(255) NULL,
        target_column VARCHAR(255) NULL,
        transformation_expression VARCHAR(MAX) NULL,
        execution_order INT NOT NULL DEFAULT 10,
        is_active BIT NOT NULL DEFAULT 1,
        CONSTRAINT FK_SilverTransform_TableId FOREIGN KEY (silver_table_id) REFERENCES control.silver_table_config(silver_table_id)
    );
END
GO

-- 3. Silver Execution Log
IF NOT EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID('control.silver_execution_log'))
BEGIN
    CREATE TABLE control.silver_execution_log (
        log_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        pipeline_run_id VARCHAR(100) NOT NULL,
        silver_table_id INT NOT NULL,
        records_read BIGINT NOT NULL DEFAULT 0,
        records_written BIGINT NOT NULL DEFAULT 0,
        records_filtered BIGINT NOT NULL DEFAULT 0,
        records_quarantined BIGINT NOT NULL DEFAULT 0,
        execution_status VARCHAR(50) NOT NULL,
        error_message NVARCHAR(MAX) NULL,
        start_time DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        end_time DATETIME2 NULL
    );
END
GO

-- 4. Data Quality Exception Log (DLQ / Quarantine Metadata)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID('control.dq_exception_log'))
BEGIN
    CREATE TABLE control.dq_exception_log (
        exception_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        silver_table_id INT NOT NULL,
        pipeline_run_id VARCHAR(100) NOT NULL,
        rule_id VARCHAR(50) NOT NULL,
        rule_type VARCHAR(50) NOT NULL,
        column_name VARCHAR(255) NULL,
        error_message VARCHAR(MAX) NOT NULL,
        row_data NVARCHAR(MAX) NOT NULL,
        exception_timestamp DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        severity VARCHAR(20) NOT NULL,
        is_resolved BIT NOT NULL DEFAULT 0,
        CONSTRAINT FK_DQException_TableId FOREIGN KEY (silver_table_id) REFERENCES control.silver_table_config(silver_table_id)
    );
END
GO