-- Create control schema if it does not exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'control')
BEGIN
    EXEC('CREATE SCHEMA [control]');
END
GO

-- 1. Silver Table Configuration
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('control') AND name = 'silver_table_config')
BEGIN
    CREATE TABLE [control].[silver_table_config] (
        silver_table_id INT IDENTITY(1,1) PRIMARY KEY,
        source_system VARCHAR(50) NOT NULL,
        schema_name VARCHAR(100) NOT NULL,
        source_bronze_table VARCHAR(255) NOT NULL,
        target_silver_table VARCHAR(255) NOT NULL,
        primary_key_columns VARCHAR(500) NOT NULL,
        scd_type INT NOT NULL DEFAULT 1, -- 1 = Overwrite, 2 = History
        track_history_columns VARCHAR(1000) NULL,
        partition_columns VARCHAR(500) NULL,
        z_order_columns VARCHAR(500) NULL,
        is_active BIT NOT NULL DEFAULT 1,
        last_pipeline_run_id VARCHAR(100) NULL,
        last_load_status VARCHAR(50) NULL,
        last_load_timestamp DATETIME2 NULL,
        records_read BIGINT DEFAULT 0,
        records_written BIGINT DEFAULT 0,
        records_filtered BIGINT DEFAULT 0,
        created_date DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        modified_date DATETIME2 NOT NULL DEFAULT GETUTCDATE()
    );
END
GO

-- 2. Silver Transformation Rules
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('control') AND name = 'silver_transformation_rules')
BEGIN
    CREATE TABLE [control].[silver_transformation_rules] (
        rule_id INT IDENTITY(1,1) PRIMARY KEY,
        silver_table_id INT NOT NULL,
        rule_sequence INT NOT NULL,
        rule_type VARCHAR(50) NOT NULL, -- 'CAST', 'TRIM', 'UPPER', 'LOWER', 'COALESCE', 'EXPRESSION'
        source_column VARCHAR(255) NOT NULL,
        target_column VARCHAR(255) NOT NULL,
        transformation_expression VARCHAR(MAX) NULL,
        is_active BIT NOT NULL DEFAULT 1,
        created_date DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT FK_SilverTransform_TableConfig FOREIGN KEY (silver_table_id) REFERENCES [control].[silver_table_config](silver_table_id)
    );
END
GO

-- 3. Silver Execution Log
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('control') AND name = 'silver_execution_log')
BEGIN
    CREATE TABLE [control].[silver_execution_log] (
        log_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        pipeline_run_id VARCHAR(100) NOT NULL,
        silver_table_id INT NOT NULL,
        execution_status VARCHAR(50) NOT NULL,
        records_read BIGINT DEFAULT 0,
        records_written BIGINT DEFAULT 0,
        records_filtered BIGINT DEFAULT 0,
        error_message NVARCHAR(MAX) NULL,
        start_time DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        end_time DATETIME2 NULL
    );
END
GO

-- 4. Silver Data Quality Rules
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('control') AND name = 'silver_dq_rules')
BEGIN
    CREATE TABLE [control].[silver_dq_rules] (
        rule_id VARCHAR(50) PRIMARY KEY,
        silver_table_id INT NOT NULL,
        column_name VARCHAR(255) NOT NULL,
        rule_type VARCHAR(50) NOT NULL,
        rule_expression VARCHAR(MAX) NOT NULL,
        severity VARCHAR(20) NOT NULL, -- 'ERROR', 'WARNING', 'SKIP_ROW'
        is_active BIT NOT NULL DEFAULT 1,
        CONSTRAINT FK_SilverDQ_TableConfig FOREIGN KEY (silver_table_id) REFERENCES [control].[silver_table_config](silver_table_id)
    );
END
GO