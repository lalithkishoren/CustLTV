-- ============================================================================
-- Create Control Schema and Tables
-- ============================================================================

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'control')
BEGIN
    EXEC('CREATE SCHEMA [control]');
END
GO

-- 1. Source Systems Registry
CREATE TABLE control.source_systems (
    source_system_id INT IDENTITY(1,1) PRIMARY KEY,
    source_system_name VARCHAR(100) NOT NULL UNIQUE,
    source_system_type VARCHAR(50) NOT NULL,
    is_active BIT NOT NULL DEFAULT 1,
    created_date DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
GO

-- 2. Table Metadata (CRITICAL TABLE)
CREATE TABLE control.table_metadata (
    table_id INT IDENTITY(1,1) PRIMARY KEY,
    source_system_id INT NOT NULL,
    schema_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    primary_key_columns VARCHAR(255) NOT NULL,
    load_type VARCHAR(20) NOT NULL,
    is_active BIT NOT NULL DEFAULT 1,
    initial_load_completed BIT NOT NULL DEFAULT 0,
    last_sync_version VARCHAR(255) NULL,
    last_load_status VARCHAR(50) NULL,
    last_load_timestamp DATETIME2 NULL,
    last_pipeline_run_id UNIQUEIDENTIFIER NULL,
    records_loaded BIGINT NULL DEFAULT 0,
    bronze_path VARCHAR(500) NOT NULL,
    silver_path VARCHAR(500) NULL,
    load_priority INT NOT NULL DEFAULT 100,
    created_date DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    modified_date DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT FK_table_metadata_source_systems FOREIGN KEY (source_system_id) 
        REFERENCES control.source_systems(source_system_id)
);
GO

-- 3. Load Dependencies
CREATE TABLE control.load_dependencies (
    dependency_id INT IDENTITY(1,1) PRIMARY KEY,
    table_id INT NOT NULL,
    depends_on_table_id INT NOT NULL,
    dependency_type VARCHAR(50) NOT NULL DEFAULT 'HARD',
    CONSTRAINT FK_load_dependencies_table FOREIGN KEY (table_id) 
        REFERENCES control.table_metadata(table_id),
    CONSTRAINT FK_load_dependencies_depends_on FOREIGN KEY (depends_on_table_id) 
        REFERENCES control.table_metadata(table_id)
);
GO

-- 4. Pipeline Execution Log
CREATE TABLE control.pipeline_execution_log (
    log_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    pipeline_run_id UNIQUEIDENTIFIER NOT NULL,
    table_id INT NULL,
    execution_status VARCHAR(50) NOT NULL,
    records_processed BIGINT NULL,
    start_time DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    end_time DATETIME2 NULL,
    error_message VARCHAR(MAX) NULL,
    CONSTRAINT FK_pipeline_execution_log_table FOREIGN KEY (table_id) 
        REFERENCES control.table_metadata(table_id)
);
GO

-- 5. Data Quality Rules
CREATE TABLE control.data_quality_rules (
    rule_id INT IDENTITY(1,1) PRIMARY KEY,
    table_id INT NOT NULL,
    rule_name VARCHAR(100) NOT NULL,
    rule_type VARCHAR(50) NOT NULL,
    rule_expression VARCHAR(MAX) NOT NULL,
    action_on_failure VARCHAR(50) NOT NULL DEFAULT 'WARN',
    is_active BIT NOT NULL DEFAULT 1,
    CONSTRAINT FK_data_quality_rules_table FOREIGN KEY (table_id) 
        REFERENCES control.table_metadata(table_id)
);
GO

-- 6. Medallion Execution Log
CREATE TABLE control.medallion_execution_log (
    execution_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    pipeline_run_id UNIQUEIDENTIFIER NOT NULL,
    layer VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    start_time DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    end_time DATETIME2 NULL
);
GO