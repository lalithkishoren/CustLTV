-- ===============================================================================
-- Create Control Schema and Tables
-- ===============================================================================

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'control')
BEGIN
    EXEC('CREATE SCHEMA [control]');
END
GO

-- 1. source_systems
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('control') AND name = 'source_systems')
BEGIN
    CREATE TABLE control.source_systems (
        source_system_id INT IDENTITY(1,1) PRIMARY KEY,
        source_system_name VARCHAR(100) NOT NULL UNIQUE,
        source_system_type VARCHAR(50) NOT NULL,
        is_active BIT NOT NULL DEFAULT 1,
        created_date DATETIME2 NOT NULL DEFAULT GETUTCDATE()
    );
END
GO

-- 2. table_metadata
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('control') AND name = 'table_metadata')
BEGIN
    CREATE TABLE control.table_metadata (
        table_id INT IDENTITY(1,1) PRIMARY KEY,
        source_system_id INT NOT NULL FOREIGN KEY REFERENCES control.source_systems(source_system_id),
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
        load_priority INT NOT NULL DEFAULT 100,
        created_date DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        modified_date DATETIME2 NOT NULL DEFAULT GETUTCDATE()
    );
END
GO

-- 3. load_dependencies
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('control') AND name = 'load_dependencies')
BEGIN
    CREATE TABLE control.load_dependencies (
        dependency_id INT IDENTITY(1,1) PRIMARY KEY,
        table_id INT NOT NULL FOREIGN KEY REFERENCES control.table_metadata(table_id),
        depends_on_table_id INT NOT NULL FOREIGN KEY REFERENCES control.table_metadata(table_id),
        dependency_type VARCHAR(50) NOT NULL DEFAULT 'FK'
    );
END
GO

-- 4. pipeline_execution_log
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('control') AND name = 'pipeline_execution_log')
BEGIN
    CREATE TABLE control.pipeline_execution_log (
        log_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        pipeline_run_id UNIQUEIDENTIFIER NOT NULL,
        table_id INT NULL FOREIGN KEY REFERENCES control.table_metadata(table_id),
        execution_start_time DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        execution_end_time DATETIME2 NULL,
        status VARCHAR(50) NOT NULL,
        rows_read BIGINT NULL,
        rows_written BIGINT NULL,
        error_message VARCHAR(MAX) NULL
    );
END
GO

-- 5. data_quality_rules
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('control') AND name = 'data_quality_rules')
BEGIN
    CREATE TABLE control.data_quality_rules (
        rule_id INT IDENTITY(1,1) PRIMARY KEY,
        table_id INT NOT NULL FOREIGN KEY REFERENCES control.table_metadata(table_id),
        column_name VARCHAR(100) NOT NULL,
        rule_type VARCHAR(50) NOT NULL,
        rule_definition VARCHAR(MAX) NOT NULL,
        action_on_failure VARCHAR(50) NOT NULL,
        is_active BIT NOT NULL DEFAULT 1
    );
END
GO