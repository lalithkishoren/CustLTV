CREATE OR ALTER PROCEDURE [control].[sp_UpdateSilverTableStatus]
    @SilverTableId INT,
    @Status VARCHAR(50),
    @PipelineRunId VARCHAR(100),
    @RecordsRead BIGINT = 0,
    @RecordsWritten BIGINT = 0,
    @RecordsFiltered BIGINT = 0,
    @ErrorMessage NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- Update silver table config
    UPDATE [control].[silver_table_config]
    SET last_load_status = @Status,
        last_load_timestamp = GETUTCDATE(),
        last_pipeline_run_id = @PipelineRunId,
        records_read = @RecordsRead,
        records_written = @RecordsWritten,
        records_filtered = @RecordsFiltered,
        modified_date = GETUTCDATE()
    WHERE silver_table_id = @SilverTableId;

    -- Insert execution log
    INSERT INTO [control].[silver_execution_log] 
    (pipeline_run_id, silver_table_id, execution_status, records_read, records_written, records_filtered, error_message, end_time)
    VALUES 
    (@PipelineRunId, @SilverTableId, @Status, @RecordsRead, @RecordsWritten, @RecordsFiltered, @ErrorMessage, GETUTCDATE());
END
GO