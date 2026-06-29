CREATE PROCEDURE [control].[sp_UpdateSilverTableStatus]
    @SilverTableId INT,
    @Status VARCHAR(50),
    @PipelineRunId VARCHAR(100),
    @RecordsRead BIGINT = 0,
    @RecordsWritten BIGINT = 0,
    @RecordsFiltered BIGINT = 0,
    @RecordsQuarantined BIGINT = 0,
    @ErrorMessage NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- Update silver table config
    UPDATE control.silver_table_config
    SET last_load_status = @Status,
        last_load_timestamp = GETUTCDATE(),
        last_pipeline_run_id = @PipelineRunId,
        modified_date = GETUTCDATE()
    WHERE silver_table_id = @SilverTableId;

    -- Insert execution log
    INSERT INTO control.silver_execution_log (
        pipeline_run_id, 
        silver_table_id, 
        records_read, 
        records_written, 
        records_filtered,
        records_quarantined,
        execution_status, 
        error_message, 
        end_time
    )
    VALUES (
        @PipelineRunId, 
        @SilverTableId, 
        @RecordsRead, 
        @RecordsWritten, 
        @RecordsFiltered,
        @RecordsQuarantined,
        @Status, 
        @ErrorMessage, 
        GETUTCDATE()
    );
END
GO