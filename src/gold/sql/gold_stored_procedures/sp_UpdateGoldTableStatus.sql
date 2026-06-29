CREATE PROCEDURE [control].[sp_UpdateGoldTableStatus]
    @GoldTableId INT,
    @Status VARCHAR(50),
    @PipelineRunId VARCHAR(100),
    @RecordsProcessed BIGINT = 0,
    @ErrorMessage NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- Update gold table config
    UPDATE control.gold_table_config
    SET last_refresh_status = @Status,
        last_refresh_timestamp = GETUTCDATE(),
        modified_date = GETUTCDATE()
    WHERE gold_table_id = @GoldTableId;

    -- Insert execution log
    INSERT INTO control.gold_execution_log (
        pipeline_run_id, 
        gold_table_id, 
        records_processed,
        execution_status, 
        start_time, 
        end_time, 
        error_message
    )
    VALUES (
        @PipelineRunId, 
        @GoldTableId, 
        @RecordsProcessed,
        @Status, 
        GETUTCDATE(), 
        GETUTCDATE(), 
        @ErrorMessage
    );

    -- Return updated record
    SELECT * FROM control.gold_table_config WHERE gold_table_id = @GoldTableId;
END
GO