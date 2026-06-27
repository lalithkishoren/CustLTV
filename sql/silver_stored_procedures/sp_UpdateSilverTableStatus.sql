CREATE PROCEDURE [control].[sp_UpdateSilverTableStatus]
    @SilverTableId INT,
    @Status VARCHAR(50),
    @PipelineRunId VARCHAR(100),
    @RecordsRead BIGINT = 0,
    @RecordsWritten BIGINT = 0,
    @RecordsFiltered BIGINT = 0,
    @RecordsQuarantined BIGINT = 0,
    @StartTime DATETIME2,
    @EndTime DATETIME2,
    @ErrorMessage NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Update silver table config
        UPDATE control.silver_table_config
        SET last_load_status = @Status,
            last_load_timestamp = GETUTCDATE(),
            last_pipeline_run_id = @PipelineRunId,
            modified_date = GETUTCDATE(),
            silver_initial_load_completed = CASE WHEN @Status = 'SUCCESS' THEN 1 ELSE silver_initial_load_completed END
        WHERE silver_table_id = @SilverTableId;

        -- Insert execution log
        INSERT INTO control.silver_execution_log (
            pipeline_run_id, 
            silver_table_id, 
            records_read, 
            records_written, 
            records_filtered, 
            records_quarantined,
            start_time, 
            end_time, 
            execution_status, 
            error_message
        )
        VALUES (
            @PipelineRunId,
            @SilverTableId,
            @RecordsRead,
            @RecordsWritten,
            @RecordsFiltered,
            @RecordsQuarantined,
            @StartTime,
            @EndTime,
            @Status,
            @ErrorMessage
        );

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
            
        DECLARE @ErrorMsg NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        RAISERROR(@ErrorMsg, @ErrorSeverity, @ErrorState);
    END CATCH
END
GO