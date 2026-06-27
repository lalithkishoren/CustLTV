CREATE OR ALTER PROCEDURE control.sp_UpdateTableMetadata
    @TableId INT,
    @Status VARCHAR(50),
    @PipelineRunId UNIQUEIDENTIFIER,
    @RecordsLoaded BIGINT,
    @SyncVersion VARCHAR(255),
    @MarkInitialLoadComplete BIT,
    @ErrorMessage VARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        -- 1. Update table_metadata
        UPDATE control.table_metadata
        SET 
            last_load_status = @Status,
            last_load_timestamp = GETUTCDATE(),
            last_pipeline_run_id = @PipelineRunId,
            records_loaded = @RecordsLoaded,
            last_sync_version = CASE WHEN @Status = 'SUCCESS' THEN @SyncVersion ELSE last_sync_version END,
            initial_load_completed = CASE WHEN @MarkInitialLoadComplete = 1 AND @Status = 'SUCCESS' THEN 1 ELSE initial_load_completed END,
            modified_date = GETUTCDATE()
        WHERE table_id = @TableId;

        -- 2. Insert into pipeline_execution_log
        INSERT INTO control.pipeline_execution_log (
            pipeline_run_id, 
            table_id, 
            execution_status, 
            records_processed, 
            end_time, 
            error_message
        )
        VALUES (
            @PipelineRunId,
            @TableId,
            @Status,
            @RecordsLoaded,
            GETUTCDATE(),
            @ErrorMessage
        );

        COMMIT TRANSACTION;

        -- Return updated record for verification
        SELECT * FROM control.table_metadata WHERE table_id = @TableId;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        DECLARE @ErrorMsg NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();

        RAISERROR(@ErrorMsg, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO