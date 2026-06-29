CREATE OR ALTER PROCEDURE control.sp_GetCDCChanges
    @SchemaName VARCHAR(100),
    @TableName VARCHAR(100),
    @PrimaryKeyColumns VARCHAR(255),
    @LastSyncVersion VARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @JoinCondition NVARCHAR(MAX) = '';
    DECLARE @PKList NVARCHAR(MAX) = '';
    
    -- Parse comma-separated PKs for Join Condition and Select List
    SELECT 
        @JoinCondition = @JoinCondition + CASE WHEN @JoinCondition = '' THEN '' ELSE ' AND ' END + 
                         'T.[' + LTRIM(RTRIM(value)) + '] = CT.[' + LTRIM(RTRIM(value)) + ']',
        @PKList = @PKList + 'CT.[' + LTRIM(RTRIM(value)) + '], '
    FROM STRING_SPLIT(@PrimaryKeyColumns, ',');

    -- Build dynamic SQL for Change Tracking
    -- Note: We use CHANGE_TRACKING_CURRENT_VERSION() to stamp the extract
    SET @SQL = N'
        DECLARE @CurrentVersion BIGINT = CHANGE_TRACKING_CURRENT_VERSION();
        
        SELECT 
            CT.SYS_CHANGE_OPERATION,
            CT.SYS_CHANGE_VERSION,
            ' + @PKList + '
            @CurrentVersion AS _current_sync_version,
            T.*
        FROM CHANGETABLE(CHANGES [' + @SchemaName + '].[' + @TableName + '], ' + ISNULL(@LastSyncVersion, '0') + ') AS CT
        LEFT JOIN [' + @SchemaName + '].[' + @TableName + '] AS T 
            ON ' + @JoinCondition + ';
    ';

    EXEC sp_executesql @SQL;
END
GO