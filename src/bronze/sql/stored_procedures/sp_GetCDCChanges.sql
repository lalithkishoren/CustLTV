CREATE OR ALTER PROCEDURE control.sp_GetCDCChanges
    @SchemaName NVARCHAR(100),
    @TableName NVARCHAR(100),
    @PrimaryKeyColumns NVARCHAR(255),
    @LastSyncVersion BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @CurrentVersion BIGINT = CHANGE_TRACKING_CURRENT_VERSION();
    DECLARE @DynamicSQL NVARCHAR(MAX);
    DECLARE @JoinCondition NVARCHAR(MAX) = '';
    DECLARE @SelectPKs NVARCHAR(MAX) = '';
    
    -- Parse comma-separated PKs for Join Condition and Select list
    DECLARE @PKTable TABLE (PKName NVARCHAR(100));
    INSERT INTO @PKTable (PKName)
    SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@PrimaryKeyColumns, ',');

    SELECT @JoinCondition = @JoinCondition + 'CT.[' + PKName + '] = T.[' + PKName + '] AND '
    FROM @PKTable;
    
    SELECT @SelectPKs = @SelectPKs + 'CT.[' + PKName + '], '
    FROM @PKTable;

    -- Remove trailing ' AND ' and ', '
    SET @JoinCondition = LEFT(@JoinCondition, LEN(@JoinCondition) - 4);
    
    -- Build the dynamic SQL
    SET @DynamicSQL = N'
    SELECT 
        CT.SYS_CHANGE_OPERATION,
        ' + @SelectPKs + '
        CAST(' + CAST(@CurrentVersion AS NVARCHAR(50)) + ' AS BIGINT) AS _current_sync_version,
        T.*
    FROM CHANGETABLE(CHANGES [' + @SchemaName + '].[' + @TableName + '], ' + CAST(@LastSyncVersion AS NVARCHAR(50)) + ') AS CT
    LEFT JOIN [' + @SchemaName + '].[' + @TableName + '] AS T 
        ON ' + @JoinCondition + ';';

    EXEC sp_executesql @DynamicSQL;
END
GO