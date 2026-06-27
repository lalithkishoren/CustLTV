CREATE OR ALTER PROCEDURE control.sp_GetCDCChanges
    @SchemaName NVARCHAR(100),
    @TableName NVARCHAR(100),
    @PrimaryKeyColumns NVARCHAR(255),
    @LastSyncVersion BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @DynamicSQL NVARCHAR(MAX);
    DECLARE @JoinConditions NVARCHAR(MAX) = '';
    DECLARE @PKSelect NVARCHAR(MAX) = '';
    DECLARE @CurrentVersion BIGINT = CHANGE_TRACKING_CURRENT_VERSION();

    -- Parse comma-separated primary keys for JOIN and SELECT
    DECLARE @PKTable TABLE (PK_Col NVARCHAR(100));
    INSERT INTO @PKTable (PK_Col)
    SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@PrimaryKeyColumns, ',');

    -- Build Join Conditions: T.PK1 = CT.PK1 AND T.PK2 = CT.PK2
    SELECT @JoinConditions = @JoinConditions + 
        CASE WHEN @JoinConditions = '' THEN '' ELSE ' AND ' END + 
        'T.[' + PK_Col + '] = CT.[' + PK_Col + ']'
    FROM @PKTable;

    -- Build PK Select: CT.PK1, CT.PK2
    SELECT @PKSelect = @PKSelect + 
        CASE WHEN @PKSelect = '' THEN '' ELSE ', ' END + 
        'CT.[' + PK_Col + ']'
    FROM @PKTable;

    -- Construct the dynamic SQL
    SET @DynamicSQL = N'
    SELECT 
        CT.SYS_CHANGE_OPERATION,
        CT.SYS_CHANGE_VERSION,
        ' + @PKSelect + N',
        ' + CAST(@CurrentVersion AS NVARCHAR(50)) + N' AS _current_sync_version,
        T.*
    FROM CHANGETABLE(CHANGES [' + @SchemaName + N'].[' + @TableName + N'], ' + CAST(@LastSyncVersion AS NVARCHAR(50)) + N') AS CT
    LEFT JOIN [' + @SchemaName + N'].[' + @TableName + N'] AS T 
        ON ' + @JoinConditions + N';
    ';

    -- Execute the dynamic SQL
    EXEC sp_executesql @DynamicSQL;
END;
GO