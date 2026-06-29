CREATE OR ALTER PROCEDURE [control].[sp_GetGoldDimensionConfig]
    @GoldTableId INT
AS
BEGIN
    SET NOCOUNT ON;

    SELECT 
        dimension_id,
        gold_table_id,
        scd_type,
        business_key_columns,
        tracked_columns,
        surrogate_key_column,
        unknown_member_key
    FROM control.gold_dimension_config
    WHERE gold_table_id = @GoldTableId;
END
GO