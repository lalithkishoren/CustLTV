CREATE PROCEDURE [control].[sp_GetGoldAggregationRules]
    @GoldTableId INT
AS
BEGIN
    SET NOCOUNT ON;

    SELECT 
        rule_id,
        gold_table_id,
        aggregation_name,
        aggregation_type,
        source_column,
        target_column,
        group_by_columns,
        filter_expression
    FROM control.gold_aggregation_rules
    WHERE gold_table_id = @GoldTableId
      AND is_active = 1;
END
GO