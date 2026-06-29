CREATE OR ALTER PROCEDURE [control].[sp_GetSilverTransformationRules]
    @SilverTableId INT
AS
BEGIN
    SET NOCOUNT ON;

    SELECT 
        rule_id,
        rule_sequence,
        rule_type,
        source_column,
        target_column,
        transformation_expression
    FROM [control].[silver_transformation_rules]
    WHERE silver_table_id = @SilverTableId
      AND is_active = 1
    ORDER BY rule_sequence ASC;
END
GO