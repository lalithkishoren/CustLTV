CREATE PROCEDURE [control].[sp_GetSilverTransformationRules]
    @SilverTableId INT
AS
BEGIN
    SET NOCOUNT ON;

    SELECT 
        rule_id,
        rule_name,
        rule_type,
        source_column,
        target_column,
        transformation_expression,
        execution_sequence
    FROM control.silver_transformation_rules
    WHERE silver_table_id = @SilverTableId
      AND is_active = 1
    ORDER BY execution_sequence ASC;
END
GO