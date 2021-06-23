CREATE FUNCTION [CTL].[udf_GetMultiColFilterClause] (
	@Column varchar(255),
	@SourceType varchar(100)
)
Returns Varchar(MAX)
AS
/**************************************************************
This function returns the filter condition for null handling of more than one column
**************************************************************/
BEGIN

DECLARE @COL VARCHAR(255)

IF CHARINDEX (',', @Column) > 0
BEGIN
	IF @SourceType = 'SQL Server'
		SET @COL = 'COALESCE(' + @Column + ')'
	ELSE IF @SourceType = 'Oracle'
		SET @COL = 'COALESCE(' + @Column + ')'
	ELSE
		SET @COL = 'COALESCE(' + @Column + ')'
END
ELSE
	SET @COL = @Column

RETURN @COL
END