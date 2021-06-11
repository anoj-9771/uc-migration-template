CREATE Procedure [CTL].[GetTableDetails] (
	@SourceId BigInt
	,@ValidationType varchar(10) = 'SOURCE'
)
As

BEGIN

--DECLARE @SourceId BIGINT = 1008
--DECLARE @ValidationType varchar(10) = 'TARGET'

DECLARE @SQL Varchar(2000) = 'SELECT ',
		@TableName Varchar(255) = ' FROM '


DECLARE @DataLoadMode varchar(100)
DECLARE @SourceType varchar(100)
DECLARE @SourceTable varchar(100)
DECLARE @SourceColumn varchar(100)
DECLARE @ValidationColumn varchar(100)
DECLARE @SourceName varchar(100)
DECLARE @NULLFunction varchar(100), @DATEFunction varchar(100), @DATEFormat varchar(100)


SELECT 
	@DataLoadMode = CT.DataLoadMode 
	,@SourceType = T.ControlType
	,@SourceTable = CS.SourceLocation
	,@SourceColumn = W.SourceColumn
	,@ValidationColumn = CS.ValidationColumn
	,@SourceName = CS.SourceName
FROM CTL.ControlTasks CT 
LEFT JOIN CTL.ControlSource CS ON CT.SourceId = CS.SourceId
LEFT JOIN CTL.ControlTypes T ON CS.SourceTypeId = T.TypeId
LEFT JOIN CTL.ControlWatermark W ON W.ControlSourceId = CS.SourceId
WHERE CS.SourceId = @SourceId

SET @NULLFunction = 'ISNULL'
SET @DATEFunction = 'FORMAT'
SET @DATEFormat = '''yyyy-MM-dd HH:mm:ss'''


IF @ValidationType = 'SOURCE'
BEGIN
	IF @SourceType = 'Oracle'
	BEGIN
		SET @NULLFunction = 'NVL'
		SET @DATEFunction = 'TO_CHAR'
		SET @DATEFormat = '''YYYY-MM-DD HH24:MI:SS'''
	END
END




IF @DataLoadMode = 'CDC'
	BEGIN
		SELECT @SQL = 'SELECT ''UPPER([__$start_lsn])'' AS SourceColumn, upper(sys.fn_varbintohexstr(sys.fn_cdc_get_max_lsn())) AS ReturnValue, sys.fn_cdc_map_lsn_to_time(sys.fn_cdc_get_max_lsn()) AS ReturnTime'
	END
ELSE
	BEGIN
		SET @TableName = @TableName + @SourceTable
		DECLARE @COL varchar(100)
		SET @COL = [CTL].[udf_GetMultiColFilterClause](@SourceColumn, @SourceColumn)
		SET @COL = [CTL].[udf_GetMultiColFilterClause](@COL, @SourceType)

		SET @SQL = @SQL + '''' + @COL + ''' As SourceColumn'
		SET @SQL = @SQL + ', ' + @DATEFunction + '(MAX(' + @COL + '), ' + @DATEFormat + ') As ReturnValue'

		SET @SQL = @SQL + ', COUNT(1) AS RecordCount'

		IF @ValidationColumn <> ''
		BEGIN
			SET @SQL = @SQL + ', SUM(' + @NULLFunction + '(' + @ValidationColumn + ',0)) AS TotalValue'
			SET @SQL = @SQL + ', MAX(' + @NULLFunction + '(' + @ValidationColumn + ',0)) AS MaxValue'
			SET @SQL = @SQL + ', MIN(' + @NULLFunction + '(' + @ValidationColumn + ',0)) AS MinValue'
		END
		ELSE
		BEGIN
			SET @SQL = @SQL + ', 0 AS TotalValue'
			SET @SQL = @SQL + ', 0 AS MaxValue'
			SET @SQL = @SQL + ', 0 AS MinValue'
		END

		SET @SQL = @SQL + ', ''' + @ValidationType + ''' AS ValidationType'

		IF UPPER(@ValidationType) = 'SOURCE'
			SET @SQL = @SQL + @TableName
		ELSE
		BEGIN
			SET @SQL = @SQL + ' FROM ' + 'edw.' + @SourceName + ' WHERE _RecordCurrent = 1 AND _RecordDeleted = 0'
		END


	END

SELECT @SQL SQLStatement

END