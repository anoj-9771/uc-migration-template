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
DECLARE @DBType varchar(100)
DECLARE @SourceTable varchar(100)
DECLARE @SourceColumn varchar(100)
DECLARE @ValidationColumn varchar(100)
DECLARE @SourceName varchar(100)
DECLARE @NULLFunction varchar(100), @DATEFunction varchar(100), @DATEFormat varchar(100)
DECLARE @TaskName varchar(255)


SELECT 
	@DataLoadMode = CT.DataLoadMode 
	,@DBType = T.ControlType
	,@SourceTable = CS.SourceLocation
	,@SourceColumn = W.SourceColumn
	,@ValidationColumn = CS.ValidationColumn
	,@SourceName = CS.SourceName
	,@TaskName = CT.TaskName
FROM CTL.ControlTasks CT 
LEFT JOIN CTL.ControlSource CS ON CT.SourceId = CS.SourceId
LEFT JOIN CTL.ControlTypes T ON CS.SourceTypeId = T.TypeId
LEFT JOIN CTL.ControlWatermark W ON W.ControlSourceId = CS.SourceId
WHERE CS.SourceId = @SourceId

IF @ValidationType = 'TARGET' SET @DBType = 'SQL Server'

IF @DataLoadMode = 'CDC'
	BEGIN
		SELECT @SQL = 'SELECT ''UPPER([__$start_lsn])'' AS SourceColumn, upper(sys.fn_varbintohexstr(sys.fn_cdc_get_max_lsn())) AS ReturnValue, sys.fn_cdc_map_lsn_to_time(sys.fn_cdc_get_max_lsn()) AS ReturnTime'
	END
ELSE
	BEGIN
		SET @TableName = @TableName + @SourceTable
		DECLARE @COL varchar(100)
		DECLARE @SQLMax varchar(1000)

		/*Define the NULL function based on the Database Type*/
		IF @DBType = 'Oracle' OR @DBType = 'MySQL' OR @DBType = 'SQL Server'
			SET @NULLFunction = 'COALESCE'
		ELSE
			SET @NULLFunction = 'COALESCE'

		/*Get the SQL Max value if the Source Column is defined*/
		IF @SourceColumn IS NOT NULL
		BEGIN
			SET @COL = [CTL].[udf_GetMultiColFilterClause](@SourceColumn, @DBType)

			IF @DBType = 'Oracle'
				SELECT @SQLMax = 'TO_CHAR(MAX(' + @COL + '), ''YYYY-MM-DD HH24:MI:SS'')'
			ELSE IF @DBType = 'MySQL'
				SELECT @SQLMax = 'FROM_UNIXTIME(MAX(COALESCE(' + @COL + ', 0)))'
			--ELSE IF @DBType = 'SQL Server'
			--	SELECT @SQLMax = 'FORMAT(MAX(' + @COL + '), ''yyyy-MM-dd HH:mm:ss'')'
			ELSE IF @DBType = 'SQL Server' AND @TaskName LIKE 'sapisu%'
				SELECT @SQLMax = 'FORMAT(MAX(convert(datetime,(CONVERT(VARCHAR(25) , CAST(LEFT(' +@COL +', 8) AS DATETIME), 23) + '' '' +  LEFT(RIGHT(' + @COL + ' , 6) ,2) + '':'' + SUBSTRING(RIGHT(' +@COL + ' , 6) , 3,2) + '':''    + RIGHT(' + @COL + ' , 2) ),120)), ''yyyy-MM-dd HH:mm:ss'')'
			ELSE
				SELECT @SQLMax = 'FORMAT(MAX(' + @COL + '), ''yyyy-MM-dd HH:mm:ss'')'
		END
		ELSE
		BEGIN
			/*Set default value if the Source Column is NOT defined*/
			SET @SourceColumn = @SourceName
			SET @SQLMax = '0'
		END

		SET @SQL = @SQL + '''' + @SourceColumn + ''' AS SourceColumn, '
		SET @SQL = @SQL + @SQLMax + ' As ReturnValue, '
		SET @SQL = @SQL + 'COUNT(1) AS RecordCount'

		IF @ValidationColumn <> ''
		BEGIN
			SET @SQL = @SQL + ', SUM(' + @NULLFunction + '(' + @ValidationColumn + ',0)) AS TotalValue'
			SET @SQL = @SQL + ', MAX(' + @NULLFunction + '(' + @ValidationColumn + ',0)) AS MaximumValue'
			SET @SQL = @SQL + ', MIN(' + @NULLFunction + '(' + @ValidationColumn + ',0)) AS MinimumValue'
		END
		ELSE
		BEGIN
			SET @SQL = @SQL + ', 0 AS TotalValue'
			SET @SQL = @SQL + ', 0 AS MaximumValue'
			SET @SQL = @SQL + ', 0 AS MinimumValue'
		END

		SET @SQL = @SQL + ', ''' + @ValidationType + ''' AS ValidationType'

		IF UPPER(@ValidationType) = 'SOURCE'
			SET @SQL = @SQL + @TableName
		ELSE
		BEGIN
			/*Check if table exists. Sometimes table may not exist on EDW because there may be no records on Source */
			DECLARE @SQLExists varchar(1000), @SQLTargetDefault varchar(1000)
			SET @SQLExists = 'IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ''edw'' AND TABLE_NAME = ''' + @SourceName + ''') ' 
			SET @SQLTargetDefault = 'ELSE SELECT '''' AS SourceColumn, 0 As ReturnValue, 0 AS RecordCount, 0 AS TotalValue, 0 AS MaximumValue, 0 AS MinimumValue, '''' AS ValidationType'

			/*Somehow the ADF Lookup errors when there is no record returned. Thus, the following block will return default values when the table does not exists
			However, as the Target is UPDATE command it will not update any rows */
			SET @SQL = @SQLExists + @SQL + ' FROM ' + 'edw.' + @SourceName + ' WHERE _RecordCurrent = 1 AND _RecordDeleted = 0 ' + @SQLTargetDefault
		END


	END

SELECT @SQL SQLStatement

END