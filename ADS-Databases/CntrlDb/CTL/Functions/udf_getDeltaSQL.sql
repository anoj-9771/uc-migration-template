CREATE FUNCTION [CTL].[udf_GetDeltaSQL] (@TaskId BigInt)
Returns Varchar(MAX)
AS
/************************************************************
The function builds the initial SQL for Source Extraction
The function is called from the SP [CTL].[GetExecutionTasks] while building initial TaskDetails

************************************************************/

BEGIN
	Declare @SQL Varchar(Max) = 'SELECT * ',
			@TableName Varchar(255) = '',
			@WHERE Varchar(2000) = ' WHERE 1 = 1 '

	DECLARE @DataLoadMode varchar(100)
	DECLARE @SourceType varchar(100)
	DECLARE @IsAuditTable bit, @UseAuditTable bit
	DECLARE @TaskName varchar(100)
	DECLARE @AuditTable varchar(100)
	DECLARE @SQLCommand varchar(max)
	DECLARE @WaterMarkCol varchar(max), @WatermarkVal varchar(40)
	DECLARE @BusinessKey varchar(100)

	--This keyword will be inserted in the current delta sql and will be replaced with endvalue in ADF
	DECLARE @EndDateKey varchar(100) = '<<ENDDATE>>'

	--Get values from table
	SELECT 
		@DataLoadMode = CT.DataLoadMode 
		,@SourceType = T.ControlType
		,@TableName = CS.SourceLocation
		,@IsAuditTable = CS.IsAuditTable
		,@UseAuditTable = CS.UseAuditTable
		,@TaskName = CT.TaskName
		,@AuditTable = CS.SoftDeleteSource
		,@SQLCommand = CTC.Command
		,@WaterMarkCol = WM.SourceColumn
		,@WatermarkVal = WM.Watermarks
		,@BusinessKey = CS.BusinessKeyColumn
	FROM CTL.ControlTasks CT 
	LEFT JOIN CTL.ControlSource CS ON CT.SourceId = CS.SourceId
	LEFT JOIN CTL.ControlTypes T ON CS.SourceTypeId = T.TypeId
	LEFT JOIN CTL.ControlTaskCommand CTC ON CT.TaskId = CTC.ControlTaskId
	LEFT JOIN CTL.ControlWatermark WM ON CS.SourceId = WM.ControlSourceId
	WHERE CT.TaskId = @TaskId

	SET @SQLCommand = (SELECT Command FROM CTL.ControlTaskCommand WHERE ControlTaskId = @TaskId)

	--Beginning of filter condition build block

	/**********************************************************************/
	--Start of Special Audit Table Handling of OneEBS
	IF @UseAuditTable = 1 AND @AuditTable <> '' AND @TaskName LIKE 'OneEBS%'
	BEGIN
		DECLARE @OracleDateFormat varchar(50) = 'YYYY-MM-DD HH24:MI:SS'
		DECLARE @ALIAS_MAIN varchar(50) = 'mn'
		DECLARE @ALIAS_AUDIT varchar(50) = 'aud'
		DECLARE @ALIAS_UPDATED_TRANS_DATE varchar(50) = '"_ONEEBS_UPDATED_TRANSACTION_DATE"' --Please ensure that this column name is used as in on DataBricks
		DECLARE @JOIN_CLAUSE VARCHAR(500), @WATERMARK_COLS VARCHAR(500)

		--Get the table join clause with columns aliased
		SET @JOIN_CLAUSE = [CTL].[udf_GetMultiColTableJoinClause](@BusinessKey, @ALIAS_MAIN, @ALIAS_AUDIT)

		--Get the SQL Column list with columns aliased
		SET @WATERMARK_COLS = [CTL].[udf_GetMultiColSelectList](@WaterMarkCol, @ALIAS_MAIN)


		SET @SQL = 'SELECT '
		SET @SQL = @SQL + @ALIAS_MAIN + '.*'
		SET @SQL = @SQL + ', COALESCE(' + @ALIAS_AUDIT + '.AUDIT__TIMESTAMP, ' + @WATERMARK_COLS + ') ' + @ALIAS_UPDATED_TRANS_DATE
		SET @SQL = @SQL + ' FROM ' + @TableName + ' ' + @ALIAS_MAIN
		SET @SQL = @SQL + ' LEFT JOIN ('
		SET @SQL = @SQL + ' SELECT ' + @BusinessKey + ', MAX(AUDIT__TIMESTAMP) AS AUDIT__TIMESTAMP'
		SET @SQL = @SQL + ' FROM ' + @AuditTable
		SET @SQL = @SQL + ' WHERE 1 = 1 '
		SET @SQL = @SQL + ' AND AUDIT__TIMESTAMP >  TO_DATE(''' + @WatermarkVal + ''', ''YYYY-MM-DD HH24:MI:SS'') '
		SET @SQL = @SQL + ' AND AUDIT__TIMESTAMP <= TO_DATE(''' + @EndDateKey + ''', ''YYYY-MM-DD HH24:MI:SS'') '
		SET @SQL = @SQL + ' GROUP BY ' + @BusinessKey
		SET @SQL = @SQL + ' ) ' + @ALIAS_AUDIT + ' ON ' + @JOIN_CLAUSE
		SET @SQL = @SQL + ' WHERE 1 = 1 '
		SET @SQL = @SQL + ' AND COALESCE(' + @ALIAS_AUDIT + '.AUDIT__TIMESTAMP, ' + @WATERMARK_COLS + ') >  TO_DATE(''' + @WatermarkVal + ''', ''YYYY-MM-DD HH24:MI:SS'') '
		SET @SQL = @SQL + ' AND COALESCE(' + @ALIAS_AUDIT + '.AUDIT__TIMESTAMP, ' + @WATERMARK_COLS + ') <= TO_DATE(''' + @EndDateKey + ''', ''YYYY-MM-DD HH24:MI:SS'') '

		RETURN @SQL

	END
	--End of Special Audit Table Handling of OneEBS
	/**********************************************************************/

	--Start of normal incrmental block. This is needed to build the WHERE Clause for Source Extraction
	--The following section checks if the source type is CDC. Use a different block altogether for CDC
	IF @DataLoadMode = 'CDC'
	BEGIN
	
			SELECT @TableName = @TableName + '[CDC].[' + (Select SourceName 
				FROM CTL.ControlTasks CT
				INNER JOIN CTL.ControlSource CS ON CT.SourceId = CS.SourceId
				WHERE CT.TaskId = @TaskId) + '_ct]'

			SET @WHERE = @WHERE + ' UPPER([__$start_lsn]) > CONVERT(BINARY(10), ''' + @WatermarkVal + ''', 1) AND [__$operation] in (1, 2, 3, 4)'

	--End of CDC block
	END
	ELSE IF @DataLoadMode IN ('INCREMENTAL', 'APPEND')
	BEGIN
		--If it is Audit table and from OneEBS than update the source sql to include only deleted records
		IF @IsAuditTable = 1 AND @TaskName LIKE 'OneEBSAudit%'
		BEGIN
			SET @WHERE = @WHERE + ' AND AUDIT__OPERATION = ''D'' '
		END

		SET @WaterMarkCol = [CTL].[udf_GetMultiColFilterClause](@WaterMarkCol, @SourceType)
		IF @SourceType = 'Oracle'
			SET @WaterMarkCol = 'TO_CHAR(' + @WaterMarkCol + ', ''YYYY-MM-DD HH24:MI:SS'')'
		ELSE IF @SourceType = 'MySQL'
			SET @WaterMarkCol = 'FROM_UNIXTIME(' + @WaterMarkCol + ')'
		ELSE IF @SourceType = 'SQL Server' AND @TaskName LIKE 'isu%'
		BEGIN
		    SET @SQL = 'SELECT * '
 			/* added the following where clause to get dynamic partitioning working when reading from SQL server through ADF Copy activity;
				Commented the @WaterMarkCol(DELTA_TS) conversion to date to improve query performance*/
			--SET @WaterMarkCol = 'FORMAT(convert(datetime,(CONVERT(VARCHAR(25) , CAST(LEFT(' +@WaterMarkCol +', 8) AS DATETIME), 23) + '' '' +  LEFT(RIGHT(' + @WaterMarkCol + ' , 6) ,2) + '':'' + SUBSTRING(RIGHT(' +@WaterMarkCol + ' , 6) , 3,2) + '':''    + RIGHT(' + @WaterMarkCol + ' , 2) ),120), ''yyyy-MM-dd HH:mm:ss'')'
			SET @WHERE = ' WHERE ?AdfDynamicRangePartitionCondition'
			SET @WatermarkVal = REPLACE(REPLACE(REPLACE(@WatermarkVal, '-', ''), ':', ''), ' ', '')
		END
		ELSE
			SET @WaterMarkCol = 'FORMAT(' + @WaterMarkCol + ', ''yyyy-MM-dd HH:mm:ss'')'

		SET @WHERE = @WHERE + ' AND ' + @WaterMarkCol + ' > ' + '''' + @WatermarkVal + ''''  
		SET @WHERE = @WHERE + ' AND ' + @WaterMarkCol + ' <= ' + '''' + @EndDateKey + ''''
		
	END
	ELSE
	BEGIN
		--If it is Full-Extract or Truncate-Load then we do not need the WHERE Clause
		SELECT @WHERE = ''
	END
	--End of normal incrmental block


	/**************FINAL SQL****************************/
	--If the Configuration defines the SQL Extraction Column use that, else use SELECT * to pick all columns from Source Table
	IF UPPER(LEFT(TRIM(@SQLCommand), 8)) = 'SELECT *'
		SET @SQL = @SQL + 'FROM ' + @TableName
	ELSE
		SET @SQL = @SQLCommand

	--Build the final SQL with the WHERE filter clause
	SET @SQL = @SQL + @WHERE

	--Return the SQL
	RETURN @SQL 
END