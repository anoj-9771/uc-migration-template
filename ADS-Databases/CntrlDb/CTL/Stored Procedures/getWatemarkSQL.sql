CREATE Procedure [CTL].[GetWatemarkSQL] @SourceId BigInt
As

BEGIN
Declare @SQL Varchar(2000) = 'SELECT ',
		@TableName Varchar(255) = ' FROM '


DECLARE @DataLoadMode varchar(100)
DECLARE @SourceType varchar(100)
SELECT 
	@DataLoadMode = CT.DataLoadMode 
	,@SourceType = T.ControlType
FROM CTL.ControlTasks CT 
LEFT JOIN CTL.ControlSource CS ON CT.SourceId = CS.SourceId
LEFT JOIN CTL.ControlTypes T ON CS.SourceTypeId = T.TypeId
WHERE CS.SourceId = @SourceId

IF @DataLoadMode = 'CDC'
	BEGIN
		SELECT @SQL = 'SELECT ''UPPER([__$start_lsn])'' AS SourceColumn, upper(sys.fn_varbintohexstr(sys.fn_cdc_get_max_lsn())) AS ReturnValue, sys.fn_cdc_map_lsn_to_time(sys.fn_cdc_get_max_lsn()) AS ReturnTime'
	END
ELSE
	BEGIN
		Select @TableName = @TableName + (Select SourceLocation From CTL.ControlSource Where SourceId = @SourceId)
	
		Declare @Columns Table
		(
		  ColumnName Varchar(255),
		  SourceSQL Varchar(2000)
		)

		Insert Into @Columns
			Select m.SourceColumn, m.SourceSQL
				From CTL.ControlWatermark m
				Where ControlSourceId = @SourceId

		While (Select Count(*) From @Columns) > 0
		  BEGIN

			--Check if the Watermark column contains any comma, meaning there may be more than one column in it
			--If so, use the COALESCE, NVL style functions to get values
			DECLARE @COL varchar(100)
			SET @COL = (Select Top 1 ColumnName From @Columns)
			SET @COL = [CTL].[udf_GetMultiColFilterClause](@COL, @SourceType)

			IF @SourceType = 'Oracle'
				Select @SQL = @SQL + ' ''' + @COL + ''' As SourceColumn,  TO_CHAR(MAX(' + @COL + '), ''YYYY-MM-DD HH24:MI:SS'') As ReturnValue, '
			ELSE IF @SourceType = 'MySQL'
				Select @SQL = @SQL + ' ''' + @COL + ''' As SourceColumn,  FROM_UNIXTIME(MAX(IFNULL(' + @COL + ', 0))) As ReturnValue, '
			ELSE
				Select @SQL = @SQL + ' ''' + @COL + ''' As SourceColumn,  FORMAT(MAX(' + @COL + '), ''yyyy-MM-dd HH:mm:ss'') As ReturnValue, '


			Delete Top(1) From @Columns
		  END

		SELECT @SQL = @SQL + ' COUNT(1) AS RecCount '

		Select @SQL = @SQL + @TableName

	END


Select @SQL SQLStatement

END