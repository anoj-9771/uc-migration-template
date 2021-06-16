-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 02/09/2020
-- Description: Function used to generate sql query to run against a cdc enabled DB
-- use  'all update old' for update before and update after
-- =============================================

CREATE     function [CTL].[udf_getSqlCdcQuery](@from_lsn nvarchar(1000), @Query nvarchar(max))
returns varchar(max)
as 
begin
	declare @SQL nvarchar(max)
	declare @TableName_cdc varchar(250)
	declare @TableName varchar(250)
	declare @Columns varchar(max)
	declare @FindFrom VARCHAR(1) = '~'
	declare @Query_full varchar(max)

	select @Query_full = replace(replace('<' + @Query, ' from ', ' ~ '), '<select ', '')	
	SELECT @Columns = replace(LEFT(@Query_full, LEN(@Query_full) - CHARINDEX(@FindFrom,REVERSE(@Query_full))),' ','')
	-- Shows text after last slash
	SELECT @TableName = RIGHT(@Query_full, CHARINDEX(@FindFrom,REVERSE(@Query_full))-1)
	select @TableName_cdc = replace(replace(@TableName, '.', '_'), ' ','')

	if @from_lsn is null or @from_lsn = ''
	begin
		declare @lsn binary
		declare @extraction_date datetime

		set @SQL = 'SELECT ' + @Columns + ',
				convert(varchar(100),
					sys.fn_cdc_map_time_to_lsn(
						''smallest greater than or equal'', 
						format(getdate(),''yyyy-MM-dd 00:00:00.000'')
					)
				,1) as __$start_lsn,
				2 as __$operation,
				''I'' as Flag,
				getdate() as tran_begin_time,
				getdate() as tran_end_time,
				''0x000000000000'' as tran_id
			from ' + @TableName 
	end
	else 
	begin
		--set @from_lsn = 'sys.fn_cdc_get_min_lsn(''' + @TableName_cdc + ''')'
		set @SQL = 'SELECT ' + @Columns + '
				,convert(varchar(100),ct.__$start_lsn, 1) __$start_lsn
				,ct.__$operation
				,case ct.__$operation
					WHEN 1 THEN ''D''
					WHEN 2 THEN ''I''
					WHEN 3 THEN ''UB''
					WHEN 4 THEN ''U''
				end as Flag
				,lsn.tran_begin_time
				,lsn.tran_end_time
				,convert(varchar(100),lsn.tran_id, 1) tran_id
			from [cdc].[fn_cdc_get_all_changes_' + @TableName_cdc + ']

			(' + @from_lsn + ', <<to_value>>,  N''all'') AS ct join
			cdc.lsn_time_mapping AS LSN ON ct.__$start_lsn = lsn.start_lsn
			order by __$start_lsn
			'
	end
	return (
		select @SQL
	)
end


--begin
--		set @SQL = 'SELECT
--				' + @Columns + '
--				,'' '' + convert(varchar(100),ct.__$start_lsn, 1) __$start_lsn
--				,ct.__$operation
--				,CASE ct.__$operation
--					WHEN 1 THEN ''D''
--					WHEN 2 THEN ''I''
--					WHEN 3 THEN ''UB''
--					WHEN 4 THEN ''U''
--				END AS Flag
--				,lsn.tran_begin_time
--				,lsn.tran_end_time
--				,'' '' + convert(varchar(100),lsn.tran_id, 1) tran_id
--			from 

--			[cdc].[fn_cdc_get_all_changes_' + @tableName + ']

--			(' + @from_lsn + ', @to_lsn, N''all'') AS ct INNER JOIN

--			cdc.lsn_time_mapping AS LSN ON ct.__$start_lsn = lsn.start_lsn
--			order by __$start_lsn
--			'
--	end