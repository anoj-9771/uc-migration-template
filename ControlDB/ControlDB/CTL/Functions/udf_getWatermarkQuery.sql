-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 02/09/2020
-- Description: Function used to generate sql query to run against a delta loads
-- =============================================

CREATE        function [CTL].[udf_getWatermarkQuery](@DatasetId bigint, @Query varchar(max))
returns varchar(max)
as 
begin
	declare @max varchar(max)
	declare @SQL varchar(max)
	declare @wm_typeId bigint
	declare @ds_type varchar(50) 
	declare @where varchar(max)
	declare @is_lsn bit
	declare @TableName_cdc varchar(250)
	declare @TableName varchar(250)
	declare @FindFrom VARCHAR(1) = '~'

	set @Query = replace(replace(replace(@Query, CHAR(13), ' '), CHAR(10), ' '),'  ',' ')
	set @Query = replace(replace(@Query,';',''),' from ','~')
	set @tableName = RIGHT(@Query, CHARINDEX(@FindFrom,REVERSE(@Query))-1)

	select @is_lsn = case when tp.Name = 'cdc' 
						then 1 
						else 0 end 
					from ctl.Watermark wm 
					join ctl.Type tp on wm.TypeId = tp.TypeId 

	select @wm_typeId = TypeId from ctl.Type where Name = 'datetime'
	select @ds_type = tp.name from ctl.Dataset ds join ctl.Type tp on ds.TypeId = tp.TypeId where DataSetId = @DatasetId
	begin
		if @DatasetId in (select distinct DatasetId from ctl.Watermark)
		begin
			select @SQL = 
				case when @ds_type = 'oracle'  and DataTypeId = @wm_typeId 
					then 'SELECT ' + string_agg(concat('TO_CHAR(max(', [Column], '),''YYYY-MM-DD HH24:MI:SS'') as ', [Column]),',' ) + ',null as to_value FROM ' +  @tableName
				when @is_lsn = 1  
					then 'SELECT convert(varchar(100),sys.fn_cdc_get_max_lsn(),1) AS ' + [Column] + ', convert(varchar(100),sys.fn_cdc_get_max_lsn(),1) to_value'
				else 'SELECT ' + string_agg(concat('max(', [Column], ') as ', [Column]),',' )  + ',null as to_value FROM ' +  @tableName end 
				from ctl.Watermark wk
				where DatasetId = @DatasetId 
				group by DataTypeId, [Column]				
		end
		else 
		begin
			set @SQL = null
		end
	end
	return (
		select @SQL
	)
end