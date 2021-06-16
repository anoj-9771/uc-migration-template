-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 02/09/2020
-- Description: Function used to generate sql query to run against a delta loads
-- =============================================

create        function [CTL].[udf_getSqlDeltaQuery](@DatasetId bigint, @Query nvarchar(max))
returns varchar(max)
as 
begin
	declare @delta nvarchar(max)
	declare @SQL nvarchar(max)
	declare @ds_type nvarchar(250)
	if @DatasetId in (select distinct DatasetId from ctl.Watermark)
	begin
		set @Query = replace(replace(replace(@Query, CHAR(13), ' '), CHAR(10), ' '),'  ',' ') 
		select @ds_type = tp.Name from ctl.DataSet ds join ctl.Type tp on ds.TypeId = tp.TypeId  where ds.DataSetId = @DatasetId
		select @delta = string_agg(
			concat([Column] , ' ' , Operator , ' ' , (
				case when isnumeric([Value]) = 1 then [Value] 
				else [CTL].[udf_getWatermarkValue]([WatermarkId],[Column],[Value],@ds_type,[DataTypeId]) end))
				,' AND ' )
			from ctl.Watermark wk
			join ctl.Type tp on wk.TypeId = tp.TypeId
			where DatasetId = @DatasetId 
		set @SQL = case when @Query like '% WHERE %' then replace(@Query,';','') + ' AND ' + @delta + ';' else  replace(@Query,';','') + ' WHERE ' + @delta + ';' end
	end
	else 
	begin
		set @SQL = null
	end
	return (
		select @SQL
	)
end