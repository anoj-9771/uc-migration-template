
create         function [CTL].[udf_getWatermarkValue](@WatermarkId bigint, @wm_column varchar(150), @wm_value varchar(50), @ds_type varchar(50), @wm_typeId varchar(50))
returns varchar(250)
as
begin
	declare @wm_type varchar(50) = (select Name from ctl.Type where TypeId = @wm_typeId)
	declare @wm_output varchar(100)
	begin
		select @wm_output = case when @wm_type = 'datetime' and @ds_type = 'oracle' then 'to_date(''' + cast(format(cast(@wm_value as datetime), 'yyyy-MM-dd HH:mm:ss') as varchar(20)) + ''', ''YYYY-MM-DD HH24:MI:SS'' )'
		else '''' + @wm_value + '''' end
	end
	return @wm_output
end