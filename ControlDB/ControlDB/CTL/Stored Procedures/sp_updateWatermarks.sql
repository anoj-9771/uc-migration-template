CREATE Procedure [CTL].[sp_updateWatermarks] @SourceId BigInt, @Watermark Varchar(max)
As
declare @values table([Column] nvarchar(100), [Value] varchar(100))
begin
	insert into @values
	select [key], [value] from openjson(json_query(@Watermark, '$[0]'))
	delete from @values where [Value] is null
	if (select count(*) from @values) > 0
	begin
		declare @wm varchar(100)
		set @wm = 1
		Update wm
			Set wm.[Value] = coalesce(vs.[Value], wm.[Value])
		from CTL.Watermark wm
		join ctl.Type wm_tp on wm.TypeId = wm_tp.TypeId
		join @values vs on wm.DatasetId = @SourceId
		and wm.[Column] = vs.[Column]
	end
END