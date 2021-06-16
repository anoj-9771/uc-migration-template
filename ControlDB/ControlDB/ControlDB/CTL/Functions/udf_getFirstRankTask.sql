

create    function [CTL].[udf_getFirstRankTask](@TaskId bigint)
returns varchar(500)
as
begin
	return(
		select wm.[Column] 
		from ctl.Task tsk
		join ctl.Watermark wm on tsk.SourceId = wm.DatasetId
		where tsk.ProcessID = (select ProcessId from ctl.Task where TaskId = @TaskId) and tsk.Rank = 1
	)
end