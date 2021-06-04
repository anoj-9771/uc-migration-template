create   view dbo.vw_ADFPipelineRun as
	select *, 
		case when json_value(Predecessors,'$[0].PipelineRunId') in (select RunId from dbo.ADFPipelineRun where status in ('Failed', 'Succeeded','Cancelled')) 
			then json_value(Predecessors,'$[0].PipelineRunId') 
			else null end Predecessors_RunId 
	from dbo.ADFPipelineRun
	where RunId in (select distinct PipelineRunId from dbo.vw_ADFActivityRun) 
	and status in ('Failed', 'Succeeded','Cancelled')