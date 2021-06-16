create   proc [dbo].[usp_InsertMonitorLogs]
as
begin
	EXEC [dbo].[usp_Upsert] @schema_src= 'stg', @source= 'ADFPipelineRun', @schema_trg= 'stg', @target = 'ADFPipelineRun', @key = 'RunId,TimeGenerated', @partition = 'TimeGenerated'
	EXEC [dbo].[usp_Upsert] @schema_src= 'stg', @source= 'ADFActivityRun', @schema_trg= 'stg', @target = 'ADFActivityRun', @key = 'ActivityRunId,TimeGenerated', @partition = 'TimeGenerated'
end