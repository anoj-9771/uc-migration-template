
CREATE VIEW [CTL].[vw_TaskExecutionDetails]
AS

SELECT [ExecutionLogId]
      ,[BatchExecutionId]
      ,[ControlTaskId]
	  ,p.ProjectName
	  ,p.ProjectId
	  ,s.ControlStageId as StageId
	  ,s.StageName
	  ,t.TaskName
      ,[StartTime]
      ,[EndTime]
	  ,DATEDIFF(MINUTE,StartTime,EndTime) as Duration
      ,[ExecutionStatus]
      ,[ErrorMessage]
      ,[TaskOutput]
  FROM [CTL].[TaskExecutionLog] as l
  left join [CTL].[ControlTasks] as t 
  on l.ControlTaskId = t.TaskId
  left join [CTL].[ControlStages] as s
  on t.ControlStageId = s.ControlStageId
  left join [CTL].[ControlProjects] as p
  on t.ProjectId = p.ProjectId
  --order by BatchExecutionId desc, StartTime desc--, ExecutionStatus, duration desc