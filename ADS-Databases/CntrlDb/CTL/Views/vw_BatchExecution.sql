

CREATE VIEW CTL.vw_BatchExecution
AS
SELECT B.[BatchExecutionLogId]
      ,B.[StartDate]
      ,B.[EndDate]
      ,B.[BatchExecutionStatus]
      ,B.[ErrorMessage]
      ,B.[ProjectID]
	  ,P.ProjectName
  FROM [CTL].[BatchExecutionLog] B
  LEFT JOIN CTL.ControlProjects P ON B.ProjectID = P.ProjectId