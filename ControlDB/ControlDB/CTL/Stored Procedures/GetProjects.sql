CREATE PROCEDURE [CTL].[GetProjects] (
	@TriggerName varchar(100)
)
As

--Get List of ProjectID for which Triggers have been configured
SELECT DISTINCT 
	P.ProjectId
	,P.ProjectName
FROM CTL.ControlProjectSchedule S
INNER JOIN CTL.ControlProjects P ON S.ControlProjectId = P.ProjectId
AND P.Enabled = 1
AND S.TriggerName = @TriggerName

/*
SELECT ProjectId as ControlProjectID, ProjectName
FROM CTL.ControlProjects
WHERE Enabled = 1
AND TriggerName = @TriggerName
*/