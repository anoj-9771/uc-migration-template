

-- EXEC [CTL].[getExecutionStages] 1

CREATE Proc [CTL].[GetExecutionStages] (
	@ProjectId BigInt,
	@TriggerName VARCHAR(100)
	)
As

SELECT DISTINCT 
	PS.ControlProjectId AS ProjectId, 
	PS.ControlStageId,
	S.StageName,
	S.StageSequence
FROM CTL.ControlProjectSchedule PS
INNER JOIN CTL.ControlStages S ON PS.ControlStageID = S.ControlStageId
WHERE PS.StageEnabled = 1
AND PS.ControlProjectId = @ProjectID
AND PS.TriggerName = @TriggerName
AND EXISTS (SELECT 1 FROM CTL.ControlTasks T WHERE T.ProjectId = PS.ControlProjectId AND T.ControlStageId = S.ControlStageId AND T.TaskEnabled = 1)
ORDER BY S.StageSequence