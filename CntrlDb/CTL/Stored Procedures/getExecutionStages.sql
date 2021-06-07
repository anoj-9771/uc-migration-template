

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
AND PS.ControlProjectId = @ProjectID
AND PS.TriggerName = @TriggerName
AND EXISTS (SELECT 1 FROM CTL.ControlTasks T WHERE T.ProjectId = PS.ControlProjectId AND T.ControlStageId = S.ControlStageId)
ORDER BY S.StageSequence