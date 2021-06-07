

CREATE PROCEDURE [CTL].[DeleteConfigTask] (
	@ProjectName varchar(255),
	@TaskName varchar(255))
AS

DECLARE @StageID int = 1

DECLARE @ProjectID bigint
DECLARE @TaskID bigint, @SourceID bigint, @TargetID bigint

WHILE @StageID <= 5
BEGIN
	SELECT @ProjectID = ProjectID FROM CTL.ControlProjects WHERE ProjectName = @ProjectName

	SELECT @TaskID = TaskId, @SourceID = SourceId, @TargetID = TargetId
	FROM CTL.ControlTasks T
	WHERE T.ProjectId = @ProjectID
	AND T.TaskName = @TaskName
	AND T.ControlStageId = @StageID

	PRINT @ProjectID
	PRINT @TaskID
	PRINT @SourceID
	PRINT @TargetID


	PRINT 'Deleting from ControlWatermark'
	DELETE FROM CTL.ControlWatermark
	WHERE ControlSourceId = @SourceID

	PRINT 'Deleting from TaskExecutionLog'
	DELETE FROM CTL.TaskExecutionLog
	WHERE ControlTaskId = @TaskID

	PRINT 'Deleting from ControlTaskCommand'
	DELETE FROM CTL.ControlTaskCommand
	WHERE ControlTaskId = @TaskID

	PRINT 'Deleting from ControlWatermark'
	DELETE FROM CTL.ControlWatermark
	WHERE ControlSourceId = @TaskID

	PRINT 'Deleting from ControlTasks'
	DELETE FROM CTL.ControlTasks
	WHERE TaskId = @TaskID

	PRINT 'Deleting from ControlSource'
	DELETE FROM CTL.ControlSource
	WHERE SourceId = @SourceId

	PRINT 'Deleting from ControlTarget'
	DELETE FROM CTL.ControlTarget
	WHERE TargetId = @TargetID


	PRINT 'Deleting from ControlManifest'
	DELETE FROM CTL.ControlManifest
	WHERE SourceObject = @TaskName

	SET @StageID = @StageID + 1

END