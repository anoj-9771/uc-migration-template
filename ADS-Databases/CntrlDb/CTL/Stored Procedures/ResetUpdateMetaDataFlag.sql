CREATE PROCEDURE CTL.ResetUpdateMetaDataFlag(
 @TaskID bigint)
 AS

 DECLARE @ProjectID INT
 DECLARE @TaskName varchar(255)

 SELECT @ProjectID = ProjectID, @TaskName = TaskName
 FROM CTL.ControlTasks
 WHERE TaskID = @TaskID

 UPDATE CTL.ControlTasks
 SET UpdateMetaData = 0
 WHERE ProjectID = @ProjectID
 AND TaskName = @TaskName