
CREATE PROCEDURE [CTL].[ResetLogs] (@ProjectID INT)
AS

/*
This SP resets the logs for BatchExecution and TaskExecution.
*/

UPDATE ctl.TaskExecutionLog 
SET ExecutionStatus = 'TERMINATED'
WHERE ExecutionStatus = 'In Progress' AND BatchExecutionId IN 
	(select BatchExecutionLogID from ctl.BatchExecutionLog where ProjectID = @ProjectID)

UPDATE CTL.BatchExecutionLog 
SET BatchExecutionStatus = 'TERMINATED'
WHERE BatchExecutionStatus = 'In Progress' AND ProjectID = @ProjectID

/*
delete from ctl.TaskExecutionLog where BatchExecutionId IN (select BatchExecutionLogID from ctl.BatchExecutionLog where BatchExecutionStatus = 'In Progress')
delete from ctl.BatchExecutionLog where BatchExecutionStatus = 'In Progress'
delete from ctl.TaskExecutionLog where ExecutionStatus = 'In Progress'

*/