CREATE Procedure [CTL].[CreateTaskLog] (@BatchLogId BigInt, @TaskId BigInt)
As
BEGIN
	If (Select Count(*) From CTL.BatchExecutionLog Where BatchExecutionLogId = @BatchLogId And BatchExecutionStatus = 'In Progress') = 0
	  BEGIN
		RAISERROR('Batch execution log could not be located',16,1)
	  END
	Else
	  BEGIN
	    If (Select Count(*) From CTL.TaskExecutionLog Where ControlTaskId = @TaskId And ExecutionStatus = 'In Progress') > 0
		  BEGIN
			RAISERROR('Task is already in progress',16,1)
		  END
		Else
		  BEGIN
			Insert Into CTL.TaskExecutionLog (BatchExecutionId, ControlTaskId, StartTime, ExecutionStatus) 
				Values (@BatchLogId, @TaskId, [CTL].[udf_GetDateLocalTZ](), 'In Progress')
			Select @@IDENTITY TaskLogId
		  END
	  END
END