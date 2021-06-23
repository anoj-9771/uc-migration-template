CREATE Procedure [CTL].[UpdateTaskLog] (@BatchLogId BigInt, @TaskId BigInt, @ExecutionLogId BigInt, @Status Varchar(50), @ErrorMessage varchar(2000), @Output Varchar(2000))
As
BEGIN
	If (Select Count(*) From CTL.BatchExecutionLog Where BatchExecutionLogId = @BatchLogId And BatchExecutionStatus = 'In Progress') = 0
	  BEGIN
		RAISERROR('Batch execution log could not be located',16,1)
	  END
	Else
	  BEGIN
	    If (Select Count(*) From CTL.TaskExecutionLog Where ControlTaskId = @TaskId And ExecutionStatus = 'In Progress') = 0
		  BEGIN
			RAISERROR('Task execution log could not be located',16,1)
		  END
		Else
		  BEGIN
			Update CTL.TaskExecutionLog
			   Set EndTime = [CTL].[udf_GetDateLocalTZ](),
			       ExecutionStatus = @Status,
				   ErrorMessage = @ErrorMessage,
				   TaskOutput = @Output
			 Where ExecutionLogId = @ExecutionLogId
		  END
	  END
END