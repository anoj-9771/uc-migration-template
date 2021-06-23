CREATE Function [CTL].[udf_GetLastLoadedFile] (@SourceName Varchar(255), @SourceLocation Varchar(255))
Returns Varchar(255)
As
BEGIN
	Declare @File Varchar(255) = ''
	Select @File = (
		select a.TaskOutput from (
			Select l.TaskOutput, TargetName, TargetLocation, row_number()over(order by l.TaskOutput desc) RowNum
			  From CTL.ControlTarget targ
				Join CTL.ControlTasks t
				  On targ.TargetId = t.TargetId
				Join CTL.TaskExecutionLog l
				  On t.TaskId = l.ControlTaskId
				 And l.ExecutionStatus = 'Success'
				 And l.EndTime = (Select Max(EndTime)
									From CTL.TaskExecutionLog
								   Where ControlTaskId = t.TaskId)
				And targ.TargetName = @SourceName
	            And targ.TargetLocation = @SourceLocation
			)a where a.RowNum = 1)
	   --And a.TargetName = @SourceName
	   --And a.TargetLocation = @SourceLocation)

	Return @File
END