CREATE Procedure [CTL].[CreateBatchLog] (@ProjectID bigint)
As
BEGIN

	If (Select Count(*) From CTL.BatchExecutionLog Where BatchExecutionStatus = 'In Progress' AND ProjectID = @ProjectID) > 0 
		BEGIN
			RAISERROR('Execution batch already in progress', 16, 1) 
		END
	Else
	  BEGIN
		Insert Into CTL.BatchExecutionLog (StartDate, BatchExecutionStatus, ProjectID) Values ([CTL].[udf_GetDateLocalTZ](), 'In Progress', @ProjectID)
		Select @@IDENTITY BatchLogId
	  END

END