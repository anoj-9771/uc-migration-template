CREATE Procedure [CTL].[UpdateBatchLog] (@BatchLogId BigInt, @Status Varchar(50), @ErrorMessage Varchar(2000))
As
BEGIN

	If (Select Count(*) From CTL.BatchExecutionLog Where BatchExecutionLogId = @BatchLogId) = 0 
		BEGIN
			RAISERROR('Execution batch could not be located', 16, 1) 
		END
	Else
	  BEGIN
		Update CTL.BatchExecutionLog
		   Set EndDate = [CTL].[udf_GetDateLocalTZ](),
		       BatchExecutionStatus = @Status,
			   ErrorMessage = @ErrorMessage
		 Where BatchExecutionLogId = @BatchLogId
	  END

END