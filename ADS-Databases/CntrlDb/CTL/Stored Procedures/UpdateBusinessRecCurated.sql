CREATE PROCEDURE [CTL].[UpdateBusinessRecCurated] (
	@TargetMeasureValue decimal(28,7),
	@BusinessRecId bigint,
	@UpdatedBatchExecutionId bigint,
	@UpdatedTaskExecutionLogId bigint,
	@CuratedPipelineRunID varchar(255),
	@UpdatedDateTime datetime
)
AS

BEGIN

DECLARE @vsourcemeasurevalue bigint

	SELECT 
	@vsourcemeasurevalue = bc.SourceMeasureValue 
	FROM CTL.BusinessRecCurated bc 
	WHERE bc.BusinessRecId = @BusinessRecId

	IF @vsourcemeasurevalue = @TargetMeasureValue
	BEGIN
		UPDATE CTL.BusinessRecCurated
		SET	   TargetMeasureValue = @TargetMeasureValue,
			   BusinessRecResult = 'PASS',
			   UpdatedBatchExecutionId = @UpdatedBatchExecutionId,
			   UpdatedTaskExecutionLogId = @UpdatedTaskExecutionLogId,
			   CuratedPipelineRunID = @CuratedPipelineRunID,
			   UpdatedDateTime = @UpdatedDateTime
		WHERE  [BusinessRecId] = @BusinessRecId
	END
	ELSE
	BEGIN
		UPDATE CTL.BusinessRecCurated
		SET	   TargetMeasureValue = @TargetMeasureValue,
			   BusinessRecResult = 'FAIL',
			   UpdatedBatchExecutionId = @UpdatedBatchExecutionId,
			   UpdatedTaskExecutionLogId = @UpdatedTaskExecutionLogId,
			   CuratedPipelineRunID = @CuratedPipelineRunID,
			   UpdatedDateTime = @UpdatedDateTime
		WHERE  [BusinessRecId] = @BusinessRecId
	END

END