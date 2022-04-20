CREATE PROCEDURE [CTL].[UpdateBusinessRecCurated]
(
  @TargetMeasureValue decimal(28,7),
	@BusinessRecId bigint,
	@UpdatedBatchExecutionId bigint,
	@UpdatedTaskExecutionLogId bigint,
	@CuratedPipelineRunID varchar(255),
	@UpdatedDateTime datetime
)
AS

BEGIN

DECLARE 
	@vsourcemeasurevalue decimal(28,7),
	@vBusinessReconGroup varchar(255),
	@vMeasureId varchar(255),	
	@vMeasureName varchar(255)

	SELECT 
	@vsourcemeasurevalue = bc.SourceMeasureValue,
	@vBusinessReconGroup = bc.BusinessReconGroup,
	@vMeasureId = bc.MeasureId,
	@vMeasureName = bc.MeasureName
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

	UPDATE CTL.BusinessRecCurated
		SET	   BusinessRecResult = 'FAIL',
			   UpdatedBatchExecutionId = @UpdatedBatchExecutionId,
			   UpdatedTaskExecutionLogId = @UpdatedTaskExecutionLogId,
			   CuratedPipelineRunID = @CuratedPipelineRunID,
			   UpdatedDateTime = @UpdatedDateTime
		WHERE BusinessReconGroup = @vBusinessReconGroup
		  AND MeasureId = @vMeasureId
		  AND MeasureName = @vMeasureName
		  AND BusinessRecResult is null
		  AND UpdatedDateTime < @UpdatedDateTime

END