
CREATE PROCEDURE [CTL].[RecordDataValidation] (
	@ProjectRunID varchar(50),
	@SourceID bigint,
	@ValidationType varchar(10),
	@HighWatermark varchar(100),
	@RecordCount bigint,
	@TotalValue bigint,
	@MinValue bigint,
	@MaxValue bigint)
AS

DECLARE @SourceName varchar(255)
DECLARE @ProjectName varchar(100)

SELECT @SourceName = TaskName, @ProjectName = ProjectName
FROM CTL.vw_ControlConfiguration WHERE SourceId = @SourceID

IF UPPER(@ValidationType) = 'SOURCE'
BEGIN

	INSERT INTO CTL.ControlDataLoadValidation (
		ProjectRunID
		,DataValidationDate
		,ObjectName
		,ProjectName
		,SourceHighWatermark
		,SourceRecordCount
		,SourceTotalValue
		,SourceMinValue
		,SourceMaxValue)
	VALUES (
		@ProjectRunID
		,CONVERT(DATE, [CTL].[udf_GetDateLocalTZ]())
		,@SourceName
		,@ProjectName
		,@HighWatermark
		,@RecordCount
		,@TotalValue
		,@MinValue
		,@MaxValue)

END
ELSE
BEGIN
	UPDATE CTL.ControlDataLoadValidation
	SET TargetHighWatermark = @HighWatermark
	,TargetRecordCount = @RecordCount
	,TargetTotalValue = @TotalValue
	,TargetMinValue = @MinValue 
	,TargetMaxValue = @MaxValue

	WHERE ProjectRunID = @ProjectRunID
	AND ObjectName = @SourceName

END