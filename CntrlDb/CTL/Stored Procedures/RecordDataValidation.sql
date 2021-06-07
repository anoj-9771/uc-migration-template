
CREATE PROCEDURE [CTL].[RecordDataValidation] (
	@ProjectRunID varchar(50),
	@ObjectName varchar(255),
	@ValidationType varchar(10),
	@HighWatermark varchar(100),
	@RecordCount bigint,
	@TotalValue bigint,
	@MinValue bigint,
	@MaxValue bigint)
AS

IF UPPER(@ValidationType) = 'SOURCE'
BEGIN

	INSERT INTO CTL.ControlDataLoadValidation (
		ProjectRunID
		,ObjectName
		,SourceHighWatermark
		,SourceRecordCount
		,SourceTotalValue
		,SourceMinValue
		,SourceMaxValue)
	VALUES (
		@ProjectRunID
		,@ObjectName
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
	AND ObjectName = @ObjectName

END