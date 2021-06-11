CREATE Procedure [CTL].[UpdateWatermarks] @SourceId BigInt, @Watermark Varchar(1000)
As

BEGIN


	DECLARE @WatermarkUpdated varchar(500)
	IF ISDATE(@WatermarkUpdated) = 1
		SET @WatermarkUpdated = FORMAT(TRY_CONVERT(DATETIME, @Watermark), 'yyyy-MM-ddTHH:mm:ss')
	ELSE
		SET @WatermarkUpdated = @Watermark

	Update CTL.ControlWatermark
	   Set Watermarks = @WatermarkUpdated
	 Where ControlSourceId = @SourceId
END