CREATE PROCEDURE [CTL].[GetAPIParams] (@SourceId BigInt)
AS

BEGIN

	DECLARE @MappingInfo VARCHAR(MAX)
	IF EXISTS(SELECT 1 FROM CTL.ControlSource WHERE SourceID = @SourceId AND AdditionalProperty <> '')
	BEGIN

		DECLARE @Prop varchar(max)
		SELECT @Prop = AdditionalProperty FROM CTL.ControlSource WHERE SourceID = @SourceId

		DECLARE @CurrPos int, @NextPos int

		SET @CurrPos = 0
		SET @NextPos = CHARINDEX('|', @Prop, @CurrPos+1)
		DECLARE @Cols varchar(MAX) = SUBSTRING(@Prop, @CurrPos+1, @NextPos - @CurrPos - 1)

		SET @CurrPos = @NextPos
		SET @NextPos = CHARINDEX('|', @Prop, @CurrPos+1)
		DECLARE @CollectionFilter varchar(100) = SUBSTRING(@Prop, @CurrPos+1, @NextPos - @CurrPos - 1)

		/*********Get Mapping Information ***************/
		IF @Cols != ''
		BEGIN
			DECLARE @json_construct varchar(MAX) = '{"type": "TabularTranslator", "mappings": {X}, "collectionReference": "{CollectionFilter}"}';
			DECLARE @json VARCHAR(MAX);
    
			SET @json = (
				SELECT
					c.[value] AS 'source.path', 
					c.[value] AS 'sink.path' 
				FROM (select value from string_split(@cols, ',')) as c
			FOR JSON PATH );
 
			SET @MappingInfo = REPLACE(REPLACE(@json_construct,'{X}', @json),'{CollectionFilter}', @CollectionFilter) 
		END
	END

	SELECT @MappingInfo AS MappingInfo
END

--[CTL].[GetAPIParams] 622