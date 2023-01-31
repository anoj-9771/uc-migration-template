

CREATE PROCEDURE [dbo].[AddIngestion] 
	@SystemCode VARCHAR(MAX),
	@Schema VARCHAR(MAX),
	@Table VARCHAR(MAX),
	@Query VARCHAR(MAX),
	@WatermarkColumn VARCHAR(MAX),
	@SourceHandler VARCHAR(MAX),
	@RawFileExtension VARCHAR(MAX),
	@KeyVaultSecret VARCHAR(MAX),
	@ExtendedProperties VARCHAR(MAX),
	@RawHandler VARCHAR(MAX) = 'raw-load',
	@CleansedHandler VARCHAR(MAX) = 'cleansed-load',
	@SystemPath VARCHAR(MAX) = null,
	@DestinationSchema VARCHAR(MAX) = null

AS
BEGIN

SET @SystemPath = COALESCE(@SystemPath,@SystemCode)
SET @DestinationSchema = COALESCE(@DestinationSchema,@Schema)
SET @RawHandler = COALESCE(@RawHandler,'raw-load')
SET @CleansedHandler = COALESCE(@CleansedHandler,'cleansed-load')


/* TODO: UPDATE EXISTING CONFIGURATION */
IF (EXISTS(SELECT * FROM [dbo].[ExtractLoadManifest] WHERE SystemCode = @SystemCode AND SourceSchema = @Schema AND SourceTableName = @Table))
BEGIN
	RETURN 
END;

WITH [Systems] AS
(
	SELECT 
	LEFT([SourceID], 2) [Order], [SystemCode], MAX([SourceID]) [LastSourceID]
	FROM [dbo].[ExtractLoadManifest] 
	GROUP BY SystemCode, LEFT([SourceID], 2)
)
,[Config] AS
(
	SELECT [RawPath], [CleansedPath], [SourceKeyVaultSecret], [Enabled] 
	FROM   
	( 
		SELECT [Key], [Value] FROM [dbo].[Config]
		WHERE [KeyGroup] = 'IngestionDefault'
	) T
	PIVOT(
		MAX([Value]) 
		FOR [Key] IN (
			[RawPath], 
			[CleansedPath], 
			[SourceKeyVaultSecret],
			[Enabled] 
		)
	) T
)
INSERT INTO [dbo].[ExtractLoadManifest]
([SourceID]
,[SystemCode]
,[SourceSchema]
,[SourceTableName]
,[SourceQuery]
,[SourceKeyVaultSecret]
,[SourceHandler]
,[LoadType]
,[BusinessKeyColumn]
,[WatermarkColumn]
,[RawPath]
,[RawHandler]
,[CleansedHandler]
,[CleansedPath]
,[DestinationSchema]
,[DestinationTableName]
,[ExtendedProperties]
,[Enabled]
,[CreatedDTS])
SELECT 
CASE 
WHEN NOT(EXISTS(SELECT * FROM [Systems])) THEN 20001 /* EMPTY */
WHEN S.[Order] IS NULL THEN CONCAT((SELECT MAX([Order]) FROM [Systems])+1, '001') /*INCREMENT NEW SYSTEM*/
ELSE S.[LastSourceID]+1 /*INCREMENT NEW SOURCE*/
END [SourceID]
,R.[SystemCode]
,[SourceSchema]
,[SourceTableName]
,[SourceQuery]
,[SourceKeyVaultSecret]
,[SourceHandler]
,[LoadType]
,[BusinessKeyColumn]
,[WatermarkColumn]
,[RawPath]
,[RawHandler]
,[CleansedHandler]
,[CleansedPath]
,[DestinationSchema]
,[DestinationTableName]
,[ExtendedProperties]
,[Enabled]
,CONVERT(DATETIME, CONVERT(DATETIMEOFFSET, GETDATE()) AT TIME ZONE 'AUS Eastern Standard Time')
FROM (
	SELECT 
	@SystemCode [SystemCode]
	,@Schema [SourceSchema]
	,@Table [SourceTableName]
	,@Query [SourceQuery]
	,COALESCE(@KeyVaultSecret, (SELECT [SourceKeyVaultSecret] FROM [Config])) [SourceKeyVaultSecret]
	,ISNULL(@SourceHandler, 'sql-load') [SourceHandler]
	,NULL [LoadType]
	,NULL [BusinessKeyColumn]
	,@WatermarkColumn [WatermarkColumn]
	,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
	(SELECT [RawPath] FROM [Config])
	,'$SYSTEM$', @SystemPath) 
	,'$SCHEMA$', @DestinationSchema) 
	,'$TABLE$', @Table) 
	,'$EXT$', ISNULL(@RawFileExtension, 'parquet'))
	,'$guid$.', IIF(@RawFileExtension='', '', '$guid$.'))
	[RawPath]
	,@RawHandler [RawHandler]
	,@CleansedHandler [CleansedHandler]
	,REPLACE(REPLACE(REPLACE(
	(SELECT [CleansedPath] FROM [Config])
	,'$SYSTEM$', @SystemPath) 
	,'$SCHEMA$', @DestinationSchema) 
	,'$TABLE$', @Table)[CleansedPath]
	,REPLACE(REPLACE(@DestinationSchema
	,'', '')
	,' ', '') [DestinationSchema]
	,REPLACE(REPLACE(@Table
	,'', '')
	,' ', '')[DestinationTableName]
	,@ExtendedProperties [ExtendedProperties]
	,(SELECT [Enabled] FROM [Config]) [Enabled]
) R
LEFT JOIN [Systems] S ON S.[SystemCode] = R.[SystemCode]

END

GO