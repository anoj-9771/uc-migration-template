

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

;WITH [Systems] AS
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

MERGE INTO [dbo].[ExtractLoadManifest] as target using(
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
    ,CONVERT(DATETIME, CONVERT(DATETIMEOFFSET, GETDATE()) AT TIME ZONE 'AUS Eastern Standard Time') AS CreatedDTS
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
) as source ON
        target.systemCode = source.systemCode 
    and target.sourceSchema = source.sourceSchema
    and target.sourceTableName = source.sourceTableName
when matched and exists(
    select 
        target.[SourceQuery]
        ,target.[SourceKeyVaultSecret]
        ,target.[SourceHandler]
        ,target.[LoadType]
        ,target.[BusinessKeyColumn]
        ,target.[WatermarkColumn]
        ,target.[RawPath]
        ,target.[RawHandler]
        ,target.[CleansedHandler]
        ,target.[CleansedPath]
        ,target.[DestinationSchema]
        ,target.[DestinationTableName]
        ,target.[ExtendedProperties]
        ,target.[Enabled]
    EXCEPT
    select
         source.[SourceQuery]
        ,source.[SourceKeyVaultSecret]
        ,source.[SourceHandler]
        ,source.[LoadType]
        ,source.[BusinessKeyColumn]
        ,source.[WatermarkColumn]
        ,source.[RawPath]
        ,source.[RawHandler]
        ,source.[CleansedHandler]
        ,source.[CleansedPath]
        ,source.[DestinationSchema]
        ,source.[DestinationTableName]
        ,source.[ExtendedProperties]
        ,source.[Enabled]
)
then update set
    target.[SourceQuery] = source.[SourceQuery]
    ,target.[SourceKeyVaultSecret] = source.[SourceKeyVaultSecret]
    ,target.[SourceHandler] = source.[SourceHandler]
    ,target.[LoadType] = source.[LoadType]
    ,target.[BusinessKeyColumn] = source.[BusinessKeyColumn]
    ,target.[WatermarkColumn] = source.[WatermarkColumn]
    ,target.[RawPath] = source.[RawPath]
    ,target.[RawHandler] = source.[RawHandler]
    ,target.[CleansedHandler] = source.[CleansedHandler]
    ,target.[CleansedPath] = source.[CleansedPath]
    ,target.[DestinationSchema] = source.[DestinationSchema]
    ,target.[DestinationTableName] = source.[DestinationTableName]
    ,target.[ExtendedProperties] = source.[ExtendedProperties]
when not matched then insert(
    [SourceID]
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
    ,[CreatedDTS]
)
values(
    source.[SourceID]
    ,source.[SystemCode]
    ,source.[SourceSchema]
    ,source.[SourceTableName]
    ,source.[SourceQuery]
    ,source.[SourceKeyVaultSecret]
    ,source.[SourceHandler]
    ,source.[LoadType]
    ,source.[BusinessKeyColumn]
    ,source.[WatermarkColumn]
    ,source.[RawPath]
    ,source.[RawHandler]
    ,source.[CleansedHandler]
    ,source.[CleansedPath]
    ,source.[DestinationSchema]
    ,source.[DestinationTableName]
    ,source.[ExtendedProperties]
    ,source.[Enabled]
    ,source.[CreatedDTS]
);
END

GO