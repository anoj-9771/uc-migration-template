DECLARE @SummaryOfChanges TABLE(Change VARCHAR(20));
WITH [_Common] AS
(
    SELECT *
    FROM (
        SELECT 'IngestionDefault' [KeyGroup], 'RawPath' [Key], '/raw/$SYSTEM$/$SCHEMA$_$TABLE$/$yyyy$/$MM$/$dd$/$HH$$mm$/$guid$.$EXT$' [Value]
        UNION SELECT 'IngestionDefault' [KeyGroup], 'CleansedPath' [Key], '/cleansed/$SYSTEM$/$SCHEMA$_$TABLE$/' [Value]
        UNION SELECT 'IngestionDefault' [KeyGroup], 'SourceKeyVaultSecret' [Key], 'daf-sa-lake-connectionstring' [Value]
        UNION SELECT 'IngestionDefault' [KeyGroup], 'Enabled' [Key], '1' [Value]
        UNION SELECT 'IngestionSheet' [KeyGroup], 'Index' [Key], '0' [Value]
        UNION SELECT 'IngestionSheet' [KeyGroup], 'Limit' [Key], '0' [Value]
        UNION SELECT 'IngestionSheet' [KeyGroup], 'Path' [Key], 'testonly/labware_upload.csv' [Value]
        UNION SELECT 'TriggerInterval' [KeyGroup], '15Min' [Key], 'maximo|15min,iicats|15min,scada|15min,hydstra|15Min' [Value]
        UNION SELECT 'TriggerInterval' [KeyGroup], '4Hrs' [Key], '' [Value]
        UNION SELECT 'SuccessiveCleansed' [KeyGroup], 'SystemCode' [Key], 'maximo' [Value]
    ) T
),
[_Env] AS
(
    SELECT Value from dbo.config where [KeyGroup] = 'System' and [Key] = 'Environment' 
),
[_Override] AS
(
    SELECT *
    FROM (
        SELECT 'cleansedLayer' [KeyGroup], 'skipIngestion' [Key], 'swirlarchive' [Value]
    ) O
    WHERE EXISTS(SELECT * FROM [_Env] WHERE VALUE = 'PROD')
),
[_SourceConfig] AS
(
    SELECT  COALESCE([_Override].[KeyGroup], [_Common].[KeyGroup]) [KeyGroup]
        ,COALESCE([_Override].[Key], [_Common].[Key]) [Key]
        ,COALESCE([_Override].[Value], [_Common].[Value]) [Value]
        ,CONVERT(DATETIME, CONVERT(DATETIMEOFFSET, GETDATE()) AT TIME ZONE 'AUS Eastern Standard Time') [CreatedDTS]
    FROM [_Common]
    FULL JOIN [_Override]
    ON ([_Common].[KeyGroup] = [_Override].[KeyGroup] and [_Common].[Key] = [_Override].[Key])
)
MERGE INTO [dbo].[Config] AS TGT
USING [_SourceConfig]
AS SRC ON TGT.[KeyGroup] = SRC.[KeyGroup] AND TGT.[Key] = SRC.[Key]
WHEN NOT MATCHED BY TARGET THEN
    INSERT ([KeyGroup],[Key],[Value],[CreatedDTS]) VALUES ([KeyGroup],[Key],[Value],[CreatedDTS])
WHEN MATCHED THEN
	UPDATE SET [Value] = SRC.[Value]    
OUTPUT $action 
    INTO @SummaryOfChanges;
SELECT * FROM @SummaryOfChanges;

