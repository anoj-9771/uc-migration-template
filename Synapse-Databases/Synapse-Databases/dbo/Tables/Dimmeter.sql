CREATE TABLE [dbo].[Dimmeter] (
    [DimMeterSK]              BIGINT         NULL,
    [sourceSystemCode]        NVARCHAR (256) NULL,
    [meterId]                 NVARCHAR (256) NULL,
    [meterSize]               NVARCHAR (256) NULL,
    [waterMeterType]          NVARCHAR (256) NULL,
    [_DLCuratedZoneTimeStamp] DATETIME2 (7)  NULL,
    [_RecordStart]            DATETIME2 (7)  NULL,
    [_RecordEnd]              DATETIME2 (7)  NULL,
    [_RecordDeleted]          INT            NULL,
    [_RecordCurrent]          INT            NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([meterId]));

