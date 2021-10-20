CREATE TABLE [dbo].[DimProperty] (
    [DimPropertySK]           BIGINT          NOT NULL,
    [propertyId]              INT             NOT NULL,
    [sourceSystemCode]        NVARCHAR (256)  NOT NULL,
    [propertyStartDate]       DATE            NULL,
    [propertyEndDate]         DATE            NULL,
    [propertyType]            NVARCHAR (256)  NULL,
    [superiorPropertyType]    NVARCHAR (256)  NULL,
    [areaSize]                DECIMAL (18, 6) NULL,
    [LGA]                     NVARCHAR (256)  NULL,
    [_DLCuratedZoneTimeStamp] DATETIME2 (7)   NOT NULL,
    [_RecordStart]            DATETIME2 (7)   NOT NULL,
    [_RecordEnd]              DATETIME2 (7)   NOT NULL,
    [_RecordDeleted]          INT             NOT NULL,
    [_RecordCurrent]          INT             NOT NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([propertyId]));

