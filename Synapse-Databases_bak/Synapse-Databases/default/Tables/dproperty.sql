CREATE TABLE [default].[dproperty] (
    [DimPropertySK]           NVARCHAR (MAX) NULL,
    [propertyId]              NVARCHAR (MAX) NULL,
    [sourceSystemCode]        NVARCHAR (MAX) NULL,
    [propertyStartDate]       NVARCHAR (MAX) NULL,
    [propertyEndDate]         NVARCHAR (MAX) NULL,
    [propertyType]            NVARCHAR (MAX) NULL,
    [superiorPropertyType]    NVARCHAR (MAX) NULL,
    [areaSize]                NVARCHAR (MAX) NULL,
    [LGA]                     NVARCHAR (MAX) NULL,
    [_DLCuratedZoneTimeStamp] NVARCHAR (MAX) NULL,
    [_RecordStart]            NVARCHAR (MAX) NULL,
    [_RecordEnd]              NVARCHAR (MAX) NULL,
    [_RecordDeleted]          NVARCHAR (MAX) NULL,
    [_RecordCurrent]          NVARCHAR (MAX) NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

