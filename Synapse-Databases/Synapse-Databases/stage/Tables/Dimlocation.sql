CREATE TABLE [stage].[Dimlocation] (
    [DimLocationSK]           BIGINT         NOT NULL,
    [LocationID]              INT            NOT NULL,
    [formattedAddress]        NVARCHAR (256) NULL,
    [streetName]              NVARCHAR (256) NULL,
    [StreetType]              NVARCHAR (256) NULL,
    [LGA]                     NVARCHAR (256) NULL,
    [suburb]                  NVARCHAR (256) NULL,
    [state]                   NVARCHAR (256) NULL,
    [latitude]                DECIMAL (9, 6) NULL,
    [longitude]               DECIMAL (9, 6) NULL,
    [_DLCuratedZoneTimeStamp] DATETIME2 (7)  NOT NULL,
    [_RecordStart]            DATETIME2 (7)  NOT NULL,
    [_RecordEnd]              DATETIME2 (7)  NOT NULL,
    [_RecordDeleted]          INT            NOT NULL,
    [_RecordCurrent]          INT            NOT NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

