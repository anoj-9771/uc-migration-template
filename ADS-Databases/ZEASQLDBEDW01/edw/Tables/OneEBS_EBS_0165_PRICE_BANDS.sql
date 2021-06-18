CREATE TABLE [edw].[OneEBS_EBS_0165_PRICE_BANDS] (
    [INSTITUTION_CONTEXT_ID]  NUMERIC (10)   NULL,
    [PRICE_BAND_ID]           NUMERIC (10)   NULL,
    [PRICE_BAND]              VARCHAR (10)   NULL,
    [DESCRIPTION]             VARCHAR (40)   NULL,
    [CREATED_BY]              VARCHAR (30)   NULL,
    [CREATED_DATE]            DATETIME2 (7)           NULL,
    [UPDATED_BY]              VARCHAR (30)   NULL,
    [UPDATED_DATE]            DATETIME2 (7)           NULL,
    [_transaction_date]       DATETIME2 (7)       NULL,
    [year]                    NVARCHAR (MAX) NULL,
    [month]                   NVARCHAR (MAX) NULL,
    [day]                     NVARCHAR (MAX) NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7)  NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7)  NULL,
    [_RecordStart]            DATETIME2 (7)  NULL,
    [_RecordEnd]              DATETIME2 (7)  NULL,
    [_RecordDeleted]          INT            NULL,
    [_RecordCurrent]          INT            NULL
);

