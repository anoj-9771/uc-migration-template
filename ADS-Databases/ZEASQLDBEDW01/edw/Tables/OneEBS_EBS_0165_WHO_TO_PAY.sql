CREATE TABLE [edw].[OneEBS_EBS_0165_WHO_TO_PAY] (
    [WHO_TO_PAY]              VARCHAR (10)   NULL,
    [LONG_DESCRIPTION]        VARCHAR (40)   NULL,
    [SHORT_DESCRIPTION]       VARCHAR (13)   NULL,
    [BAL_TO_UPDATE]           NUMERIC (1)    NULL,
    [TRANSFER_ALLOWED]        VARCHAR (1)    NULL,
    [CREATED_BY]              VARCHAR (30)   NULL,
    [CREATED_DATE]            DATETIME2 (7)  NULL,
    [UPDATED_BY]              VARCHAR (30)   NULL,
    [UPDATED_DATE]            DATETIME2 (7)  NULL,
    [IS_INCLUDED_IN_RETURN]   VARCHAR (1)    NULL,
    [INSTITUTION_CONTEXT_ID]  NUMERIC (10)   NULL,
    [_transaction_date]       DATETIME2 (7)  NULL,
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

