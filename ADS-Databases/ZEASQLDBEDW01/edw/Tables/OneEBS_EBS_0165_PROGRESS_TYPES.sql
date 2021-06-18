CREATE TABLE [edw].[OneEBS_EBS_0165_PROGRESS_TYPES] (
    [FES_ACTIVE]              VARCHAR (1)    NULL,
    [TYPE_CODE]               VARCHAR (6)    NULL,
    [STUDENT_STATUS]          VARCHAR (1)    NULL,
    [DESCRIPTION]             VARCHAR (30)   NULL,
    [INSTITUTION_CONTEXT_ID]  NUMERIC (10)   NULL,
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

