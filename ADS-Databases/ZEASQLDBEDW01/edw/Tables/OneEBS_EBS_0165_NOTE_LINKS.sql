CREATE TABLE [edw].[OneEBS_EBS_0165_NOTE_LINKS] (
    [CREATED_BY]              VARCHAR (30)   NULL,
    [CREATED_BY_PERSON_CODE]  NUMERIC (10)   NULL,
    [CREATED_DATE]            DATETIME2 (7)  NULL,
    [ID]                      NUMERIC (10)   NULL,
    [INSTITUTION_CONTEXT_ID]  NUMERIC (10)   NULL,
    [IS_READ]                 VARCHAR (1)    NULL,
    [NOTES_ID]                NUMERIC (10)   NULL,
    [NOTE_TYPE]               VARCHAR (40)   NULL,
    [PARENT_ID]               NUMERIC (10)   NULL,
    [PARENT_TABLE]            VARCHAR (30)   NULL,
    [_transaction_date]       DATETIME2 (7)  NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7)  NULL,
    [year]                    NVARCHAR (MAX) NULL,
    [month]                   NVARCHAR (MAX) NULL,
    [day]                     NVARCHAR (MAX) NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7)  NULL,
    [_RecordStart]            DATETIME2 (7)  NULL,
    [_RecordEnd]              DATETIME2 (7)  NULL,
    [_RecordDeleted]          INT            NULL,
    [_RecordCurrent]          INT            NULL
);

