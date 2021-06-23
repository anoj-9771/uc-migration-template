CREATE TABLE [edw].[OneEBS_EBS_0165_PEOPLE_UNIT_LINKS] (
    [CREATED_DATE]            DATETIME2 (7)           NULL,
    [INSTITUTION_CONTEXT_ID]  NUMERIC (10)   NULL,
    [ID]                      NUMERIC (10)   NULL,
    [PARENT_ID]               NUMERIC (10)   NULL,
    [CHILD_ID]                NUMERIC (10)   NULL,
    [MANDATORY]               VARCHAR (1)    NULL,
    [CREATED_BY]              VARCHAR (30)   NULL,
    [UPDATED_BY]              VARCHAR (30)   NULL,
    [UPDATED_DATE]            DATETIME2 (7)           NULL,
    [IS_ELECTIVE]             VARCHAR (1)    NULL,
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

