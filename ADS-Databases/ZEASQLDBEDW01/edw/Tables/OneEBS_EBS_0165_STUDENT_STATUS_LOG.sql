CREATE TABLE [edw].[OneEBS_EBS_0165_STUDENT_STATUS_LOG] (
    [INSTITUTION_CONTEXT_ID]  NUMERIC (10)   NULL,
    [ID]                      NUMERIC (10)   NULL,
    [PERSON_CODE]             NUMERIC (10)   NULL,
    [STUDENT_STATUS_CODE]     VARCHAR (4)    NULL,
    [START_DATE]              DATETIME2 (7)           NULL,
    [END_DATE]                DATETIME2 (7)           NULL,
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

