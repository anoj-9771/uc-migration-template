CREATE TABLE [edw].[OneEBS_EBS_0165_AWARDS_PRINTED_DETAILS] (
    [INSTITUTION_CONTEXT_ID]       NUMERIC (10)   NULL,
    [ID]                           NUMERIC (10)   NULL,
    [ATTAINMENT_CODE]              NUMERIC (10)   NULL,
    [TESTAMUR_DATE_LAST_PRINTED]   DATETIME2 (7)           NULL,
    [TRANSCRIPT_DATE_LAST_PRINTED] DATETIME2 (7)           NULL,
    [EMPLOYER_REPORT_LAST_PRINTED] DATETIME2 (7)           NULL,
    [HAS_TRANSCRIPT_CHANGED]       VARCHAR (1)    NULL,
    [CREATED_BY]                   VARCHAR (30)   NULL,
    [CREATED_DATE]                 DATETIME2 (7)           NULL,
    [UPDATED_BY]                   VARCHAR (30)   NULL,
    [UPDATED_DATE]                 DATETIME2 (7)           NULL,
    [_transaction_date]            DATETIME2 (7)       NULL,
    [year]                         NVARCHAR (MAX) NULL,
    [month]                        NVARCHAR (MAX) NULL,
    [day]                          NVARCHAR (MAX) NULL,
    [_DLRawZoneTimeStamp]          DATETIME2 (7)  NULL,
    [_DLTrustedZoneTimeStamp]      DATETIME2 (7)  NULL,
    [_RecordStart]                 DATETIME2 (7)  NULL,
    [_RecordEnd]                   DATETIME2 (7)  NULL,
    [_RecordDeleted]               INT            NULL,
    [_RecordCurrent]               INT            NULL
);

