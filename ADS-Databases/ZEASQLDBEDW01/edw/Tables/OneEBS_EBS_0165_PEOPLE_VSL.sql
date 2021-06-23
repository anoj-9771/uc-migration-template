CREATE TABLE [edw].[OneEBS_EBS_0165_PEOPLE_VSL] (
    [CREATED_BY]                     VARCHAR (30)   NULL,
    [CREATED_DATE]                   DATETIME2 (7)  NULL,
    [HAS_HS_OR_CERTIV]               VARCHAR (1)    NULL,
    [HAS_HS_OR_CERTIV_BEEN_VERIFIED] VARCHAR (1)    NULL,
    [HAS_LLN_BEEN_VERIFIED]          VARCHAR (1)    NULL,
    [HS_OR_CERTIV_VERIFIED_DATE]     DATETIME2 (7)  NULL,
    [ID]                             NUMERIC (10)   NULL,
    [LLN_EXAM_PASSED_DATE]           DATETIME2 (7)  NULL,
    [PERSON_CODE]                    NUMERIC (10)   NULL,
    [UPDATED_BY]                     VARCHAR (30)   NULL,
    [UPDATED_DATE]                   DATETIME2 (7)  NULL,
    [_transaction_date]              DATETIME2 (7)  NULL,
    [_DLRawZoneTimeStamp]            DATETIME2 (7)  NULL,
    [year]                           NVARCHAR (MAX) NULL,
    [month]                          NVARCHAR (MAX) NULL,
    [day]                            NVARCHAR (MAX) NULL,
    [_DLTrustedZoneTimeStamp]        DATETIME2 (7)  NULL,
    [_RecordStart]                   DATETIME2 (7)  NULL,
    [_RecordEnd]                     DATETIME2 (7)  NULL,
    [_RecordDeleted]                 INT            NULL,
    [_RecordCurrent]                 INT            NULL
);

