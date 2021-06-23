CREATE TABLE [edw].[OneEBS_EBS_0165_MARKING_RULE_ASSESSMENTS] (
    [INSTITUTION_CONTEXT_ID]   NUMERIC (10)   NULL,
    [GRADING_SCHEME_GRADES_ID] NUMERIC (10)   NULL,
    [ID]                       NUMERIC (10)   NULL,
    [MARKING_RULE_ID]          NUMERIC (10)   NULL,
    [ASSESSMENT_ID]            NUMERIC (10)   NULL,
    [CREATED_BY]               VARCHAR (30)   NULL,
    [CREATED_DATE]             DATETIME2 (7)           NULL,
    [UPDATED_BY]               VARCHAR (30)   NULL,
    [UPDATED_DATE]             DATETIME2 (7)           NULL,
    [WEIGHTING]                NUMERIC (6, 3) NULL,
    [_transaction_date]        DATETIME2 (7)       NULL,
    [year]                     NVARCHAR (MAX) NULL,
    [month]                    NVARCHAR (MAX) NULL,
    [day]                      NVARCHAR (MAX) NULL,
    [_DLRawZoneTimeStamp]      DATETIME2 (7)  NULL,
    [_DLTrustedZoneTimeStamp]  DATETIME2 (7)  NULL,
    [_RecordStart]             DATETIME2 (7)  NULL,
    [_RecordEnd]               DATETIME2 (7)  NULL,
    [_RecordDeleted]           INT            NULL,
    [_RecordCurrent]           INT            NULL
);

