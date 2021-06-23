CREATE TABLE [edw].[Prada_dbo_STS_DATA] (
    [ID_CPU]                     NUMERIC (18)  NULL,
    [INST]                       NUMERIC (18)  NULL,
    [COMMITMENT_ID]              VARCHAR (12)  NULL,
    [PRODUCT_CODE]               VARCHAR (18)  NULL,
    [ENROLMENT_STATUS]           VARCHAR (16)  NULL,
    [STREAM_GROUP]               VARCHAR (53)  NULL,
    [STREAM]                     VARCHAR (50)  NULL,
    [COURSE10]                   VARCHAR (10)  NULL,
    [COURSE_PU_ID]               NUMERIC (18)  NULL,
    [STS_REPORT_DATE]            DATE          NULL,
    [MAX_STAGE_LOAD]             NUMERIC (18)  NULL,
    [SUM_STAGE_LOAD]             SMALLMONEY    NULL,
    [DATE_STAGE_LOAD]            DATE          NULL,
    [MAX_LOCATION_LOAD]          NUMERIC (18)  NULL,
    [SUM_LOCATION_LOAD]          SMALLMONEY    NULL,
    [DATE_LOCATION_LOAD]         DATE          NULL,
    [MAX_NEEDS_LOAD]             NUMERIC (18)  NULL,
    [SUM_NEED_LOAD]              SMALLMONEY    NULL,
    [DATE_NEED_LOAD]             DATE          NULL,
    [REGION]                     VARCHAR (100) NULL,
    [PROPOSED_COMMENCEMENT_DATE] DATE          NULL,
    [ESTIMATED_END_DATE]         DATE          NULL,
    [_DLRawZoneTimeStamp]        DATETIME      NULL,
    [_DLTrustedZoneTimeStamp]    DATETIME      NULL,
    [_RecordStart]               DATETIME      NULL,
    [_RecordEnd]                 DATETIME      NULL,
    [_RecordCurrent]             INT           NULL,
    [_RecordDeleted]             INT           NULL
);




GO
CREATE CLUSTERED COLUMNSTORE INDEX [CCI_edw_Prada_dbo_STS_DATA] ON [edw].[Prada_dbo_STS_DATA]
WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) 
;

