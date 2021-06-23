CREATE TABLE [edw].[Prada_dbo_UNIT_INSTANCE_OCCURRENCES] (
    [ID_UIO]                     NUMERIC (18)  NULL,
    [ID_UI]                      VARCHAR (40)  NULL,
    [INSTITUTE]                  VARCHAR (4)   NULL,
    [CALOCC_CALENDAR_TYPE_CODE]  VARCHAR (15)  NULL,
    [CALOCC_OCCURRENCE_CODE]     VARCHAR (15)  NULL,
    [FES_START_DATE]             DATE          NULL,
    [FES_END_DATE]               DATE          NULL,
    [FES_UINS_INSTANCE_CODE]     VARCHAR (25)  NULL,
    [OFFERING_CODE]              VARCHAR (75)  NULL,
    [UIO_ID]                     NUMERIC (18)  NULL,
    [FES_ACTIVE]                 VARCHAR (3)   NULL,
    [OWNING_ORGANISATION]        VARCHAR (10)  NULL,
    [OFFERING_ORGANISATION]      VARCHAR (10)  NULL,
    [SLOC_LOCATION_CODE]         VARCHAR (10)  NULL,
    [FES_MOA_CODE]               VARCHAR (10)  NULL,
    [STATUS]                     VARCHAR (12)  NULL,
    [UIO_TYPE]                   VARCHAR (12)  NULL,
    [NZ_FUNDING]                 VARCHAR (10)  NULL,
    [FES_USER_8]                 VARCHAR (20)  NULL,
    [FES_USER_26]                VARCHAR (20)  NULL,
    [VERSION_NUMBER]             NUMERIC (18)  NULL,
    [MAXIMUM_HOURS]              NUMERIC (18)  NULL,
    [OFFERING_TYPE]              VARCHAR (15)  NULL,
    [VFH_DELIVERY_MODE]          VARCHAR (2)   NULL,
    [PROSP_USER_19]              VARCHAR (2)   NULL,
    [OFFERING_STATUS]            VARCHAR (15)  NULL,
    [DELIVERY_LOCATION]          VARCHAR (75)  NULL,
    [AWARD_CODE]                 VARCHAR (25)  NULL,
    [AWARD_RULESET_ID]           NUMERIC (18)  NULL,
    [UNIT_OF_STUDY_CODE]         VARCHAR (15)  NULL,
    [COURSE_EFTSL_FACTOR]        NUMERIC (18)  NULL,
    [WAIVER_CODE]                VARCHAR (10)  NULL,
    [IS_GOVT_SUBSID_COURSE]      VARCHAR (5)   NULL,
    [OFFERING_ORGANISATION_DESC] VARCHAR (75)  NULL,
    [OWNING_ORGANISATION_DESC]   VARCHAR (75)  NULL,
    [EXTRACT_DATE]               NUMERIC (18)  NULL,
    [_DLRawZoneTimeStamp]        DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp]    DATETIME2 (7) NULL
);




GO
CREATE CLUSTERED COLUMNSTORE INDEX [CCI_edw_Prada_dbo_UNIT_INSTANCE_OCCURRENCES] ON [edw].[Prada_dbo_UNIT_INSTANCE_OCCURRENCES]
WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) 
;

