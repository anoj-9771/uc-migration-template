CREATE TABLE [dbo].[sapisu_ehauisu] (
    [/SAPCE/IURU_IED]     VARCHAR (100) NULL,
    [/SAPCE/IURU_IEDO]    VARCHAR (100) NULL,
    [COUNC]               VARCHAR (100) NULL,
    [CRM_GUID]            VARCHAR (100) NULL,
    [EXTRACT_DATETIME]    VARCHAR (100) NULL,
    [EXTRACT_RUN_ID]      BIGINT        NULL,
    [HAUS]                VARCHAR (100) NULL,
    [MANDT]               VARCHAR (100) NULL,
    [REGIOGROUP]          VARCHAR (100) NULL,
    [REGIOGROUP_PERM]     VARCHAR (100) NULL,
    [REGPOLIT]            VARCHAR (100) NULL,
    [_DLRawZoneTimeStamp] DATETIME      NULL,
    [year]                VARCHAR (4)   NULL,
    [month]               VARCHAR (2)   NULL,
    [day]                 VARCHAR (2)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

