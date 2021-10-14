CREATE TABLE [dbo].[estih] (
    [AEDAT]               VARCHAR (100) NULL,
    [AENAM]               VARCHAR (100) NULL,
    [AUTOEND]             VARCHAR (100) NULL,
    [ERDAT]               VARCHAR (100) NULL,
    [ERNAM]               VARCHAR (100) NULL,
    [EXTRACT_DATETIME]    VARCHAR (100) NULL,
    [EXTRACT_RUN_ID]      BIGINT        NULL,
    [INDEXNR]             BIGINT        NULL,
    [MANDT]               VARCHAR (100) NULL,
    [PRUEFGR]             VARCHAR (100) NULL,
    [ZWZUART]             BIGINT        NULL,
    [_DLRawZoneTimeStamp] DATETIME      NULL,
    [year]                VARCHAR (12)  NULL,
    [month]               VARCHAR (12)  NULL,
    [day]                 VARCHAR (12)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

