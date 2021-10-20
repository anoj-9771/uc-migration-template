CREATE TABLE [dbo].[sapisu_0bp_cat_text] (
    [EXTRACT_DATETIME]    VARCHAR (100) NULL,
    [EXTRACT_RUN_ID]      BIGINT        NULL,
    [KEY1]                VARCHAR (100) NULL,
    [LANGU]               VARCHAR (100) NULL,
    [ODQ_CHANGEMODE]      VARCHAR (100) NULL,
    [ODQ_ENTITYCNTR]      BIGINT        NULL,
    [TXTLG]               VARCHAR (100) NULL,
    [_DLRawZoneTimeStamp] DATETIME      NULL,
    [year]                VARCHAR (4)   NULL,
    [month]               VARCHAR (2)   NULL,
    [day]                 VARCHAR (2)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

