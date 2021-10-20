CREATE TABLE [dbo].[Test1] (
    [EXTRACT_DATETIME]    NVARCHAR (255) NULL,
    [EXTRACT_RUN_ID]      BIGINT         NULL,
    [KEY1]                NVARCHAR (256) NULL,
    [LANGU]               NVARCHAR (256) NULL,
    [ODQ_CHANGEMODE]      NVARCHAR (256) NULL,
    [ODQ_ENTITYCNTR]      BIGINT         NULL,
    [TXTLG]               NVARCHAR (256) NULL,
    [_DLRawZoneTimeStamp] DATETIME2 (7)  NULL,
    [year]                NVARCHAR (256) NULL,
    [month]               NVARCHAR (256) NULL,
    [day]                 NVARCHAR (256) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

