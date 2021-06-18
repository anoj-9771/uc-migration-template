CREATE TABLE [edw].[Prada_dbo_REFERENCE_DATA] (
    [DOMAIN]                  VARCHAR (50)  NULL,
    [VALUE]                   VARCHAR (50)  NULL,
    [DESCRIPTION]             VARCHAR (100) NULL,
    [ACTIVE]                  VARCHAR (1)   NULL,
    [_DLRawZoneTimeStamp]     DATETIME      NULL,
    [_DLTrustedZoneTimeStamp] DATETIME      NULL
);




GO
CREATE CLUSTERED COLUMNSTORE INDEX [CCI_edw_Prada_dbo_REFERENCE_DATA] ON [edw].[Prada_dbo_REFERENCE_DATA]
WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) 
;

