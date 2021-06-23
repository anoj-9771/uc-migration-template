CREATE TABLE [edw].[Prada_dbo_SKILLS_TEAM_MAPPING] (
    [COST_CENTRE]             VARCHAR (30)  NULL,
    [Cost_Centre_Name]        VARCHAR (100) NULL,
    [Cost_Centre_with_Name]   VARCHAR (100) NULL,
    [Region_-_iPlan]          VARCHAR (100) NULL,
    [Head_Teacher]            VARCHAR (100) NULL,
    [Teaching_Section]        VARCHAR (100) NULL,
    [Activity_Level_3]        VARCHAR (100) NULL,
    [Skills_Team]             VARCHAR (150) NULL,
    [load_date]               DATE          NULL,
    [_DLRawZoneTimeStamp]     DATETIME      NULL,
    [_DLTrustedZoneTimeStamp] DATETIME      NULL,
    [_RecordStart]            DATETIME      NULL,
    [_RecordEnd]              DATETIME      NULL,
    [_RecordCurrent]          INT           NULL,
    [_RecordDeleted]          INT           NULL
);




GO
CREATE CLUSTERED COLUMNSTORE INDEX [CCI_edw_Prada_dbo_skills_team_mapping] ON [edw].[Prada_dbo_SKILLS_TEAM_MAPPING]
WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) 
;

