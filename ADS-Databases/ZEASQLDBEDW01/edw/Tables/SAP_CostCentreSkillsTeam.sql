CREATE TABLE [edw].[SAP_CostCentreSkillsTeam] (
    [CostCentreCode]          INT            NULL,
    [CostCentreName]          NVARCHAR (MAX) NULL,
    [CostCentreDescription]   NVARCHAR (MAX) NULL,
    [RegioniPlan]             NVARCHAR (MAX) NULL,
    [HeadTeachers]            NVARCHAR (MAX) NULL,
    [TeachingSection]         NVARCHAR (MAX) NULL,
    [ActivityLevel3]          NVARCHAR (MAX) NULL,
    [SkillsTeam]              NVARCHAR (MAX) NULL,
    [CreatedDate]             NVARCHAR (MAX) NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7)  NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7)  NULL,
    [_RecordStart]            DATETIME2 (7)  NULL,
    [_RecordEnd]              DATETIME2 (7)  NULL,
    [_RecordCurrent]          INT            NULL,
    [_RecordDeleted]          INT            NULL
);




GO
CREATE CLUSTERED COLUMNSTORE INDEX [CCI_edw_SAP_CostCentreSkillsTeam] ON [edw].[SAP_CostCentreSkillsTeam]
WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) 
;

