CREATE TABLE [tsEChecklist].[ChecklistTemplates] (
    [CheckListId]             INT            NULL,
    [Version]                 FLOAT (53)     NULL,
    [Sponsor]                 NVARCHAR (MAX) NULL,
    [CheckListAcronym]        NVARCHAR (MAX) NULL,
    [CheckListName]           NVARCHAR (MAX) NULL,
    [CheckListSponsors]       NVARCHAR (MAX) NULL,
    [WULPublicName]           NVARCHAR (MAX) NULL,
    [ImplementationDate]      DATETIME       NULL,
    [CC_LastUpdated]          DATETIME       NULL,
    [ReviewDate]              DATETIME       NULL,
    [_RecordStart]            DATETIME       NULL,
    [_RecordEnd]              DATETIME       NULL,
    [_RecordDeleted]          INT            NULL,
    [_RecordCurrent]          INT            NULL,
    [_DLCuratedZoneTimeStamp] DATETIME       NULL
);



