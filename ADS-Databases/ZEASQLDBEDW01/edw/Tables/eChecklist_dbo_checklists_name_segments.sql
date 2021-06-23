CREATE TABLE [edw].[eChecklist_dbo_checklists_name_segments] (
    [CheckListId]                      INT           NULL,
    [SegmentNo]                        INT           NULL,
    [Type]                             VARCHAR (100) NULL,
    [ControlId]                        VARCHAR (100) NULL,
    [DDLOptionsDomain]                 VARCHAR (100) NULL,
    [Config]                           VARCHAR (MAX) NULL,
    [IncludeInCustomReport]            BIT           NULL,
    [IncludeInActionedReport]          BIT           NULL,
    [IncludeInInFlightReport]          BIT           NULL,
    [IncludeInInitiatedReport]         BIT           NULL,
    [IncludeInNeglectedReport]         BIT           NULL,
    [IncludeInRejectedReport]          BIT           NULL,
    [IncludeInWorkforcePlanningReport] BIT           NULL,
    [_DLRawZoneTimeStamp]              DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp]          DATETIME2 (7) NULL,
    [_RecordStart]                     DATETIME2 (7) NULL,
    [_RecordEnd]                       DATETIME2 (7) NULL,
    [_RecordCurrent]                   INT           NULL,
    [_RecordDeleted]                   INT           NULL
);



