CREATE TABLE [edw].[eChecklist_dbo_checklists_actions] (
    [InstanceId]              INT                                                NULL,
    [TaskNo]                  INT                                                NULL,
    [Actioner]                VARCHAR (50)                                       NULL,
    [TaskStatusId]            SMALLINT                                           NULL,
    [ActionedDate]            SMALLDATETIME                                      NULL,
    [Comment]                 VARCHAR (MAX)                                      NULL,
    [ControlValues]           VARCHAR (MAX)                                      NULL,
    [CreationDate]            SMALLDATETIME                                      NULL,
    [ReminderEmailData]       VARCHAR (MAX) MASKED WITH (FUNCTION = 'default()') NULL,
    [OtherTaskSpecificData]   VARCHAR (MAX)                                      NULL,
    [SendActionerEmail]       BIT                                                NULL,
    [ShelvedTill]             DATETIME                                           NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7)                                      NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7)                                      NULL,
    [_RecordStart]            DATETIME2 (7)                                      NULL,
    [_RecordEnd]              DATETIME2 (7)                                      NULL,
    [_RecordCurrent]          INT                                                NULL,
    [_RecordDeleted]          INT                                                NULL,
    [SnoozedTill]             SMALLDATETIME                                      NULL,
    [SavedNextActioner]       VARCHAR (50)  NULL,
    [SavedNextTaskNo]         INT           NULL);





