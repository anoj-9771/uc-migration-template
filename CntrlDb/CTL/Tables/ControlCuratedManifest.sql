CREATE TABLE [CTL].[ControlCuratedManifest] (
    [BatchExecutionLogID] BIGINT        NOT NULL,
    [TaskExecutionLogID]  BIGINT        NOT NULL,
    [SubjectArea]         VARCHAR (255) NOT NULL,
    [Project]             VARCHAR (255) NOT NULL,
    [StartPeriod]         DATETIME      NULL,
    [EndPeriod]           DATETIME      NULL,
    [LoadStatus]          VARCHAR (100) NULL,
    [StartTimestamp]      DATETIME      NULL,
    [EndTimeStamp]        DATETIME      NULL, 
    CONSTRAINT [PK_ControlCuratedManifest] PRIMARY KEY ([BatchExecutionLogID], [TaskExecutionLogID])
);

