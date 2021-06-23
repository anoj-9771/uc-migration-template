CREATE TABLE [CTL].[ControlTasks] (
    [TaskId]                       BIGINT        IDENTITY (1, 1) NOT NULL,
    [TaskName]                     VARCHAR (255) NOT NULL,
    [SourceId]                     BIGINT        NOT NULL,
    [TargetId]                     BIGINT        NOT NULL,
    [TruncateTarget]               BIT           NOT NULL,
    [TaskEnabled]                  BIT           NOT NULL,
    [LoadLatestOnly]               BIT           NOT NULL,
    [ExecuteSourceSQLasStoredProc] BIT           NOT NULL,
    [ControlStageId]               BIGINT        NOT NULL,
    [ProjectId]                    BIGINT        NULL,
    [ObjectGrain]                  VARCHAR (30)  NULL,
    [DataLoadMode]                 VARCHAR (100) NULL,
    [TrackChanges]                 BIT           NULL,
    [LoadToSqlEDW]                 BIT           NULL,
    [UpdateMetaData]               BIT           NULL,
    CONSTRAINT [PK_ControlTasks] PRIMARY KEY CLUSTERED ([TaskId] ASC),
    CONSTRAINT [FK_ControlTasks_ControlSource] FOREIGN KEY ([SourceId]) REFERENCES [CTL].[ControlSource] ([SourceId]),
    CONSTRAINT [FK_ControlTasks_ControlStages] FOREIGN KEY ([ControlStageId]) REFERENCES [CTL].[ControlStages] ([ControlStageId]),
    CONSTRAINT [FK_ControlTasks_ControlTarget] FOREIGN KEY ([TargetId]) REFERENCES [CTL].[ControlTarget] ([TargetId]),
    CONSTRAINT [UQ_ControlTasks_TaskName] UNIQUE NONCLUSTERED ([TaskName] ASC, [ControlStageId] ASC, [ProjectId] ASC)
);















