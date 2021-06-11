CREATE TABLE [CTL].[TaskExecutionLog] (
    [ExecutionLogId]   BIGINT         IDENTITY (1, 1) NOT NULL,
    [BatchExecutionId] BIGINT         NOT NULL,
    [ControlTaskId]    BIGINT         NOT NULL,
    [StartTime]        DATETIME       NOT NULL,
    [EndTime]          DATETIME       NULL,
    [ExecutionStatus]  VARCHAR (50)   NULL,
    [ErrorMessage]     VARCHAR (2000) NULL,
    [TaskOutput]       VARCHAR (2000) NULL,
    CONSTRAINT [FK_TaskExecutionLog_BatchExecutionLog] FOREIGN KEY ([BatchExecutionId]) REFERENCES [CTL].[BatchExecutionLog] ([BatchExecutionLogId]),
    CONSTRAINT [FK_TaskExecutionLog_ControlTasks] FOREIGN KEY ([ControlTaskId]) REFERENCES [CTL].[ControlTasks] ([TaskId]), 
    CONSTRAINT [PK_TaskExecutionLog] PRIMARY KEY ([ExecutionLogId])
);

