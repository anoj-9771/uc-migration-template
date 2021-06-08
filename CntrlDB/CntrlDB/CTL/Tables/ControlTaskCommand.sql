CREATE TABLE [CTL].[ControlTaskCommand] (
    [CommandId]     BIGINT        IDENTITY (1, 1) NOT NULL,
    [ControlTaskId] BIGINT        NOT NULL,
    [CommandTypeId] BIGINT        NOT NULL,
    [Command]       VARCHAR (MAX) NULL,
    CONSTRAINT [FK_ControlTaskCommand_ControlTasks] FOREIGN KEY ([ControlTaskId]) REFERENCES [CTL].[ControlTasks] ([TaskId]), 
    CONSTRAINT [PK_ControlTaskCommand] PRIMARY KEY ([CommandId])
);

