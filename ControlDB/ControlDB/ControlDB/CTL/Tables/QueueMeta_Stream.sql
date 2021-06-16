CREATE TABLE [CTL].[QueueMeta_Stream] (
    [QueueID] INT    IDENTITY (1, 1) NOT NULL,
    [TaskId]  BIGINT NOT NULL,
    [Status]  INT    NOT NULL,
    [BatchId] BIGINT NULL,
    CONSTRAINT [PK_Queue_Stream] PRIMARY KEY CLUSTERED ([QueueID] ASC),
    CONSTRAINT [FK_QueueMeta_Stream_Task] FOREIGN KEY ([TaskId]) REFERENCES [CTL].[Task] ([TaskId])
);

