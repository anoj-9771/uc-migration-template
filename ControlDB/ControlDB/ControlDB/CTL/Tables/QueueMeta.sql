CREATE TABLE [CTL].[QueueMeta] (
    [QueueID]         INT    IDENTITY (1, 1) NOT NULL,
    [TaskId]          BIGINT NOT NULL,
    [ProjectPriority] INT    NOT NULL,
    [ProcessPriority] INT    NOT NULL,
    [Status]          INT    NOT NULL,
    [BatchId]         BIGINT NULL,
    CONSTRAINT [PK_Queue] PRIMARY KEY CLUSTERED ([QueueID] ASC),
    CONSTRAINT [FK_QueueMeta_Task] FOREIGN KEY ([TaskId]) REFERENCES [CTL].[Task] ([TaskId])
);


GO
CREATE NONCLUSTERED INDEX [nci_wi_QueueMeta_D4C981550A2FE734BE2478C85A955F9F]
    ON [CTL].[QueueMeta]([TaskId] ASC, [BatchId] ASC, [Status] ASC);

