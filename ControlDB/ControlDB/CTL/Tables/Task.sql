CREATE TABLE [CTL].[Task] (
    [TaskId]     BIGINT        IDENTITY (1, 1) NOT NULL,
    [ProcessId]  BIGINT        NOT NULL,
    [SourceId]   BIGINT        NOT NULL,
    [TargetId]   BIGINT        NOT NULL,
    [StageId]    INT           NOT NULL,
    [WorkerId]   BIGINT        NOT NULL,
    [Query]      VARCHAR (MAX) NULL,
    [TaskTypeId] INT           NULL,
    [Rank]       INT           NOT NULL,
    CONSTRAINT [PK_Task] PRIMARY KEY CLUSTERED ([TaskId] ASC),
    CONSTRAINT [FK_Task_DataSet_Source] FOREIGN KEY ([SourceId]) REFERENCES [CTL].[DataSet] ([DataSetId]),
    CONSTRAINT [FK_Task_DataSet_Target] FOREIGN KEY ([TargetId]) REFERENCES [CTL].[DataSet] ([DataSetId]),
    CONSTRAINT [FK_Task_Process] FOREIGN KEY ([ProcessId]) REFERENCES [CTL].[Process] ([ProcessId]),
    CONSTRAINT [FK_Task_Stage] FOREIGN KEY ([StageId]) REFERENCES [CTL].[Stage] ([StageId]),
    CONSTRAINT [FK_Task_Type] FOREIGN KEY ([TaskTypeId]) REFERENCES [CTL].[Type] ([TypeId]),
    CONSTRAINT [FK_Task_Worker] FOREIGN KEY ([WorkerId]) REFERENCES [CTL].[Worker] ([WorkerId])
);


GO
ALTER TABLE [CTL].[Task] NOCHECK CONSTRAINT [FK_Task_Type];


GO
CREATE NONCLUSTERED INDEX [nci_wi_Task_5E9B72D72927497C75F403DA66F828B4]
    ON [CTL].[Task]([WorkerId] ASC, [SourceId] ASC)
    INCLUDE([ProcessId], [TargetId]);


GO
CREATE NONCLUSTERED INDEX [nci_wi_Task_662D72AFF1D3142A4DE9202681B768BE]
    ON [CTL].[Task]([ProcessId] ASC, [Rank] ASC)
    INCLUDE([Query], [SourceId], [StageId], [TargetId], [TaskTypeId], [WorkerId]);

