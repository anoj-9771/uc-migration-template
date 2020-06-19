CREATE TABLE [CTL].[TaskLog] (
    [TaskLogId]      BIGINT        IDENTITY (1, 1) NOT NULL,
    [BatchId]        BIGINT        NOT NULL,
    [Status]         VARCHAR (25)  NOT NULL,
    [TaskId]         BIGINT        NOT NULL,
    [FullBlobName]   VARCHAR (500) NOT NULL,
    [Output]         VARCHAR (MAX) NOT NULL,
    [RunID]          VARCHAR (100) NULL,
    [InitialLogTime] DATETIME      NULL,
    [EndLogTime]     DATETIME      NULL,
    CONSTRAINT [FK_TaskLog_Task] FOREIGN KEY ([TaskId]) REFERENCES [CTL].[Task] ([TaskId])
);


GO
CREATE NONCLUSTERED INDEX [nci_wi_TaskLog_53564EF6A2A1704967E6559CE098B3E2]
    ON [CTL].[TaskLog]([TaskId] ASC, [BatchId] ASC, [Status] ASC)
    INCLUDE([EndLogTime], [InitialLogTime]);


GO
CREATE NONCLUSTERED INDEX [nci_wi_TaskLog_A4BF5E303936EF6F21ADF4A8F8F083D3]
    ON [CTL].[TaskLog]([BatchId] ASC, [Status] ASC)
    INCLUDE([EndLogTime], [InitialLogTime], [TaskId]);

