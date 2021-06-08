CREATE TABLE [CTL].[BatchExecutionLog] (
    [BatchExecutionLogId]  BIGINT         IDENTITY (1, 1) NOT NULL,
    [StartDate]            DATETIME       NOT NULL,
    [EndDate]              DATETIME       NULL,
    [BatchExecutionStatus] VARCHAR (50)   NOT NULL,
    [ErrorMessage]         VARCHAR (2000) NULL,
    [ProjectID]            BIGINT         NULL,
    CONSTRAINT [PK_BatchExecutionLog] PRIMARY KEY CLUSTERED ([BatchExecutionLogId] ASC)
);



