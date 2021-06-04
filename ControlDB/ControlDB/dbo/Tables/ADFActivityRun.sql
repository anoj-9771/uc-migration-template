CREATE TABLE [dbo].[ADFActivityRun] (
    [TimeGenerated]     DATETIME       NULL,
    [Status]            NVARCHAR (MAX) NULL,
    [Start]             DATETIME       NULL,
    [ActivityName]      NVARCHAR (MAX) NULL,
    [ActivityRunId]     NVARCHAR (MAX) NULL,
    [PipelineRunId]     NVARCHAR (MAX) NULL,
    [ActivityType]      NVARCHAR (MAX) NULL,
    [LinkedServiceName] NVARCHAR (MAX) NULL,
    [End]               DATETIME       NULL,
    [FailureType]       NVARCHAR (MAX) NULL,
    [PipelineName]      NVARCHAR (MAX) NULL,
    [Input]             NVARCHAR (MAX) NULL,
    [Output]            NVARCHAR (MAX) NULL,
    [ErrorMessage]      NVARCHAR (MAX) NULL,
    [Error]             NVARCHAR (MAX) NULL,
    [Type]              NVARCHAR (MAX) NULL,
    [Category]          VARCHAR (250)  NULL
);

