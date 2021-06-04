CREATE TABLE [dbo].[ADFPipelineRun] (
    [TimeGenerated] DATETIME       NULL,
    [Status]        NVARCHAR (MAX) NULL,
    [Start]         DATETIME       NULL,
    [PipelineName]  NVARCHAR (MAX) NULL,
    [RunId]         NVARCHAR (MAX) NULL,
    [Predecessors]  NVARCHAR (MAX) NULL,
    [Parameters]    NVARCHAR (MAX) NULL,
    [End]           DATETIME       NULL,
    [Tags]          NVARCHAR (MAX) NULL,
    [Type]          NVARCHAR (MAX) NULL
);

