CREATE TABLE [CTL].[Process] (
    [ProcessId]         BIGINT         IDENTITY (1, 1) NOT NULL,
    [Name]              VARCHAR (250)  NULL,
    [Description]       VARCHAR (1000) NULL,
    [ProjectId]         BIGINT         NOT NULL,
    [Enabled]           BIT            NOT NULL,
    [Priority]          INT            NOT NULL,
    [Grain]             VARCHAR (50)   NOT NULL,
    [TypeId]            INT            NOT NULL,
    [ProcessDependency] VARCHAR (500)  NULL,
    [ProjectDependency] VARCHAR (500)  NULL,
    CONSTRAINT [PK_Process] PRIMARY KEY CLUSTERED ([ProcessId] ASC),
    CONSTRAINT [FK_Process_Project] FOREIGN KEY ([ProjectId]) REFERENCES [CTL].[Project] ([ProjectId]),
    CONSTRAINT [FK_Process_Type] FOREIGN KEY ([TypeId]) REFERENCES [CTL].[Type] ([TypeId])
);


GO
ALTER TABLE [CTL].[Process] NOCHECK CONSTRAINT [FK_Process_Type];


GO
CREATE NONCLUSTERED INDEX [nci_wi_Process_86C7901ACCA1036B4245897892B6BB74]
    ON [CTL].[Process]([ProjectId] ASC, [Enabled] ASC)
    INCLUDE([Grain], [Name], [Priority], [TypeId]);


GO
CREATE NONCLUSTERED INDEX [nci_wi_Process_F0EA8400F136BC701266115E4BF39EBE]
    ON [CTL].[Process]([Name] ASC, [ProjectId] ASC);

