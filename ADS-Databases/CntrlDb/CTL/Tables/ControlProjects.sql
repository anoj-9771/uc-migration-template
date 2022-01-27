CREATE TABLE [CTL].[ControlProjects] (
    [ProjectId]   BIGINT        IDENTITY (1, 1) NOT NULL,
    [ProjectName] VARCHAR (255) NOT NULL,
    [Enabled]     BIT           NULL,
    [RunSequence] BIGINT        NOT NULL,
    CONSTRAINT [PK_ControlProjects] PRIMARY KEY CLUSTERED ([ProjectId] ASC)
);





