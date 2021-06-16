CREATE TABLE [CTL].[Stage] (
    [StageId]     INT            NOT NULL,
    [Name]        VARCHAR (250)  NULL,
    [Description] VARCHAR (1000) NULL,
    CONSTRAINT [PK_Stage] PRIMARY KEY CLUSTERED ([StageId] ASC)
);

