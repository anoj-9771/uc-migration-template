CREATE TABLE [CTL].[Project] (
    [ProjectId]   BIGINT         IDENTITY (1, 1) NOT NULL,
    [Name]        VARCHAR (250)  NOT NULL,
    [Description] VARCHAR (1000) NULL,
    [Enabled]     BIT            NOT NULL,
    [Schedule]    VARCHAR (100)  NULL,
    [Priority]    INT            NOT NULL,
    [Streaming]   BIT            DEFAULT ((0)) NULL,
    CONSTRAINT [PK_Project] PRIMARY KEY CLUSTERED ([ProjectId] ASC)
);



