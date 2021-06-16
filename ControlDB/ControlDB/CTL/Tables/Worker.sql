CREATE TABLE [CTL].[Worker] (
    [WorkerId]    BIGINT         IDENTITY (1, 1) NOT NULL,
    [Name]        VARCHAR (250)  NULL,
    [Description] VARCHAR (1000) NULL,
    [Limit]       INT            NULL,
    [Details]     VARCHAR (500)  NULL,
    [TypeId]      INT            NOT NULL,
    CONSTRAINT [PK_Worker] PRIMARY KEY CLUSTERED ([WorkerId] ASC),
    CONSTRAINT [FK_Worker_Type] FOREIGN KEY ([TypeId]) REFERENCES [CTL].[Type] ([TypeId])
);


GO
ALTER TABLE [CTL].[Worker] NOCHECK CONSTRAINT [FK_Worker_Type];

