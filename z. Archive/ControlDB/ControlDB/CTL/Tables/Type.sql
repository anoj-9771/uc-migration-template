CREATE TABLE [CTL].[Type] (
    [TypeId]      INT            IDENTITY (1, 1) NOT NULL,
    [Name]        VARCHAR (250)  NULL,
    [Description] VARCHAR (1000) NULL,
    [Format]      VARCHAR (50)   NULL,
    CONSTRAINT [PK_Type] PRIMARY KEY CLUSTERED ([TypeId] ASC)
);

