CREATE TABLE [CTL].[DataStore] (
    [DataStoreId] BIGINT         IDENTITY (1, 1) NOT NULL,
    [Name]        VARCHAR (250)  NULL,
    [Description] VARCHAR (1000) NULL,
    [TypeId]      INT            NOT NULL,
    [Connection]  VARCHAR (250)  NULL,
    [Limit]       INT            NULL,
    CONSTRAINT [PK_DataStore] PRIMARY KEY CLUSTERED ([DataStoreId] ASC),
    CONSTRAINT [FK_DataStore_Type] FOREIGN KEY ([TypeId]) REFERENCES [CTL].[Type] ([TypeId])
);


GO
ALTER TABLE [CTL].[DataStore] NOCHECK CONSTRAINT [FK_DataStore_Type];

