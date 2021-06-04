CREATE TABLE [CTL].[DataSet] (
    [DataSetId]   BIGINT         IDENTITY (1, 1) NOT NULL,
    [Name]        VARCHAR (250)  NOT NULL,
    [Description] VARCHAR (1000) NULL,
    [DataStoreId] BIGINT         NOT NULL,
    [Location]    VARCHAR (1000) NULL,
    [TypeId]      INT            NULL,
    CONSTRAINT [PK_DataSet] PRIMARY KEY CLUSTERED ([DataSetId] ASC),
    CONSTRAINT [FK_DataSet_DataStore] FOREIGN KEY ([DataStoreId]) REFERENCES [CTL].[DataStore] ([DataStoreId]),
    CONSTRAINT [FK_DataSet_Type] FOREIGN KEY ([TypeId]) REFERENCES [CTL].[Type] ([TypeId])
);


GO
ALTER TABLE [CTL].[DataSet] NOCHECK CONSTRAINT [FK_DataSet_Type];


GO
CREATE NONCLUSTERED INDEX [nci_wi_DataSet_68F5835770217368BCB3A640800308AE]
    ON [CTL].[DataSet]([DataStoreId] ASC, [Name] ASC);

