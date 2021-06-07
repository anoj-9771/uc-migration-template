CREATE TABLE [CTL].[ControlSourceColumns] (
    [ControlSourceColumnId] BIGINT         IDENTITY (1, 1) NOT NULL,
    [SourceTableId]         BIGINT         NOT NULL,
    [ColumnName]            VARCHAR (255)  NOT NULL,
    [DataType]              VARCHAR (50)   NOT NULL,
    [MaxLength]             INT            NOT NULL,
    [Precision]             INT            NOT NULL,
    [ColumnQuery]           VARCHAR (1000) NOT NULL,
    [SourceProcessor]       VARCHAR (255)  NULL, 
    CONSTRAINT [PK_ControlSourceColumns] PRIMARY KEY ([ControlSourceColumnId])
);

