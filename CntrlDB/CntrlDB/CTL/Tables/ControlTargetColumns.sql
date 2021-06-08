CREATE TABLE [CTL].[ControlTargetColumns] (
    [ControlTargetColumnId] BIGINT        IDENTITY (1, 1) NOT NULL,
    [TargetId]              BIGINT        NOT NULL,
    [ColumnName]            VARCHAR (255) NOT NULL,
    [DataType]              VARCHAR (50)  NOT NULL,
    [MaxLength]             INT           NOT NULL,
    [Precision]             INT           NOT NULL,
    CONSTRAINT [FK_ControlTargetColumns_ControlTarget] FOREIGN KEY ([TargetId]) REFERENCES [CTL].[ControlTarget] ([TargetId]), 
    CONSTRAINT [PK_ControlTargetColumns] PRIMARY KEY ([ControlTargetColumnId])
);

