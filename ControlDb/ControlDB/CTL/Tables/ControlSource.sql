CREATE TABLE [CTL].[ControlSource] (
    [SourceId]           BIGINT         IDENTITY (1, 1) NOT NULL,
    [SourceName]         VARCHAR (255)  NOT NULL,
    [SourceTypeId]       INT            NOT NULL,
    [SourceLocation]     VARCHAR (1000) NOT NULL,
    [LoadSource]         BIT            NOT NULL,
    [SourceServer]       VARCHAR (255)  NULL,
    [Processor]          VARCHAR (255)  NULL,
    [BusinessKeyColumn]  VARCHAR (100)  NULL,
    [AdditionalProperty] VARCHAR (MAX)  NULL,
    [IsAuditTable]       BIT            NULL,
    [SoftDeleteSource]   VARCHAR (255)  NULL,
    [UseAuditTable]      BIT            NULL,
    [ValidationColumn]   VARCHAR (100)  NULL,
    CONSTRAINT [PK_ControlSource] PRIMARY KEY CLUSTERED ([SourceId] ASC),
    CONSTRAINT [FK_ControlSource_ControlTypes] FOREIGN KEY ([SourceTypeId]) REFERENCES [CTL].[ControlTypes] ([TypeId])
);





















