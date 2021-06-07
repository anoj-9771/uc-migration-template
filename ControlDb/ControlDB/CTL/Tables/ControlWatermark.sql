CREATE TABLE [CTL].[ControlWatermark] (
    [WatermarkId]     BIGINT         IDENTITY (1, 1) NOT NULL,
    [ControlSourceId] BIGINT         NOT NULL,
    [SourceColumn]    VARCHAR (255)  NULL,
    [SourceSQL]       VARCHAR (2000) NOT NULL,
    [Watermarks]      VARCHAR (MAX)  NULL,
    [SourceName]      NVARCHAR (255) NULL,
    CONSTRAINT [PK_ControlWatermark] PRIMARY KEY CLUSTERED ([WatermarkId] ASC),
    CONSTRAINT [FK_ControlWatermark_ControlSource] FOREIGN KEY ([ControlSourceId]) REFERENCES [CTL].[ControlSource] ([SourceId])
);



