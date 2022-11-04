CREATE TABLE [dbo].[ExtractLoadStatus] (
    [ID]                  INT            IDENTITY (1, 1) NOT NULL,
    [BatchID]             VARCHAR (64)   NOT NULL,
    [SystemCode]          NVARCHAR (100) NOT NULL,
    [SourceID]            INT            NOT NULL,
    [LowWatermark]        NVARCHAR (100) NULL,
    [HighWatermark]       NVARCHAR (100) NULL,
    [SourceRowCount]      BIGINT         NULL,
    [SinkRowCount]        BIGINT         NULL,
    [RawPath]             NVARCHAR (200) NULL,
    [RawStatus]           NVARCHAR (50)  NULL,
    [RawStartDTS]         DATETIME       NULL,
    [RawEndDTS]           DATETIME       NULL,
    [CleansedStatus]      NVARCHAR (50)  NULL,
    [CleansedSourceCount] BIGINT         NULL,
    [CleansedSinkCount]   BIGINT         NULL,
    [CleansedStartDTS]    DATETIME       NULL,
    [CleansedEndDTS]      DATETIME       NULL,
    [CreatedDTS]          DATETIME       NOT NULL,
    [EndedDTS]            DATETIME       NULL
);
GO

ALTER TABLE [dbo].[ExtractLoadStatus]
    ADD CONSTRAINT [PK_ExtractLoadStatus] PRIMARY KEY CLUSTERED ([ID] ASC, [BatchID] ASC, [SystemCode] ASC, [SourceID] ASC);
GO

