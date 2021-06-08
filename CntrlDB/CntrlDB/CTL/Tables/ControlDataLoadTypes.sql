CREATE TABLE [CTL].[ControlDataLoadTypes] (
    [DataLoadTypeID] INT           IDENTITY (1, 1) NOT NULL,
    [DataLoadType]   NVARCHAR (50) NOT NULL,
    [DeltaExtract]   BIT           NOT NULL,
    [CDCSource]      BIT           NOT NULL,
    [TruncateTarget] BIT           NOT NULL,
    [UpsertTarget]   BIT           NOT NULL,
    CONSTRAINT [PK_ControlDataLoadTypes] PRIMARY KEY CLUSTERED ([DataLoadTypeID] ASC)
);




