CREATE TABLE [CTL].[ControlManifest_New] (
	[ManifestID] [bigint] IDENTITY(1,1) NOT NULL, 
    [BatchExecutionLogID] [bigint] NOT NULL,
	[TaskExecutionLogID] [bigint] NOT NULL,
	[SourceObject] [varchar](1000) NULL,
	[Container] [varchar](1000) NULL,
	[SourceFileName] [varchar](1000) NOT NULL,
	[SourceFileDateStamp] [char](14) NULL,
	[StartCounter] [varchar](500) NULL,
	[EndCounter] [varchar](500) NULL,
	[ManifestValue] [bigint] NULL, 
	[RecordCountLoaded] [bigint] NULL,
	[RecordCountDeltaTable] [bigint] NULL,
	[FolderName] [varchar](1000) NULL,
	[FileName] [varchar](1000) NULL,
	[ProcessedToCleansedZone] [bit] NULL,
	[DeltaColumn] [nvarchar](100) NULL,
	[ProcessedToSQLEDW] [bit] NULL,
	[RawZonePipelineRunID] [varchar](50) NULL,
	[CleansedZonePipelineRunID] [varchar](50) NULL,
	[SQLEDWPipelineRunID] [varchar](50) NULL,
    CONSTRAINT [PK_CTL_ControlManifest_New] PRIMARY KEY CLUSTERED ([ManifestID] ASC)
);

















