CREATE TABLE [CTL].[ControlManifest] (
	[ManifestID] [bigint] IDENTITY(1, 1) NOT NULL,
    [BatchExecutionLogID] [bigint] NOT NULL,
	[TaskExecutionLogID] [bigint] NOT NULL,
	[SourceObject] [varchar](1000) NULL,
	[Container] [varchar](1000) NULL,
	[StartCounter] [varchar](500) NULL,
	[EndCounter] [varchar](500) NULL,
	[RecordCountLoaded] [bigint] NULL,
	[RecordCountDeltaTable] [bigint] NULL,
	[RecordCountTargetTable] [bigint] NULL,
	[FolderName] [varchar](1000) NULL,
	[FileName] [varchar](1000) NULL,
	[ProcessedToCleansedZone] [bit] NULL,
	[DeltaColumn] [nvarchar](100) NULL,
	[ProcessedToSQLEDW] [bit] NULL,
	[RawZonePipelineRunID] [varchar](50) NULL,
	[CleansedZonePipelineRunID] [varchar](50) NULL,
	[SQLEDWPipelineRunID] [varchar](50) NULL,
	[SourceFileDateStamp] [char](14) NULL,
	[SourceFileName] [varchar](1000) NULL,
	[M_DeltaRecordCount] [bigint] NULL,
	[M_TotalNoRows] [bigint] NULL,
    [M_Message] VARCHAR(255) NULL, 
    CONSTRAINT [PK_CTL_ControlManifest] PRIMARY KEY CLUSTERED ([ManifestID] ASC),
	CONSTRAINT [UQ_ControlManifest_SrcFileDateStamp] UNIQUE NONCLUSTERED 
	(
		[BatchExecutionLogID] ASC,
		[TaskExecutionLogID] ASC,
		[SourceFileDateStamp] ASC
	)
);

















