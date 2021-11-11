CREATE TABLE [CTL].[TechRecRawToCleansed]
(
	[TechRecID] [bigint] IDENTITY(1, 1) NOT NULL,
	[BatchExecutionId] [bigint] NOT NULL,
	[TaskExecutionLogId] [bigint] NOT NULL,
	[TaskId] [bigint] NOT NULL,
	[SourceObject] [varchar](255) NOT NULL,
	[SourceFileDateStamp] [char](14) NOT NULL,
	[TargetName] [varchar](255) NOT NULL,
	[ManifestId] [bigint] NOT NULL,
	[ManifestTotalNoRecords] [bigint] NULL,
	[TargetTableRowCount] [bigint] NULL, 
    CONSTRAINT [PK_TechRecRawToCleansed] PRIMARY KEY ([TechRecID])
)
