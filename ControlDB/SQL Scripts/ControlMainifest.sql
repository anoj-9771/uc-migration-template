/****** Object:  Table [CTL].[ControlManifest]    Script Date: 3/12/2021 3:44:57 PM ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[CTL].[ControlManifest]') AND type in (N'U'))
DROP TABLE [CTL].[ControlManifest]
GO

/****** Object:  Table [CTL].[ControlManifest]    Script Date: 3/12/2021 3:44:57 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [CTL].[ControlManifest](
	[BatchExecutionLogID] [bigint] NOT NULL,
	[TaskExecutionLogID] [bigint] NOT NULL,
	[SourceObject] [varchar](1000) NULL,
	[Container] [varchar](1000) NULL,
	[StartCounter] [varchar](500) NULL,
	[EndCounter] [varchar](500) NULL,
	[RecordCountLoaded] [bigint] NULL,
	[RecordCountDeltaTable] [bigint] NULL,
	[FolderName] [varchar](1000) NULL,
	[FileName] [varchar](1000) NOT NULL,
	[ProcessedToTrustedZone] [bit] NULL,
	[DeltaColumn] [nvarchar](100) NULL,
	[ProcessedToSQLEDW] [bit] NULL,
	[RawZonePipelineRunID] [varchar](50) NULL,
	[TrustedZonePipelineRunID] [varchar](50) NULL,
	[SQLEDWPipelineRunID] [varchar](50) NULL
) ON [PRIMARY]
GO

ALTER TABLE [CTL].[ControlManifest] ADD  CONSTRAINT [PK_CTL_ControlManifest] PRIMARY KEY CLUSTERED 
(
	[BatchExecutionLogID] ASC,
	[TaskExecutionLogID] ASC,
	[FileName] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF) ON [PRIMARY]
GO


