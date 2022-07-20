﻿CREATE TABLE [CTL].[ControlManifest] (
 [ManifestID] BIGINT IDENTITY (1, 1) NOT NULL,
 [BatchExecutionLogID] BIGINT NOT NULL,
 [TaskExecutionLogID] BIGINT NOT NULL,
 [SourceObject] VARCHAR (1000) NULL,
 [Container] VARCHAR (1000) NULL,
 [StartCounter] VARCHAR (500) NULL,
 [EndCounter] VARCHAR (500) NULL,
 [RecordCountLoaded] BIGINT NULL,
 [RecordCountDeltaTable] BIGINT NULL,
 [RecordCountTargetTable] BIGINT NULL,
 [FolderName] VARCHAR (1000) NULL,
 [FileName] VARCHAR (1000) NULL,
 [ProcessedToCleansedZone] BIT NULL,
 [DeltaColumn] NVARCHAR (100) NULL,
 [ProcessedToSQLEDW] BIT NULL,
 [RawZonePipelineRunID] VARCHAR (50) NULL,
 [CleansedZonePipelineRunID] VARCHAR (50) NULL,
 [SQLEDWPipelineRunID] VARCHAR (50) NULL,
 [SourceFileDateStamp] CHAR (14) NULL,
 [SourceFileName] VARCHAR (1000) NULL,
 [M_DeltaRecordCount] BIGINT NULL,
 [M_TotalNoRows] BIGINT NULL,
 [M_Message] VARCHAR (255) NULL,
 [taskid] BIGINT NULL,
 [controlstageid] BIGINT NULL,
 [projectname] VARCHAR (100) NULL,
 [triggername] VARCHAR (100) NULL,
 [projectid] BIGINT NULL,
 CONSTRAINT [UQ_ControlManifest_SrcFileDateStamp] UNIQUE NONCLUSTERED ([BatchExecutionLogID] ASC, [TaskExecutionLogID] ASC, [SourceFileDateStamp] ASC),
 CONSTRAINT [PK_CTL_ControlManifest] PRIMARY KEY CLUSTERED ([ManifestID] ASC)
);
