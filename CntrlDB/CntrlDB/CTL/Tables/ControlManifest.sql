CREATE TABLE [CTL].[ControlManifest] (
    [BatchExecutionLogID]      BIGINT         NOT NULL,
    [TaskExecutionLogID]       BIGINT         NOT NULL,
    [SourceObject]             VARCHAR (1000) NULL,
    [Container]                VARCHAR (1000) NULL,
    [StartCounter]             VARCHAR (500)  NULL,
    [EndCounter]               VARCHAR (500)  NULL,
    [RecordCountLoaded]        BIGINT         NULL,
    [RecordCountDeltaTable]    BIGINT         NULL,
    [FolderName]               VARCHAR (1000) NULL,
    [FileName]                 VARCHAR (1000) NOT NULL,
    [ProcessedToTrustedZone]   BIT            NULL,
    [DeltaColumn]              NVARCHAR (100) NULL,
    [ProcessedToSQLEDW]        BIT            NULL,
    [RawZonePipelineRunID]     VARCHAR (50)   NULL,
    [TrustedZonePipelineRunID] VARCHAR (50)   NULL,
    [SQLEDWPipelineRunID]      VARCHAR (50)   NULL,
    CONSTRAINT [PK_CTL_ControlManifest] PRIMARY KEY CLUSTERED ([BatchExecutionLogID] ASC, [TaskExecutionLogID] ASC, [FileName] ASC)
);

















