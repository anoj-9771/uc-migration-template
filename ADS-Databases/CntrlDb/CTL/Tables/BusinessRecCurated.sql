CREATE TABLE [CTL].[BusinessRecCurated]
(
  	BusinessRecId				bigint IDENTITY(1,1) NOT NULL,
	ExtractRunID				varchar(255) NOT NULL,
	CreatedDateTime				datetime NOT NULL,
	UpdatedDateTime				datetime NOT NULL,
	CreatedBatchExecutionId		bigint NOT NULL,
	CreatedTaskExecutionLogId	bigint NOT NULL,
	RawZonePipelineRunId		varchar(255) NOT NULL,
	UpdatedBatchExecutionId		bigint NOT NULL,
	UpdatedTaskExecutionLogId	bigint NOT NULL,
	CuratedPipelineRunId		varchar(255) NOT NULL,
	BusinessReconGroup			varchar(255) NOT NULL,
	SourceObject				varchar(255) NOT NULL,
	TargetObject				varchar(255) NOT NULL,
	WatermarkValue				bigint NOT NULL,
	MeasureId					varchar(255) NOT NULL,
	MeasureName					varchar(255) NOT NULL,
	SourceMeasureValue			decimal(28,7),
	TargetMeasureValue			decimal(28,7),
	BusinessRecResult			varchar(255),
 CONSTRAINT PK_BusinessRecCurated PRIMARY KEY CLUSTERED (BusinessRecID ASC),
 CONSTRAINT UK_BusinessRecCurated UNIQUE (CreatedBatchExecutionId, CreatedTaskExecutionLogId, TargetObject, MeasureId)
)
