CREATE TABLE [CTL].[BusinessRecCurated]
(
  BusinessRecID			bigint IDENTITY(1,1) NOT NULL,
	CreatedDateTime			bigint NOT NULL,
	UpdatedDateTime			bigint NOT NULL,
	BatchExecutionId		bigint NOT NULL,
	TaskExecutionLogId		bigint NOT NULL,
	RawZonePipelineRunID	varchar(255) NOT NULL,
	BusinessReconGroup		varchar(255) NOT NULL,
	SourceObject			varchar(255) NOT NULL,
	TargetObject			varchar(255) NOT NULL,
	WatermarkValue			bigint NOT NULL,
	MeasureID				varchar(255) NOT NULL,
	MeasureName				varchar(255) NOT NULL,
	SourceMeasureValue		decimal(28,7),
	TargetMeasureValue		decimal(28,7),
	BusinessRecResult		varchar(255),
 CONSTRAINT PK_BusinessRecCurated PRIMARY KEY CLUSTERED (BusinessRecID ASC),
 CONSTRAINT UK_BusinessRecCurated UNIQUE (BatchExecutionId, TaskExecutionLogId, TargetObject, MeasureID)
)
