CREATE TABLE [CTL].[BusinessRecConfig]
(
	BusinessRecConfigId   bigint IDENTITY(1,1) NOT NULL,
	BusinessReconGroup    varchar(255) NULL,
	MeasureId             varchar(255) NULL,
	MeasureName           varchar(255) NULL,
	TargetObject          varchar(255) NULL,
	TargetQuery           varchar(2000) NULL,
	Enabled               bit NULL,
  CONSTRAINT PK_BusinessRecConfig PRIMARY KEY CLUSTERED (BusinessRecConfigId ASC),
  CONSTRAINT UK_BusinessRecConfig UNIQUE (BusinessReconGroup, MeasureId)
)