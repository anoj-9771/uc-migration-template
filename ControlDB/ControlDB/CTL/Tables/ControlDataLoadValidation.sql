CREATE TABLE [CTL].[ControlDataLoadValidation] (
    [DataValidationID]    BIGINT        IDENTITY (1, 1) NOT NULL,
    [ProjectRunID]        VARCHAR (50)  NOT NULL,
    [ObjectName]          VARCHAR (255) NULL,
    [SourceHighWatermark] VARCHAR (100) NULL,
    [SourceRecordCount]   BIGINT        NULL,
    [SourceTotalValue]    BIGINT        NULL,
    [SourceMinValue]      BIGINT        NULL,
    [SourceMaxValue]      BIGINT        NULL,
    [TargetHighWatermark] VARCHAR (100) NULL,
    [TargetRecordCount]   BIGINT        NULL,
    [TargetTotalValue]    BIGINT        NULL,
    [TargetMinValue]      BIGINT        NULL,
    [TargetMaxValue]      BIGINT        NULL
);

