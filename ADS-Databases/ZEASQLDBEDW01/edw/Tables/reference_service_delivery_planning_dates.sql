CREATE TABLE [edw].[reference_service_delivery_planning_dates] (
    [SemesterID]                INT           NULL,
    [PlanStartDate]             DATE          NULL,
    [PlanEndDate]               DATE          NULL,
    [ReviewStartDate]           DATE          NULL,
    [ReviewEndDate]             DATE          NULL,
    [AdjustmentStartDate]       DATE          NULL,
    [AdjustmentEndDate]         DATE          NULL,
    [SecondReviewStartDate]     DATE          NULL,
    [SecondReviewEndDate]       DATE          NULL,
    [SecondAdjustmentStartDate] DATE          NULL,
    [SecondAdjustmentEndDate]   DATE          NULL,
    [_DLRawZoneTimeStamp]       DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp]   DATETIME2 (7) NULL,
    [_RecordStart]              DATETIME2 (7) NULL,
    [_RecordEnd]                DATETIME2 (7) NULL,
    [_RecordCurrent]            INT           NULL,
    [_RecordDeleted]            INT           NULL
);



