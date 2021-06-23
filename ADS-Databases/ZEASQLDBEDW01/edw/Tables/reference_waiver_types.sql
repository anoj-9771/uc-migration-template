CREATE TABLE [edw].[reference_waiver_types] (
    [WaiverTypeCode]          NVARCHAR (MAX) NULL,
    [WaiverTypeName]          NVARCHAR (MAX) NULL,
    [WaiverTypeDescription]   NVARCHAR (MAX) NULL,
    [DisabilityDependencies]  NVARCHAR (MAX) NULL,
    [ExclusiveWaiver]         NVARCHAR (MAX) NULL,
    [ExclusiveFee]            NVARCHAR (MAX) NULL,
    [FeeExemption]            NVARCHAR (MAX) NULL,
    [IsOncePerCalendarYear]   NVARCHAR (MAX) NULL,
    [IsFeeHelpApplicable]     NVARCHAR (MAX) NULL,
    [IsVetFeeHelpApplicable]  NVARCHAR (MAX) NULL,
    [_DLRawZoneTimeStamp]     DATETIME2 (7)  NULL,
    [_DLTrustedZoneTimeStamp] DATETIME2 (7)  NULL,
    [_RecordStart]            DATETIME2 (7)  NULL,
    [_RecordEnd]              DATETIME2 (7)  NULL,
    [_RecordCurrent]          INT            NULL,
    [_RecordDeleted]          INT            NULL
);



