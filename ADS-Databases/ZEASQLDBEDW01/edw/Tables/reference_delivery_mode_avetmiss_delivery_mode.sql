CREATE TABLE [edw].[reference_delivery_mode_avetmiss_delivery_mode] (
    [DeliveryModeCode]         NVARCHAR (12) NULL,
    [AvetmissDeliveryModeCode] NVARCHAR (3)  NULL,
    [_DLRawZoneTimeStamp]      DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp]  DATETIME2 (7) NULL,
    [_RecordStart]             DATETIME2 (7) NULL,
    [_RecordEnd]               DATETIME2 (7) NULL,
    [_RecordCurrent]           INT           NULL,
    [_RecordDeleted]           INT           NULL
);



