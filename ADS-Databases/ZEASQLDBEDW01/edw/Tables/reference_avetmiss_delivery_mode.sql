CREATE TABLE [edw].[reference_avetmiss_delivery_mode] (
    [AvetmissDeliveryModeCode]        NVARCHAR (3)  NULL,
    [Internal]                        NVARCHAR (1)  NULL,
    [External]                        NVARCHAR (1)  NULL,
    [WorkplaceBased]                  NVARCHAR (1)  NULL,
    [AvetmissDeliveryModeDescription] NVARCHAR (60) NULL,
    [_DLRawZoneTimeStamp]             DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp]         DATETIME2 (7) NULL,
    [_RecordStart]                    DATETIME2 (7) NULL,
    [_RecordEnd]                      DATETIME2 (7) NULL,
    [_RecordCurrent]                  INT           NULL,
    [_RecordDeleted]                  INT           NULL
);



