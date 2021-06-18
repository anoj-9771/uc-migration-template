CREATE TABLE [edw].[reference_avetmiss_delivery_mode_pre_2018] (
    [AvetmissDeliveryModeID]          INT           NULL,
    [AvetmissDeliveryModeDescription] NVARCHAR (26) NULL,
    [_DLRawZoneTimeStamp]             DATETIME2 (7) NULL,
    [_DLTrustedZoneTimeStamp]         DATETIME2 (7) NULL,
    [_RecordStart]                    DATETIME2 (7) NULL,
    [_RecordEnd]                      DATETIME2 (7) NULL,
    [_RecordCurrent]                  INT           NULL,
    [_RecordDeleted]                  INT           NULL
);



