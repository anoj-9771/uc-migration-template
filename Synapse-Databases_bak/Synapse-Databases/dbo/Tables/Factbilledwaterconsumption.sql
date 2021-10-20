CREATE TABLE [dbo].[Factbilledwaterconsumption] (
    [factBilledWaterConsumptionSK] BIGINT          NULL,
    [sourceSystemCode]             NVARCHAR (256)  NULL,
    [dimBillingDocumentSK]         BIGINT          NULL,
    [dimPropertySK]                BIGINT          NULL,
    [dimMeterSK]                   BIGINT          NULL,
    [dimLocationSK]                BIGINT          NULL,
    [dimWaterNetworkSK]            INT             NULL,
    [billingPeriodStartDateSK]     BIGINT          NULL,
    [billingPeriodEndDateSK]       BIGINT          NULL,
    [meteredWaterConsumption]      DECIMAL (28, 6) NULL,
    [_DLCuratedZoneTimeStamp]      DATETIME2 (7)   NULL,
    [_RecordStart]                 DATETIME2 (7)   NULL,
    [_RecordEnd]                   DATETIME2 (7)   NULL,
    [_RecordDeleted]               INT             NULL,
    [_RecordCurrent]               INT             NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([dimPropertySK]));

