CREATE TABLE [stage].[Factdailyapportionedconsumption] (
    [factDailyApportionedConsumptionSK] BIGINT          NULL,
    [sourceSystemCode]                  NVARCHAR (256)  NULL,
    [consumptionDateSK]                 BIGINT          NULL,
    [dimBillingDocumentSK]              BIGINT          NULL,
    [dimPropertySK]                     BIGINT          NULL,
    [dimMeterSK]                        BIGINT          NULL,
    [dimLocationSK]                     BIGINT          NULL,
    [dimWaterNetworkSK]                 INT             NULL,
    [dailyApportionedConsumption]       DECIMAL (18, 6) NULL,
    [_DLCuratedZoneTimeStamp]           DATETIME2 (7)   NULL,
    [_RecordStart]                      DATETIME2 (7)   NULL,
    [_RecordEnd]                        DATETIME2 (7)   NULL,
    [_RecordDeleted]                    INT             NULL,
    [_RecordCurrent]                    INT             NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);

