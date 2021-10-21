CREATE VIEW [billing].[factbilledwaterconsumption]
AS select
factBilledWaterConsumptionSK as factBilledWaterConsumptionSK
, sourceSystemCode as "Source System Code"
, dimBillingDocumentSK as dimBillingDocumentSK
, dimPropertySK as dimPropertySK
, dimMeterSK as dimMeterSK
, dimLocationSK as dimLocationSK
, dimWaterNetworkSK as dimWaterNetworkSK
, billingPeriodStartDateSK as billingPeriodStartDateSK
, billingPeriodEndDateSK as billingPeriodEndDateSK
, meteredWaterConsumption as "Metered Water Consumption"
from dbo.factbilledwaterconsumption;