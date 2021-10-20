CREATE VIEW [billing].[factdailyapportionedconsumption]
AS select
factDailyApportionedConsumptionSK as factDailyApportionedConsumptionSK
, sourceSystemCode as "Source System Code"
, consumptionDateSK as consumptionDateSK
, dimBillingDocumentSK as dimBillingDocumentSK
, dimPropertySK as dimPropertySK
, dimMeterSK as dimMeterSK
, dimLocationSK as dimLocationSK
, dimWaterNetworkSK as dimWaterNetworkSK
, dailyApportionedConsumption as "Daily Apportioned Consumption"
from dbo.factdailyapportionedconsumption;