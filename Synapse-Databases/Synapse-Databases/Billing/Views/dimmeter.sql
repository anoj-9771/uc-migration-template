CREATE VIEW [billing].[dimmeter]
AS select
DimMeterSK as DimMeterSK
, sourceSystemCode as "Source System Code"
, meterId as "Meter Number"
, meterSize as "Meter Size"
, waterMeterType as "Water Meter Type"
from dbo.dimmeter;