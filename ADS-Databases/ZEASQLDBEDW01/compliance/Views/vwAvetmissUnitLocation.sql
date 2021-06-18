





CREATE   view [compliance].[vwAvetmissUnitLocation]
as

select  LOCATION_CODE
, [RTO Code] as INSTITUTE_CODE
, [Delivery Location]
, [Delivery Region]
, [RTO Name]
, Region
, [RTO Number]
, [Location Code] 
from [compliance].[vwAvetmissLocation]