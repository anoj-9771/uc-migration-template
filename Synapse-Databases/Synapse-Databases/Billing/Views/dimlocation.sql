CREATE VIEW [billing].[dimlocation]
AS select
DimLocationSK as DimLocationSK
, LocationID as "Location ID"
, formattedAddress as "Formatted Address"
, streetName as "Street Name"
, StreetType as "Street Type"
, LGA as "LGA"
, suburb as "Suburb"
, state as "State"
, latitude as "Latitude"
, longitude as "Longitude"
from dbo.dimlocation;