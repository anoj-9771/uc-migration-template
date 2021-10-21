CREATE VIEW [billing].[dimproperty]
AS select
DimPropertySK as DimPropertySK
, propertyId as "Property Number"
, sourceSystemCode as "Source System Code"
, propertyStartDate as "Property Start Date"
, propertyEndDate as "Property End Date"
, propertyType as "Property Type"
, superiorPropertyType as "Superior Property Type"
, areaSize as "Area Size"
, LGA as "LGA"
from dbo.dimproperty;