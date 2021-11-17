CREATE VIEW [billing].[dimdate]
AS select
DimDateSK as DimDateSK
, calendarDate as "Calendar Date"
, dayOfWeek as "Day Of Week"
, dayName as "Day Name"
, dayOfMonth as "Day Of Month"
, dayOfYear as "Day Of Year"
, monthOfYear as "Month Of Year"
, monthName as "Month Name"
, quarterOfYear as "Quarter Of Year"
, halfOfYear as "Half Of Year"
, monthStartDate as "Month Start Date"
, monthEndDate as "Month End Date"
, yearStartDate as "Year Start Date"
, yearEndDate as "Year End Date"
, financialYear as "Financial Year"
, financialYearStartDate as "Financial Year Start Date"
, financialYearEndDate as "Financial Year End Date"
, monthOfFinancialYear as "Month Of Financial Year"
, quarterOfFinancialYear as "Quarter Of Financial Year"
, halfOfFinancialYear as "Half Of Financial Year"
from dbo.dimdate;