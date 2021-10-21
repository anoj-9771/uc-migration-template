CREATE VIEW [billing].[dimbillingdocument]
AS select
dimbillingDocumentSK as dimbillingDocumentSK
, sourceSystemCode as "Source System Code"
, billingDocumentNumber as "Billing Document Number"
, billingPeriodStartDate as "Billing Period Start Date"
, billingPeriodEndDate as "Billing Period End Date"
, billCreatedDate as "Bill Created Date"
, isOutsortedFlag as "Outsorted Flag"
, isReversedFlag as "Reversed Flag"
, reversalDate as "Reversal Date"
from dbo.dimbillingdocument;