

CREATE View [Avetmiss].[ReportingPeriod]
AS

SELECT [AvetmissReportingPeriodSK]
, [ReportingPeriodKey]
, [ReportingDate]
, [ReportingWeek]
, [ReportingMonthNo]
, [ReportingMonthName]
, [ReportingQuarter]
, [ReportingSemester]
, [ReportingYear]
, [ReportingDatePreviousYear]
, [ReportingDateNextYear]
, [ReportingYearNo]
, [MonthNameFY]
, [SpecialReportingYear]
, [ReportingPeriodKeyLastYear]
, [ReportingYearStartDate]
, [ReportingYearEndDate]


,	CASE
	 WHEN ReportingDate > GETDATE() THEN NULL		
	WHEN ReportingDate = (SELECT max(ReportingDate) FROM compliance.AvetmissReportingPeriod rp where left(ReportingYear, 2) = 'CY' and rp.ReportingDate <= getdate())
			THEN 'Current Week ' + cast(ReportingYear as varchar(6))
		WHEN ReportingPeriodKey = (SELECT max(ReportingPeriodKeyLastYear)
								from compliance.AvetmissReportingPeriod
								WHERE left(ReportingYear, 2) = 'CY'
								and ReportingDate = (SELECT max(ReportingDate) FROM compliance.AvetmissReportingPeriod rp where left(reportingyear, 2) = 'CY' and rp.ReportingDate <= getdate()))
			THEN 'Current Week Last Year ' + cast(ReportingYear as varchar(6))
		WHEN ReportingDate = (SELECT max(dateadd(week, -1, ReportingDate))
								from compliance.AvetmissReportingPeriod
								WHERE left(ReportingYear, 2) = 'CY'
								and ReportingDate = (SELECT max(ReportingDate) FROM compliance.AvetmissReportingPeriod rp where left(reportingyear, 2) = 'CY' and rp.ReportingDate <= getdate()))
			THEN 'Current Week Last Week ' + cast(ReportingYear as varchar(6))
		ELSE 'Previous Week ' + cast(ReportingYear as varchar(6)) + ' - ' + format(ReportingDate,'yyyyMMdd') 
				
					
	END AS CURRENT_WEEK

,	Case 
	when ReportingWeek between 1 and 5 then 'January'
	when ReportingWeek between 6 and 9 then 'February'
	when ReportingWeek between 10 and 13 then 'March'
	when ReportingWeek between 14 and 18 then 'April'
	when ReportingWeek between 19 and 22 then 'May'
	when ReportingWeek between 23 and 26 then 'June'
	when ReportingWeek between 27 and 31 then 'July'
	when ReportingWeek between 32 and 35 then 'August'
	when ReportingWeek between 36 and 39 then 'September'
	when ReportingWeek between 40 and 44 then 'October'
	when ReportingWeek between 45 and 48 then 'November'
	when ReportingWeek >= 49 then 'December'
	Else Null
	End as MonthName554Calendar
,		Case 
	when ReportingWeek between 1 and 4 then 'January'
	when ReportingWeek between 5 and 8 then 'February'
	when ReportingWeek between 9 and 13 then 'March'
	when ReportingWeek between 14 and 17 then 'April'
	when ReportingWeek between 18 and 21 then 'May'
	when ReportingWeek between 22 and 26 then 'June'
	when ReportingWeek between 27 and 30 then 'July'
	when ReportingWeek between 31 and 34 then 'August'
	when ReportingWeek between 35 and 39 then 'September'
	when ReportingWeek between 40 and 43 then 'October'
	when ReportingWeek between 44 and 47 then 'November'
	when ReportingWeek >= 48 then 'December'
	Else Null
	End as MonthName445Calendar
, [_DLCuratedZoneTimeStamp]
, [_RecordStart]
, [_RecordEnd]
, [_RecordDeleted]
, [_RecordCurrent]
  FROM --[compliance].[AvetmissReportingPeriod]

  (
				Select a.* 
				from  [compliance].[AvetmissReportingPeriod] a 
				left 
				join [reference].[AvetmissReportingYearCutOffDate] cutoff 
				on a.reportingyear = cutoff.reportingyear 
				and a.reportingdate > cutoff.AvetmissCutOffReportingDate 
				where cutoff.reportingyear is  null
			) rp  --[compliance].[AvetmissReportingPeriod] C 