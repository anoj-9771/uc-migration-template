



CREATE view [compliance].[vwAvetmissReportingPeriod]
as

select 
	ReportingPeriodKey as REPORT_DATE,
	SpecialReportingYear as REPORTING_YEAR, 
	convert(varchar(10), ReportingDate, 112) as EXTRACT_DATE, 
	convert(varchar(10), ReportingPeriodKeyLastYear, 112) as WEEK_LAST_YEAR,
	ReportingMonthName MONTH_NAME, 
	ReportingMonthNo MONTH_NUM,
	NULL as MONTH_NAME_FY,
	CASE
		WHEN ReportingDate = (SELECT max(ReportingDate) FROM compliance.AvetmissReportingPeriod rp where left(ReportingYear, 2) = 'CY' and rp.ReportingDate <= getdate())
			THEN 'Current Week ' + cast(ReportingYearNo as varchar(4))
		WHEN ReportingPeriodKey = (SELECT max(ReportingPeriodKeyLastYear)
								from compliance.AvetmissReportingPeriod
								WHERE left(ReportingYear, 2) = 'CY'
								and ReportingDate = (SELECT max(ReportingDate) FROM compliance.AvetmissReportingPeriod rp where left(reportingyear, 2) = 'CY' and rp.ReportingDate <= getdate()))
			THEN 'Current Week Last Year ' + cast(ReportingYearNo as varchar(4))
		WHEN ReportingDate = (SELECT max(dateadd(week, -1, ReportingDate))
								from compliance.AvetmissReportingPeriod
								WHERE left(ReportingYear, 2) = 'CY'
								and ReportingDate = (SELECT max(ReportingDate) FROM compliance.AvetmissReportingPeriod rp where left(reportingyear, 2) = 'CY' and rp.ReportingDate <= getdate()))
			THEN 'Current Week Last Week ' + cast(ReportingYearNo as varchar(4))
		ELSE 'Previous Week ' + cast(ReportingYearNo as varchar(4)) + ' - ' + format(ReportingDate,'yyyyMMdd') 
				
					
	END AS CURRENT_WEEK
	,[ReportingSemester]
,[ReportingMonthNo]
,[ReportingMonthName]

from compliance.AvetmissReportingPeriod rp
where left(ReportingYear, 2) = 'CY'
and rp.ReportingDate <= getdate() 
and rp._RecordCurrent = 1 and rp._RecordDeleted = 0
and rp.ReportingPeriodKey not in (	
20150104	,
20150111	,
20150118	,
20150125	,
20150201	,
20150208	,
20150215	,
20150222	,
20160103	,
20170101	
)
and NOT (reportingYearNo = 2015 and convert(varchar(10), ReportingDate, 112)  in ( 20160313, 20160320, 20160327))
and NOT (reportingYearNo = 2016 and convert(varchar(10), ReportingDate, 112) in ( 20170226, 20170305, 20170312, 20170319, 20170326))
and NOT (reportingYearNo = 2017 and convert(varchar(10), ReportingDate, 112) in ( 20180304, 20180311,20180318, 20180325))
and NOT (reportingYearNo = 2018 and convert(varchar(10), ReportingDate, 112) in (20190310,20190317,20190324, 20190331))
and NOT (reportingYearNo = 2019 and convert(varchar(10), ReportingDate, 112) in ( 20200308,20200315, 20200322, 20200329))

--201718: 20180909,20180916, 20180923

union all

select 

	cast(ReportingPeriodKey as varchar(12)) as REPORT_DATE,
	SpecialReportingYear as REPORTING_YEAR, 
	convert(varchar(10), ReportingDate, 112) as EXTRACT_DATE, 
	ReportingPeriodKeyLastYear as WEEK_LAST_YEAR,
	ReportingMonthName MONTH_NAME, 
	ReportingMonthNo MONTH_NUM,
	MonthNameFY as MONTH_NAME_FY,
	CASE
		WHEN ReportingDate = (SELECT max(ReportingDate) FROM compliance.AvetmissReportingPeriod rp where left(ReportingYear, 2) = 'FY' and rp.ReportingDate <= getdate())
			THEN 'Current Week FY ' + left(ReportingPeriodKey, 6)
		WHEN ReportingPeriodKey = (SELECT max(ReportingPeriodKeyLastYear)
								from compliance.AvetmissReportingPeriod
								WHERE left(ReportingYear, 2) = 'FY'
								and ReportingDate = (SELECT max(ReportingDate) FROM compliance.AvetmissReportingPeriod rp where left(reportingyear, 2) = 'CY' and rp.ReportingDate <= getdate()))
			THEN 'Current Week Last Year FY ' + left(ReportingPeriodKey, 6)
		WHEN ReportingDate = (SELECT max(dateadd(week, -1, ReportingDate))
								from compliance.AvetmissReportingPeriod
								WHERE left(ReportingYear, 2) = 'FY'
								and ReportingDate = (SELECT max(ReportingDate) FROM compliance.AvetmissReportingPeriod rp where left(reportingyear, 2) = 'CY' and rp.ReportingDate <= getdate()))
			THEN 'Current Week Last Week FY ' + left(ReportingPeriodKey, 6)
		ELSE 'Financial Year ' +  '- ' + cast(ReportingPeriodKey as varchar(12)) 
	END AS CURRENT_WEEK
	,[ReportingSemester]
,[ReportingMonthNo]
,[ReportingMonthName]
from compliance.AvetmissReportingPeriod rp
where left(ReportingYear, 2) = 'FY' 
and rp.ReportingDate <= getdate() 
and rp._RecordCurrent = 1 and rp._RecordDeleted = 0

and rp.ReportingPeriodKey not in (	
2017180702	,
2017180709	,
2017180716	,
2017180723	,
2017180730	,
2017180806	,
2017180813	,
2017180820	,
2017180827	,
2018190708	,
2018190715	,
2018190722	,
2018190729	,
2018190805	,
2018190812	,
2018190819	,
2018190826	,
2018190902	,
2018190909	)
and NOT (reportingYear = 'FY2018' and convert(varchar(10), ReportingDate, 112) in ( 20180909,20180916, 20180923))


union all

select * 
from
(
	select 2014 as REPORT_DATE, 2014 as REPORTING_YEAR, 2014 as EXTRACT_DATE, 2013 as WEEK_LAST_YEAR, 'January' as MONTH_NAME, 1 as MONTH_NUM, NULL as MONTH_NAME_FY, 'Final 2014' as CURRENT_WEEK,NULL [ReportingSemester] ,NULL [ReportingMonthNo] ,NULL [ReportingMonthName]
	union all
	select 2015 as REPORT_DATE, 2015 as REPORTING_YEAR, 2015 as EXTRACT_DATE, 2014 as WEEK_LAST_YEAR, 'January' as MONTH_NAME, 1 as MONTH_NUM, NULL as MONTH_NAME_FY, 'Final 2015' as CURRENT_WEEK,NULL [ReportingSemester] ,NULL [ReportingMonthNo] ,NULL [ReportingMonthName]
	union all
	select 2016 as REPORT_DATE, 2016 as REPORTING_YEAR, 2016 as EXTRACT_DATE, 2015 as WEEK_LAST_YEAR, 'January' as MONTH_NAME, 1 as MONTH_NUM, NULL as MONTH_NAME_FY, 'Final 2016' as CURRENT_WEEK,NULL [ReportingSemester] ,NULL [ReportingMonthNo] ,NULL [ReportingMonthName]
	union all
	select 2017 as REPORT_DATE, 2017 as REPORTING_YEAR, 2017 as EXTRACT_DATE, 2016 as WEEK_LAST_YEAR, 'January' as MONTH_NAME, 1 as MONTH_NUM, NULL as MONTH_NAME_FY, 'Final 2017' as CURRENT_WEEK,NULL [ReportingSemester] ,NULL [ReportingMonthNo] ,NULL [ReportingMonthName]
	union all
	select 20171 as REPORT_DATE, 2017 as REPORTING_YEAR, 20171 as EXTRACT_DATE, 2016 as WEEK_LAST_YEAR, 'January' as MONTH_NAME, 1 as MONTH_NUM, NULL as MONTH_NAME_FY, 'Final 2017' as CURRENT_WEEK,NULL [ReportingSemester] ,NULL [ReportingMonthNo] ,NULL [ReportingMonthName]
	union all
	select 2018 as REPORT_DATE, 2018 as REPORTING_YEAR, 2018 as EXTRACT_DATE, 2017 as WEEK_LAST_YEAR, 'January' as MONTH_NAME, 1 as MONTH_NUM, NULL as MONTH_NAME_FY, 'Final 2018' as CURRENT_WEEK,NULL [ReportingSemester] ,NULL [ReportingMonthNo] ,NULL [ReportingMonthName]
	union all
	select 2019 as REPORT_DATE, 2019 as REPORTING_YEAR, 2019 as EXTRACT_DATE, 2018 as WEEK_LAST_YEAR, 'January' as MONTH_NAME, 1 as MONTH_NUM, NULL as MONTH_NAME_FY, 'Final 2019' as CURRENT_WEEK,NULL [ReportingSemester] ,NULL [ReportingMonthNo] ,NULL [ReportingMonthName]
	union all
	select 2020 as REPORT_DATE, 2020 as REPORTING_YEAR, 2020 as EXTRACT_DATE, 2019 as WEEK_LAST_YEAR, 'January' as MONTH_NAME, 1 as MONTH_NUM, NULL as MONTH_NAME_FY, 'Final 2020' as CURRENT_WEEK,NULL [ReportingSemester] ,NULL [ReportingMonthNo] ,NULL [ReportingMonthName]
	union all
	select 201617 as REPORT_DATE, 201617 as REPORTING_YEAR, 201617 as EXTRACT_DATE, 201516 as WEEK_LAST_YEAR, NULL as MONTH_NAME, NULL as MONTH_NUM, NULL as MONTH_NAME_FY, 'Final 2016 - 17' as CURRENT_WEEK,NULL [ReportingSemester] ,NULL [ReportingMonthNo] ,NULL [ReportingMonthName]
	union all
	select 201718 as REPORT_DATE, 201718 as REPORTING_YEAR, 201718 as EXTRACT_DATE, 201617 as WEEK_LAST_YEAR, NULL as MONTH_NAME, NULL as MONTH_NUM, NULL as MONTH_NAME_FY, 'Final 2017 - 18' as CURRENT_WEEK,NULL [ReportingSemester] ,NULL [ReportingMonthNo] ,NULL [ReportingMonthName]
	union all
	select 201819 as REPORT_DATE, 201819 as REPORTING_YEAR, 201819 as EXTRACT_DATE, 201718 as WEEK_LAST_YEAR, NULL as MONTH_NAME, NULL as MONTH_NUM, NULL as MONTH_NAME_FY, 'Final 2018 - 19' as CURRENT_WEEK,NULL [ReportingSemester] ,NULL [ReportingMonthNo] ,NULL [ReportingMonthName]
	union all
	select 201920 as REPORT_DATE, 201920 as REPORTING_YEAR, 201920 as EXTRACT_DATE, 201819 as WEEK_LAST_YEAR, NULL as MONTH_NAME, NULL as MONTH_NUM, NULL as MONTH_NAME_FY, 'Final 2019 - 20' as CURRENT_WEEK,NULL [ReportingSemester] ,NULL [ReportingMonthNo] ,NULL [ReportingMonthName]
	union all
	select 20201215 as REPORT_DATE, 2020 as REPORTING_YEAR, 20191215 as EXTRACT_DATE, 20190106 as WEEK_LAST_YEAR, 'January' as MONTH_NAME, 1 as MONTH_NUM, NULL as MONTH_NAME_FY, 'Previous Week 2020 - 20190106' as CURRENT_WEEK,1 [ReportingSemester] ,1 [ReportingMonthNo] ,'January' [ReportingMonthName]
) FinalYear