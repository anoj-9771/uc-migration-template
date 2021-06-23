




CREATE VIEW [aas].[PartitionAllUnitEnrolment] AS 

WITH
CTE_unit AS (
	SELECT 
	C.ReportingPeriodKey 
	,FORMATMESSAGE('%s%dQ%d - %d', IIF(LEFT(C.ReportingYear, 1)='F', 'FY', ''), IIF(LEFT(C.ReportingYear, 1)='C', C.ReportingYearNo, C.SpecialReportingYear), C.ReportingQuarter, C.ReportingPeriodKey) [PartitionName]
	,FORMATMESSAGE('SELECT * FROM [compliance].[vwAvetmissUnitEnrolment] WHERE ENROLMENT_YEAR = %s AND REPORT_DATE = ''%d''',Right(C.ReportingYear,4), C.ReportingPeriodKey) [Query]
	,'CurrentWeekly' [Grouping]
	,NULL as [PreviousReportingPeriodKey]
	,NULL as [PreviousPartitionName]
	,NULL as [PreviousQuery]
	,Null as [PreviousGrouping]
	,DATEADD(DAY, 1, C.[ReportingDate]) [FromDate]
	,DATEADD(DAY, 7, C.[ReportingDate]) [ToDate]
	FROM (
				Select a.* 
				from  [compliance].[AvetmissReportingPeriod] a 
				left 
				join [reference].[AvetmissReportingYearCutOffDate] cutoff 
				on a.reportingyear = cutoff.reportingyear 
				and a.reportingdate > cutoff.AvetmissCutOffReportingDate 
				where cutoff.reportingyear is  null
				and left(a.ReportingYear,2) = 'CY'
			) c  --[compliance].[AvetmissReportingPeriod] C 
	JOIN [compliance].[AvetmissReportingPeriod] P ON P.ReportingPeriodKey = C.ReportingPeriodKeyLastYear AND P._RecordCurrent = 1
	WHERE 
	C._RecordCurrent = 1

),

CTE_unitCourse AS (
	SELECT 
	C.ReportingPeriodKey 
	,'OneEBS-CourseEnrolment' [PartitionName]
	,FORMATMESSAGE('SELECT * FROM [compliance].[vwAvetmissUnitCourseEnrolment] WHERE ENROLMENT_YEAR = %s AND REPORT_DATE = ''%d''',Right(C.ReportingYear,4), C.ReportingPeriodKey) [Query]
	,'Dimensions' [Grouping]
	,NULL as [PreviousReportingPeriodKey]
	,NULL as [PreviousPartitionName]
	,NULL as [PreviousQuery]
	,Null as [PreviousGrouping]
	,DATEADD(DAY, 1, C.[ReportingDate]) [FromDate]
	,DATEADD(DAY, 7, C.[ReportingDate]) [ToDate]
	FROM (
				Select a.* 
				from  [compliance].[AvetmissReportingPeriod] a 
				left 
				join [reference].[AvetmissReportingYearCutOffDate] cutoff 
				on a.reportingyear = cutoff.reportingyear 
				and a.reportingdate > cutoff.AvetmissCutOffReportingDate 
				where cutoff.reportingyear is  null
				and left(a.ReportingYear,2) = 'CY'
			) c  --[compliance].[AvetmissReportingPeriod] C 
	JOIN [compliance].[AvetmissReportingPeriod] P ON P.ReportingPeriodKey = C.ReportingPeriodKeyLastYear AND P._RecordCurrent = 1
	WHERE 
	C._RecordCurrent = 1

),


UNION_CTE AS (

	SELECT 'UnitEnrolment' [ModelName], [PartitionName], [Table], [Query], 'Dimensions' [Grouping], NULL [ReportingPeriodKey], '1900-01-01' [FromDate], '9999-12-31' [ToDate]
	FROM (
		SELECT 'OneEBS-Learner' [PartitionName], 'Learner' [Table], 'SELECT * FROM [compliance].[vwAvetmissUnitStudent]' [Query]
		UNION
		SELECT 'OneEBS-Unit' [PartitionName], 'Units' [Table], 'SELECT * FROM [compliance].[vwAvetmissUnit]' [Query]
		UNION
		SELECT 'OneEBS-UnitOffering' [PartitionName], 'Unit Offering' [Table], 'SELECT * FROM [compliance].[vwAvetmissUnitOffering]' [Query]
		UNION
		SELECT 'ReportingPeriod' [PartitionName], 'Report Date' [Table], 'SELECT * FROM [compliance].[vwAvetmissUnitReportingPeriod]' [Query]
		UNION
		SELECT 'Location' [PartitionName], 'Location' [Table], 'SELECT * FROM [compliance].[vwAvetmissUnitLocation]' [Query]
	) u


	UNION
	SELECT 'UnitEnrolment' [ModelName], [PartitionName], 'Unit Enrolments' [Table], [Query], [Grouping], [ReportingPeriodKey], [FromDate], [ToDate] FROM CTE_unit
	UNION
	SELECT 'UnitEnrolment' [ModelName], [PartitionName], 'Course Enrolments' [Table], [Query], [Grouping], [ReportingPeriodKey], [FromDate], [ToDate] FROM CTE_unitCourse


)
SELECT
*
,REPLACE(REPLACE(REPLACE(REPLACE('{ "createOrReplace": {  "object":{ "database": "$DATABASE$", "table": "$TABLE$", "partition": "$PARTITION$" }, "partition": { "name": "$PARTITION$", "source": { "query": "$QUERY$", "dataSource": "SqlServer" } } } }' ,'$PARTITION$', [PartitionName]) ,'$QUERY$', [Query]) ,'$DATABASE$', [ModelName]) ,'$TABLE$', [Table]) [CreateTMSL]
,REPLACE(REPLACE(REPLACE('{ "refresh": { "type": "full", "objects": [ { "database": "$DATABASE$", "table": "$TABLE$", "partition": "$PARTITION$" } ] } }','$PARTITION$', [PartitionName]),'$DATABASE$', [ModelName]) ,'$TABLE$', [Table]) [ProcessTMSL]
,REPLACE(REPLACE('{ "type": "full", "objects": [ { "table": "$TABLE$", "partition": "$PARTITION$" } ] }', '$TABLE$', [Table]), '$PARTITION$', [PartitionName]) [RefreshCommand]
,RANK() OVER (PARTITION BY [ModelName]  ORDER BY IIF([Grouping]='Dimensions', 0, 1), [Grouping], [PartitionName]) [ProcessOrder]
FROM UNION_CTE 
--WHERE CONVERT(DATETIME, CONVERT(DATETIMEOFFSET, GETDATE()) AT TIME ZONE 'AUS Eastern Standard Time') BETWEEN [FromDate] AND [ToDate]
--WHERE '2020-12-14' BETWEEN [FromDate] AND [ToDate]