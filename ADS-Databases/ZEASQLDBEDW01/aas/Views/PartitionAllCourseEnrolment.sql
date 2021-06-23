


CREATE VIEW [aas].[PartitionAllCourseEnrolment] AS 

WITH CTE AS (
	SELECT 
	C.ReportingPeriodKey 
	,FORMATMESSAGE('%s%dQ%d - %d', IIF(LEFT(C.ReportingYear, 1)='F', 'FY', ''), IIF(LEFT(C.ReportingYear, 1)='C', C.ReportingYearNo, C.SpecialReportingYear), C.ReportingQuarter, C.ReportingPeriodKey) [PartitionName]
	,FORMATMESSAGE('SELECT * FROM [compliance].[vwAvetmissCourseEnrolment] WHERE ReportingYear = ''%s'' AND REPORT_DATE = %d',C.ReportingYear, C.ReportingPeriodKey) [Query]
	,'CurrentWeekly' [Grouping]
	,P.ReportingPeriodKey [PreviousReportingPeriodKey]
	,FORMATMESSAGE('%s%dQ%d - %d', IIF(LEFT(C.ReportingYear, 1)='F', 'FY', ''), IIF(LEFT(P.ReportingYear, 1)='C', P.ReportingYearNo, P.SpecialReportingYear), P.ReportingQuarter, P.ReportingPeriodKey) [PreviousPartitionName]
	,FORMATMESSAGE('SELECT * FROM [compliance].[vwAvetmissCourseEnrolment_ReprocessOldPrada] WHERE ENROLMENT_YEAR = %s AND REPORT_DATE = %d', RIGHT(P.SpecialReportingYear, 4), P.ReportingPeriodKey) [PreviousQuery]
	,'PreviousWeekly' [PreviousGrouping]
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
			) c  --[compliance].[AvetmissReportingPeriod] C 
	JOIN [compliance].[AvetmissReportingPeriod] P ON P.ReportingPeriodKey = C.ReportingPeriodKeyLastYear AND P._RecordCurrent = 1
	WHERE 
	C._RecordCurrent = 1
	
			
),
UNION_CTE AS (
	SELECT 'CourseEnrolment' [ModelName], [PartitionName], 'Enrolment' [Table], [Query], [Grouping], [ReportingPeriodKey], [FromDate], [ToDate] FROM CTE
	UNION
	SELECT 'CourseEnrolment' [ModelName], [PreviousPartitionName], 'Enrolment' [Table], [PreviousQuery], [PreviousGrouping], [PreviousReportingPeriodKey], [FromDate], [ToDate] FROM CTE
	UNION
	SELECT 'CourseEnrolment' [ModelName], [PartitionName], [Table], [Query], 'Dimensions' [Grouping], NULL [ReportingPeriodKey], '1900-01-01' [FromDate], '9999-12-31' [ToDate]
	FROM (
		SELECT 'OneEBS-CurrentCourseMapping' [PartitionName], 'Current Course Mapping' [Table], 'SELECT * FROM [compliance].[vwAvetmissCurrentCourseMapping]' [Query]
		UNION
		SELECT 'OneEBS-Learner' [PartitionName], 'Learner' [Table], 'SELECT * FROM [compliance].[vwAvetmissStudent]' [Query]
		UNION
		SELECT 'OneEBS-Product' [PartitionName], 'Product' [Table], 'SELECT * FROM [compliance].[vwAvetmissCourse]' [Query]
		UNION
		SELECT 'OneEBS-ProductOffering' [PartitionName], 'Product Offering' [Table], 'SELECT * FROM [compliance].[vwAvetmissCourseOffering]' [Query]
		UNION
		SELECT 'ReportingPeriod' [PartitionName], 'Report Date' [Table], 'SELECT * FROM [compliance].[vwAvetmissReportingPeriod]' [Query]
		UNION
		SELECT 'Location' [PartitionName], 'Location' [Table], 'SELECT * FROM [compliance].[vwAvetmissLocation]' [Query]
	) T
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