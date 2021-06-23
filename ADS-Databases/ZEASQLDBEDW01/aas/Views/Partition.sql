



CREATE VIEW [aas].[Partition] AS 
SELECT 
[ModelName]
,[PartitionName]
,[Table]
,[Query]
,[Grouping]
,[ReportingPeriodKey]
,[FromDate]
,[ToDate]
,[CreateTMSL]
,[ProcessTMSL]
,[RefreshCommand]
,[ProcessOrder]
,1 AS [CubeSequence]
FROM [aas].[PartitionAllCourseEnrolment]
WHERE CONVERT(DATETIME, CONVERT(DATETIMEOFFSET, GETDATE()) AT TIME ZONE 'AUS Eastern Standard Time') BETWEEN [FromDate] AND [ToDate]

UNION

SELECT 
[ModelName]
,[PartitionName]
,[Table]
,[Query]
,[Grouping]
,[ReportingPeriodKey]
,[FromDate]
,[ToDate]
,[CreateTMSL]
,[ProcessTMSL]
,[RefreshCommand]
,[ProcessOrder]
,2 AS [CubeSequence]
FROM [aas].[PartitionAllUnitEnrolment]
WHERE CONVERT(DATETIME, CONVERT(DATETIMEOFFSET, GETDATE()) AT TIME ZONE 'AUS Eastern Standard Time') BETWEEN [FromDate] AND [ToDate]