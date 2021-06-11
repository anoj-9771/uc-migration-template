
CREATE PROCEDURE [CTL].[CuratedLogStart] (
	@BatchExecutionLogID bigint,
	@TaskExecutionLogID bigint,
	@SubjectArea varchar(255),
	@Project varchar(255),
	@StartPeriod datetime,
	@EndPeriod datetime)
AS

--Reset the previous status to Terminated if they were unfinished
UPDATE CTL.ControlCuratedManifest
SET LoadStatus = 'TERMINATED'
WHERE SubjectArea = @SubjectArea
	AND Project = @Project
	AND LoadStatus = 'STARTED'

--Create new log entry on the Manifest table
INSERT INTO CTL.ControlCuratedManifest (
	BatchExecutionLogID
	,TaskExecutionLogID
	,SubjectArea
	,Project
	,StartPeriod
	,EndPeriod
	,LoadStatus
	,StartTimestamp
	,EndTimeStamp)
VALUES (
	@BatchExecutionLogID
	,@TaskExecutionLogID
	,@SubjectArea
	,@Project
	,@StartPeriod
	,@EndPeriod
	,'STARTED'
	,[CTL].[udf_GetDateLocalTZ]()
	,NULL)