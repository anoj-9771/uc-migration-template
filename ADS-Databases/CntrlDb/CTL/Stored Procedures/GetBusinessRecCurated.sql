CREATE PROCEDURE [CTL].[GetBusinessRecCurated] 
  (
	  @BusinessReconGroup varchar(255)
  )
AS
SELECT 
     recMstr.TargetQuery,
	   recLog.BusinessRecId,
	   recLog.CreatedBatchExecutionId,
	   recLog.CreatedTaskExecutionLogId     
  FROM (select *, row_number() over (partition by BusinessReconGroup, MeasureId, MeasureName order by CreatedDateTime desc) as CurrentRec
        from CTL.BusinessRecCurated
        where (CuratedPipelineRunID is null or BusinessRecResult is null)
        ) recLog,
       CTL.BusinessRecConfig recMstr
 where recLog.BusinessReconGroup = recMstr.BusinessReconGroup
   AND recLog.MeasureId = recMstr.MeasureId
   and recLog.MeasureName = recMstr.MeasureName
   AND recMstr.Enabled = 1
   AND recLog.CurrentRec = 1
   AND recMstr.BusinessReconGroup = @BusinessReconGroup


