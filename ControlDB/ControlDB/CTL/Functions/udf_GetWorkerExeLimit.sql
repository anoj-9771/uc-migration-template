-- =============================================
-- Author:		Stephen Lundall
-- Create date: 4/11/2019
-- Description:	Function to return a table of worker execute limits
-- =============================================
CREATE FUNCTION [CTL].[udf_GetWorkerExeLimit]
(	
)
RETURNS TABLE 
AS
RETURN 
(
		with cte_wQueuing as (
			select row_number() over(order by a.WorkerId) RowNum,*
			from (
				select distinct 
				w.WorkerId, w.Limit
				from ctl.QueueMeta q
				join ctl.Task t on q.TaskId = t.TaskId
				join ctl.Worker w on t.WorkerId = w.WorkerId
			)a
		),  cte_UtilisedWorkers as (
			select a.WorkerId, sum(a.Counts) Workers
			from (
				select w.WorkerId, case when q.status in (1,2) then 1 else 0 end Counts
				from ctl.QueueMeta q
				join ctl.Task t on q.TaskId = t.TaskId
				join ctl.Worker w on t.WorkerId = w.WorkerId
			)a
			group by a.workerId		
		)
				select qg.RowNum, 
					qg.WorkerId, 
					case when qg.Limit = 0 then 1000000 else qg.Limit end Limit, 
					uw.Workers, 
					--Added WorkerBatchSize: Stephen.;L
					case when qg.Limit <= uw.Workers then 0 else (qg.Limit - uw.Workers) end WorkerBatchSize
			from cte_wQueuing qg
			join cte_UtilisedWorkers uw on qg.WorkerId = uw.WorkerId

)