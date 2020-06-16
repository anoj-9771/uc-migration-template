-- =============================================
-- Author:		Stephen Lundall
-- Create date: 4/11/2019
-- Description:	Function to return a table of dataset execute limits
-- =============================================
CREATE FUNCTION [CTL].[udf_GetDataSetExeLimit]
(	
	@WorkerId bigint
)
RETURNS TABLE 
AS
RETURN 
(
	with cte_DSQueuing_Source as (
			select distinct t.TaskId, t.WorkerId, d.DataStoreId, d.Limit
			from ctl.QueueMeta q
				join ctl.Task t on q.TaskId = t.TaskId
				join ctl.DataSet ds on t.SourceId = ds.DataSetId
				join ctl.DataStore d on ds.DataStoreId = d.DataStoreId
			
		),  cte_DSQueuing_Target as (
			select distinct t.TaskId, t.WorkerId, d.DataStoreId, d.Limit
			from ctl.QueueMeta q
				join ctl.Task t on q.TaskId = t.TaskId
				join ctl.DataSet ds on t.TargetId = ds.DatasetId
				join ctl.DataStore d on ds.DataStoreId = d.DataStoreId
		),cte_UtilisedDS_Source as (
			select a.WorkerId, a.DataStoreId, sum(a.Counts) DSWorkers
			from (
				select t.WorkerId, ds.dataStoreId, case when q.status = 1 then 1 else 0 end Counts
				from ctl.QueueMeta q
				join ctl.Task t on q.TaskId = t.TaskId
				join ctl.DataSet ds on t.SourceId = ds.DataSetId
				join ctl.DataStore d on ds.DataStoreId = d.DataStoreId
			) a
			group by a.WorkerId, a.DataStoreId
		),cte_UtilisedDS_Target as (
			select a.WorkerId, a.DataStoreId, sum(a.Counts) DSWorkers
			from (
				select t.WorkerId, ds.dataStoreId, case when q.status =1 then 1 else 0 end Counts
				from ctl.QueueMeta q
				join ctl.Task t on q.TaskId = t.TaskId
				join ctl.DataSet ds on t.TargetId = ds.DatasetId
				join ctl.DataStore d on ds.DataStoreId = d.DataStoreId
			) a
			group by a.WorkerId, a.DataStoreId
		)

		select row_number() over(order by DataStoreId) RowNum,a.*
			from ( 
				select distinct qgs.DataStoreId, 
				case when qgs.Limit = 0 then 1000000 else qgs.Limit end Source_Limit, 
				case when qgt.Limit = 0 then 1000000 else qgt.Limit end Target_Limit, 
				uds.DSWorkers Source_DSWorkers, 
				udt.DSWorkers Target_DSWorkers
				from cte_DSQueuing_Source qgs
				left join cte_DSQueuing_Target qgt on qgs.WorkerId = qgt.WorkerId
				left join cte_UtilisedDS_Source uds on qgs.WorkerId = uds.WorkerId
				left join cte_UtilisedDS_Target udt on qgs.WorkerId = udt.WorkerId
				where qgs.WorkerId = @WorkerId
			)a
)