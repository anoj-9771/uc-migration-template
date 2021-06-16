-- =============================================
-- Author:		Stephen Lundall
-- Create date: 4/11/2019
-- Description:	Function to return a table of datastore execute limits
-- =============================================
CREATE FUNCTION [CTL].[udf_GetDataStoreExeLimit]
(	
	@WorkerId bigint
)
RETURNS TABLE 
AS
RETURN 
(
	--Select limits for worker/datastore pairs in QueueMeta for sources and targets
	with cte_DSQueuing_Source as (
			select t.WorkerId, 
				ds.DataStoreId SourceDataStoreId, 
				dt.DataStoreId TargetDataStoreId, 
				ds.Limit Source_Limit, 
				dt.Limit Target_Limit, 
				count(qm_b.TaskId) DS_AllocatedUnits
			from ctl.QueueMeta qm_a
				left join (select TaskId from ctl.QueueMeta where Status in (1,2)) qm_b on qm_a.TaskId = qm_b.TaskId 
				join ctl.Task t on qm_a.TaskId = t.TaskId
				join ctl.DataSet dss on t.SourceId = dss.DataSetId
				join ctl.DataSet dst on t.TargetId = dst.DataSetId
				join ctl.DataStore ds on dss.DataStoreId = ds.DataStoreId
				join ctl.DataStore dt on dst.DataStoreId = dt.DataStoreId
			where Status < 3
			group by ds.DataStoreId, dt.DataStoreId, WorkerId, ds.Limit, dt.Limit
		)
		select row_number() over(order by SourceDataStoreId, TargetDataStoreId) Row_Num,a.*
			from ( 
				select distinct SourceDataStoreId,  
				TargetDataStoreId,
				case when Source_Limit = 0 then 1000000 else Source_Limit end Source_Limit, 
				case when Target_Limit = 0 then 1000000 else Target_Limit end Target_Limit, 
				isnull(qgs.DS_AllocatedUnits, 0) DS_AllocatedUnits
				from cte_DSQueuing_Source qgs
				where qgs.WorkerId = @WorkerId
			)a
)