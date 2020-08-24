-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/10/2019
-- Description: Returns the Project Task count as count or 0 if project is taking 3 time as long to complete 
-- and updates log status to incomplete if 
--==============================================
CREATE     Procedure [CTL].[sp_checkBatchStatus] @BatchId bigint--, @minBatchSize int
As
begin
	declare @maxRank table (ProcessId bigint, MaxRank int)
	declare @delay varchar(5)
	declare @batchSize int = (select [CTL].[udf_getBatchDelay]())
	insert into @maxRank 
		select ProcessId, max(rank) 
		from ctl.Task tsk
		group by ProcessId	

	declare @tskCount int = (
		select distinct count(*)
		from ctl.QueueMeta	qm
		join ctl.Task tsk on tsk.TaskId = qm.TaskId
		join @maxRank mr on tsk.ProcessId = mr.ProcessId
		where BatchId = @BatchId 
		and qm.status < 2 
		or (
			qm.status = 2 and 
			tsk.rank < mr.MaxRank
		)
		--or qm.TaskId not in (
		--						select TaskId from ctl.Task tsk
		--						join (
		--							select ProcessId, max(Rank) Rank 
		--							from ctl.Task group by ProcessId
		--						) max_tsk on tsk.ProcessId = max_tsk.ProcessId
		--						where tsk.Rank = max_tsk.Rank
		--					)
	)
	declare @avgPrjDuration int = (
		select avg(datediff(second, tl.InitialLogTime, tl.EndLogTime))
		from ctl.TaskLog tl
		join ctl.Task tsk on tsk.TaskId = tl.TaskId
		where tl.BatchId <> @BatchId
		)
	declare @curPrjDuration int = (
		select avg(datediff(second, tl.InitialLogTime,[CTL].[udf_getWAustDateTime](getdate())))
		from ctl.TaskLog tl
		join ctl.Task tsk on tsk.TaskId = tl.TaskId
		where  tl.BatchId = @BatchId
		and tl.Status in ('Failed' , 'Succeeded')
	)
		
	declare @minBatchSize int = 1
	set @batchSize = (select [CTL].[udf_getBatchDelay]())
	
	while @batchSize <  @minBatchSize and  @tskCount > 0 
	begin
		waitfor delay '00:00:01'
		set @batchSize = [CTL].[udf_getBatchDelay]()
		set @tskCount =  (
			select distinct count(*)
			from ctl.QueueMeta	qm
			join ctl.Task tsk on tsk.TaskId = qm.TaskId
			join @maxRank mr on tsk.ProcessId = mr.ProcessId
			where BatchId = @BatchId 
			and qm.status < 2 
			or (
				qm.status = 2 and 
				tsk.rank < mr.MaxRank
			)
		)
	end
	select @tskCount TaskCount	
end