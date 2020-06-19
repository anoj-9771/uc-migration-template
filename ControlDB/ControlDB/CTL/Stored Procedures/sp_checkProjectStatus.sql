-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/10/2019
-- Description: Returns the Project Task count as count or 0 if project is taking 3 time as long to complete 
-- and updates log status to incomplete if 
--==============================================
CREATE       Procedure [CTL].[sp_checkProjectStatus] @ProjectId int, @BatchId bigint
As
begin
	declare @prjCount int = (
		select distinct count(*)
		from ctl.QueueMeta	qm
		join ctl.Task tsk on tsk.TaskId = qm.TaskId
		join ctl.Process prc on tsk.ProcessId = prc.ProcessId
		where ProjectId = @ProjectId and BatchId = @BatchId 
		and qm.status < 3
	)
	declare @avgPrjDuration int = (
		select avg(datediff(second, tl.InitialLogTime, tl.EndLogTime))
		from ctl.TaskLog tl
		join ctl.Task tsk on tsk.TaskId = tl.TaskId
		join ctl.Process prc on tsk.ProcessId = prc.ProcessId
		where prc.ProjectId = @ProjectId and tl.BatchId <> @BatchId
		)
	declare @curPrjDuration int = (
		select avg(datediff(second, tl.InitialLogTime,[CTL].[udf_getWAustDateTime](getdate())))
		from ctl.TaskLog tl
		join ctl.Task tsk on tsk.TaskId = tl.TaskId
		join ctl.Process prc on tsk.ProcessId = prc.ProcessId
		where (prc.ProjectId = @ProjectId and tl.BatchId = @BatchId)
		and tl.Status in ('Failed' , 'Succeeded')
	)
	
	if @prjCount >= 0 
		begin 
			select @prjCount TaskCount
		end
	if @curPrjDuration > (3 * @avgPrjDuration)
	begin
		begin
			update tl
			set Status = 'Incomplete'
			from ctl.TaskLog tl
			join ctl.Task tsk on tl.TaskId = tsk.TaskId
			join ctl.Process prc on tsk.ProcessId = prc.ProcessId
			where prc.ProjectId = @ProjectId and tl.BatchId = @BatchId
			and tl.Status = 'Processing'
		end
		select 0 TaskCount
	end
end