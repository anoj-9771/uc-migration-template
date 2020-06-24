-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/10/2019
-- Description: Get a list of task to process every x mins as specified in ADF trigger
--==============================================
CREATE       Procedure [CTL].[sp_getProjects]
As

--begin
--	with cte_items as(
--		select distinct prj.*,
--			ctl.udf_CronToDatetime(coalesce(prj.Schedule, '1 * * * * *')) ExecTime, 
--			pd.ProjectId ChildId--, prcp.ProjectId
--		from ctl.Project prj
--		join ctl.Process prc on prj.ProjectId = prc.ProjectId
--		left join ctl.ProjectDependency pd on prc.ProcessId = pd.ProcessId
--		where prj.Enabled = 1
--		and prc.Enabled = 1
--	), cte_status as(
--		select distinct i.*, sts.ProjectStatus
--		from cte_items i
--		join [CTL].[udf_getStatus]() sts on i.ProjectId = sts.ProjectId   
--	), cte_statusp as (
--		select stsp.ProjectId, stsp.ChildId, stsp.ExecTime, stsp.ProjectStatus, stsc.ProjectStatus ParentStatus
--		from cte_status stsp
--		left join cte_status stsc on stsp.ProjectId = stsc.ChildId 
--	), cte_Projects as(
--		select distinct p.*, 
--		ctl.udf_getWAustDateTime(getdate()) CurrentTime
--		from cte_statusp p
--		join ctl.Project prj on p.ProjectId = prj.ProjectId
--		join ctl.Process prc on prj.ProjectId = prc.ProjectId
--		where (ParentStatus >= 1 and p.ProjectId not in (select distinct ProjectId from [CTL].[udf_getStatus]() where BatchId is not null) )
--		 or ParentStatus is null
--	), cte_pc as (
--		select p.ProjectId, i.ProjectId ParentId
--		from cte_items i
--		left join cte_items p on i.ChildId = p.ProjectId
--	)

--	Select distinct i.ProjectId, i.ExecTime, 
--		case when i.ParentStatus is null 
--			then convert(bigint, replace([CTL].[udf_getFileDateHierarchy]('Second',i.ExecTime), '/',''))
--		else (select top(1) BatchId from [CTL].[udf_getStatus]() f where f.ProjectId = pc.ParentId and f.batchId is not null) end	BatchId
--	From cte_Projects i
--	left join cte_pc pc on i.ProjectId = pc.ProjectId
--	Where Case When CurrentTime >= ExecTime And CurrentTime <= DATEADD(MI, 10, ExecTime)
--			Then 1
--			Else 0
--		End = 1
--	and i.ProjectId not in (select distinct prc.ProjectId 
--		from ctl.QueueMeta qm 
--		join ctl.Task tsk on qm.TaskId = tsk.TaskId
--		join ctl.Process prc on tsk.ProcessId = prc.ProcessId)
--end


begin
 with cte_projetcs as (
		select  distinct prj.ProjectId,ctl.udf_CronToDatetime(coalesce(prj.Schedule, '1 * * * * *')) ExecTime , 
			ctl.udf_getWAustDateTime(getdate()) CurrentTime
		from ctl.Process prc
		join ctl.Project prj on prc.ProjectId = prj.ProjectId
		where prj.Enabled = 1
)
		select *,
		convert(bigint, replace([CTL].[udf_getFileDateHierarchy]('Second',ExecTime), '/','')) BatchId
		from cte_projetcs
		where Case When CurrentTime >= ExecTime And CurrentTime <= DATEADD(MI, 10, ExecTime)
				Then 1
				Else 0
			End = 1
		and ProjectId not in (select distinct prc.ProjectId 
			from ctl.QueueMeta qm 
			join ctl.Task tsk on qm.TaskId = tsk.TaskId
			join ctl.Process prc on tsk.ProcessId = prc.ProcessId)
end