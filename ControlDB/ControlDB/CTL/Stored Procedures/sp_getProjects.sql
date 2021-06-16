-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/10/2019
-- Description: Get a list of task to process every x mins as specified in ADF trigger
--==============================================
CREATE       Procedure [CTL].[sp_getProjects] (@json varchar(max) = null,@Streaming bit = null)
As

--insert into dbo.OutputTest (val) values (@json)
declare @ProjectTime table (ProjectName varchar(250), ExecTime datetime2 )
insert into @ProjectTime select [key] as ProjectName, [value] as ExecTime from openjson(@json)
if @Streaming = 1 
begin
	-- Get all enabled Origin Projects
	with cte_projetcs as (
		select  distinct prj.ProjectId, crn.ExecTime ExecTime, ctl.udf_getWAustDateTime(getdate()) CurrentTime
		from ctl.Project prj 
		join @ProjectTime crn on prj.Name = crn.ProjectName
		where prj.Enabled = 1
		and prj.Streaming = 0
	)
	select *,
	convert(bigint, replace([CTL].[udf_getFileDateHierarchy]('Second',ExecTime), '/','')) BatchId
	from cte_projetcs
	where Case When CurrentTime >= ExecTime And CurrentTime <= DATEADD(MI, 10, ExecTime)
			Then 1
			Else 0
		End = 1
	and ProjectId not in (select distinct prc.ProjectId 
		from ctl.QueueMeta_Stream qms 
		join ctl.Task tsk on qms.TaskId = tsk.TaskId
		join ctl.Process prc on tsk.ProcessId = prc.ProcessId)
	and projectId not in (select distinct b.ProjectId from ctl.Process a
		join ctl.Project prj on a.ProjectId = prj.ProjectId, ctl.Process b
		where a.ProjectId in (select Value from  [CTL].[udf_string_split](b.ProjectDependency,','))
		and prj.Enabled = 1)
end
if @Streaming is null or @Streaming = 0
begin
	-- Get all enabled Origin Projects
	with cte_projetcs as (
		select  distinct prj.ProjectId, crn.ExecTime ExecTime, ctl.udf_getWAustDateTime(getdate()) CurrentTime
		from ctl.Project prj 
		join @ProjectTime crn on prj.Name = crn.ProjectName
		where prj.Enabled = 1
		and prj.Streaming = 0
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
	and projectId not in (select distinct b.ProjectId from ctl.Process a
		join ctl.Project prj on a.ProjectId = prj.ProjectId, ctl.Process b
		where a.ProjectId in (select Value from  [CTL].[udf_string_split](b.ProjectDependency,','))
		and prj.Enabled = 1)
end
--begin
-- with cte_projetcs as (
--		select  distinct prj.ProjectId,ctl.udf_CronToDatetime(coalesce(prj.Schedule, '1 * * * * *')) ExecTime , 
--			ctl.udf_getWAustDateTime(getdate()) CurrentTime
--		from ctl.Process prc
--		join ctl.Project prj on prc.ProjectId = prj.ProjectId
--		where prj.Enabled = 1
--		and prj.Streaming = 1
--)
--		select *,
--		convert(bigint, replace([CTL].[udf_getFileDateHierarchy]('Second',ExecTime), '/','')) BatchId
--		from cte_projetcs
--		where Case When CurrentTime >= ExecTime And CurrentTime <= DATEADD(MI, 10, ExecTime)
--				Then 1
--				Else 0
--			End = 1
--		and ProjectId not in (select distinct prc.ProjectId 
--			from ctl.QueueMeta qm 
--			join ctl.Task tsk on qm.TaskId = tsk.TaskId
--			join ctl.Process prc on tsk.ProcessId = prc.ProcessId)
--end
--else 
--begin
-- with cte_projetcs as (
--		select  distinct prj.ProjectId,ctl.udf_CronToDatetime(coalesce(prj.Schedule, '1 * * * * *')) ExecTime , 
--			ctl.udf_getWAustDateTime(getdate()) CurrentTime
--		from ctl.Process prc
--		join ctl.Project prj on prc.ProjectId = prj.ProjectId
--		where prj.Enabled = 1
--		and prj.Streaming <> 1
--)
--		select *,
--		convert(bigint, replace([CTL].[udf_getFileDateHierarchy]('Second',ExecTime), '/','')) BatchId
--		from cte_projetcs
--		where Case When CurrentTime >= ExecTime And CurrentTime <= DATEADD(MI, 10, ExecTime)
--				Then 1
--				Else 0
--			End = 1
--		and ProjectId not in (select distinct prc.ProjectId 
--			from ctl.QueueMeta qm 
--			join ctl.Task tsk on qm.TaskId = tsk.TaskId
--			join ctl.Process prc on tsk.ProcessId = prc.ProcessId)
--end
--go