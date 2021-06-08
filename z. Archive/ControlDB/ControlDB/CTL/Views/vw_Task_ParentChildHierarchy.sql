create   view ctl.vw_Task_ParentChildHierarchy as 
	select distinct --count(*)
			prjc.ProjectId ChildProjectId, 
			prjc.Enabled ChildProjectEnabled,
			prcc.ProcessId ChildProcessId,
			prcc.Enabled ChildProcessEnabled,
			tskc.TaskId ChildTaskId, 
			tskc.Rank ChildRank,
			prjp.ProjectId ParentProjectId, 
			prjp.Enabled ParentProjectEnabled,
			prcp.ProcessId ParentProcessId,
			prcp.Enabled ParentProcessEnabled,
			tskp.TaskId ParentTaskId,
			tskp.Rank ParentRank
		--select * 
		from ctl.Process prcp
		left outer join ctl.Process prcc on prcp.ProcessId = prcc.ProcessDependency 
		or  prcp.ProjectId = prcc.ProjectDependency		
		join ctl.Task tskc on tskc.ProcessId = prcc.ProcessId
		join ctl.Task tskp on tskp.ProcessId = prcp.ProcessId
		join ctl.Project prjc on prcc.ProjectId = prjc.ProjectId
		join ctl.Project prjp on prcp.ProjectId = prjp.ProjectId
		--order by ParentProcessId