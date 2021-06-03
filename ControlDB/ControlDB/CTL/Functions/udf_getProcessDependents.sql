---- =============================================
---- Author:      Stephen Lundall
---- Create Date: 31/10/2019
---- Description: Get a project level execution plan
----==============================================

create     function [CTL].[udf_getProcessDependents](@processId bigint)
returns table
as	
return 
WITH tmp(ProcessId,DataItem, ProcessDependency) AS
(
    SELECT
        prc.ProcessId,
        LEFT(replace(ProcessDependency,' ',''), CHARINDEX(',', replace(ProcessDependency,' ','') + ',') - 1),
        STUFF(replace(ProcessDependency,' ',''), 1, CHARINDEX(',', replace(ProcessDependency,' ','')), '')
    FROM ctl.Process prc
	join ctl.Project prj on prc.ProjectId = prj.ProjectId
	where 1=1
	and prc.Enabled = 1
	--and prj.Enabled = 1
    UNION all

    SELECT
        ProcessId,
        LEFT(replace(ProcessDependency,' ',''), CHARINDEX(',', replace(ProcessDependency,' ','') + ',') - 1),
        STUFF(replace(ProcessDependency,' ',''), 1, CHARINDEX(',', replace(ProcessDependency,' ','') + ','), '')
    FROM tmp
    WHERE
        ProcessDependency > ''
),

cte_Process as (
	SELECT distinct
		ProcessId,
		DataItem
	FROM tmp
),
cte_ProcessPlan as(
	select distinct 
		a.ProcessId,
		b.ProcessId _1,
		c.ProcessId _2,
		d.ProcessId _3,
		e.ProcessId _4,
		f.ProcessId _5,
		g.ProcessId _6,
		h.ProcessId _7,
		i.ProcessId _8
	from cte_Process a
	left join cte_Process b on b.DataItem = a.ProcessId
	left join cte_Process c on c.DataItem = b.ProcessId
	left join cte_Process d on d.DataItem = c.ProcessId
	left join cte_Process e on e.DataItem = d.ProcessId
	left join cte_Process f on f.DataItem = e.ProcessId
	left join cte_Process g on g.DataItem = f.ProcessId
	left join cte_Process h on h.DataItem = g.ProcessId
	left join cte_Process i on i.DataItem = h.ProcessId
	where a.ProcessId = @processId
)
select distinct ProcessId, Dependents--, cast(replace(DependentsId, '_','') as int)ExecSequence
from cte_ProcessPlan 
unpivot
(
	Dependents
	for DependentsId in (_1,_2,_3,_4,_5,_6,_7,_8)
)unpiv