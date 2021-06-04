---- =============================================
---- Author:      Stephen Lundall
---- Create Date: 31/10/2019
---- Description: Get a project level execution plan
----==============================================

create     function [CTL].[udf_getProjectDependents](@projectId bigint)
returns table
as	
return 
WITH tmp(ProjectId,DataItem, ProjectDependency) AS
(
    SELECT
        prc.ProjectId,
        LEFT(replace(ProjectDependency,' ',''), CHARINDEX(',', replace(ProjectDependency,' ','') + ',') - 1),
        STUFF(replace(ProjectDependency,' ',''), 1, CHARINDEX(',', replace(ProjectDependency,' ','')), '')
    FROM ctl.Process prc
	join ctl.Project prj on prc.ProjectId = prj.ProjectId
	where 1=1
	and prc.Enabled = 1
	and prj.Enabled = 1
    UNION all

    SELECT
        ProjectId,
        LEFT(replace(ProjectDependency,' ',''), CHARINDEX(',', replace(ProjectDependency,' ','') + ',') - 1),
        STUFF(replace(ProjectDependency,' ',''), 1, CHARINDEX(',', replace(ProjectDependency,' ','') + ','), '')
    FROM tmp
    WHERE
        ProjectDependency > ''
),

cte_Projects as (
	SELECT distinct
		ProjectId,
		DataItem
	FROM tmp
),
cte_ProjectPlan as(
	select distinct a.ProjectId,
		b.ProjectId _1,
		c.ProjectId _2,
		d.ProjectId _3,
		e.ProjectId _4,
		f.ProjectId _5,
		g.ProjectId _6,
		h.ProjectId _7,
		i.ProjectId _8
	from cte_Projects a
	left join cte_Projects b on b.DataItem = a.ProjectId
	left join cte_Projects c on c.DataItem = b.ProjectId
	left join cte_Projects d on d.DataItem = c.ProjectId
	left join cte_Projects e on e.DataItem = d.ProjectId
	left join cte_Projects f on f.DataItem = e.ProjectId
	left join cte_Projects g on g.DataItem = f.ProjectId
	left join cte_Projects h on h.DataItem = g.ProjectId
	left join cte_Projects i on i.DataItem = h.ProjectId
	where a.ProjectId = @projectId
)
select distinct ProjectId, Dependents--, cast(replace(DependentsId, '_','') as int)ExecSequence
from cte_ProjectPlan 
unpivot
(
	Dependents
	for DependentsId in (_1,_2,_3,_4,_5,_6,_7,_8)
)unpiv