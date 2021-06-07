---- =============================================
---- Author:      Stephen Lundall
---- Create Date: 31/10/2019
---- Description: Get a project level execution plan
----==============================================

CREATE   function [CTL].[udf_getProjectExecPlan](@projectId bigint)
returns table
as	
return 
WITH tmp(ProjectId,DataItem, ProjectDependency) AS
(
    SELECT
        ProjectId,
        LEFT(replace(ProjectDependency,' ',''), CHARINDEX(',', replace(ProjectDependency,' ','') + ',') - 1),
        STUFF(replace(ProjectDependency,' ',''), 1, CHARINDEX(',', replace(ProjectDependency,' ','')), '')
    FROM ctl.Process
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
select row_number()over(order by len(ProjectPlan) - len(replace(ProjectPlan, ',', '')) desc) RowId, *
from (
	select distinct a.ProjectId,
		concat_ws(',',
		   b.ProjectId,
		   c.ProjectId,
		   d.ProjectId,
		   e.ProjectId
		   --f.ProjectId,
		   --g.ProjectId
		   --h.ProjectId,
		   --i.ProjectId
		) ProjectPlan
	from cte_Projects a
	left join cte_Projects b on b.DataItem = a.ProjectId
	left join cte_Projects c on c.DataItem = b.ProjectId
	left join cte_Projects d on d.DataItem = c.ProjectId
	left join cte_Projects e on e.DataItem = d.ProjectId
	--left join cte_Projects f on f.DataItem = e.ProjectId
	--left join cte_Projects g on g.DataItem = f.ProjectId
	--left join cte_Projects h on h.DataItem = g.ProjectId
	--left join cte_Projects i on i.DataItem = h.ProjectId
	where a.ProjectId = @projectId
	)a
)
select ProjectId, ProjectPlan from cte_ProjectPlan where RowId = 1