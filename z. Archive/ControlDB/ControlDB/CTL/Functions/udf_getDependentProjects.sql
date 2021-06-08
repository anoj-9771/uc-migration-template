---- =============================================
---- Author:      Stephen Lundall
---- Create Date: 31/10/2019
---- Description: Get a project level dependents
----==============================================

create   function [CTL].[udf_getDependentProjects](@projectId bigint)
returns varchar(max)
as	
begin
	return (select ProjectPlan from [CTL].[udf_getProjectExecPlan](@projectId))
end