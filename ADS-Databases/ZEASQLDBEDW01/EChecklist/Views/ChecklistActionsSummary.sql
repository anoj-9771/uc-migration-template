
Create View [eChecklist].[ChecklistActionsSummary] as 
select [InstanceID],
	   [No_Checklists_Completed],
	   [No_Checklists_Cancelled]
from [tsEChecklist].[ChecklistActionsSummary] a