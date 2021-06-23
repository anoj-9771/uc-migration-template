Create View [eChecklist].[ChecklistTemplates] as 
select [CheckListId],
	[Version],
	[Sponsor],
	[CheckListAcronym],
	[CheckListName],
	[CheckListSponsors],
	[WULPublicName],
	[ImplementationDate],
	[CC_LastUpdated],
	[ReviewDate]
from [tsEChecklist].[ChecklistTemplates] ct
