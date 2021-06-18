CREATE View [EChecklist].[EChecklistExceptionReport]
as
select CheckListId,
	   InstanceId,
	   TaskNo,
	   TaskName,
	   StartDate,
	   EndDate,
	   [Year],
	   [Month],
	   [DAY],
	   [_DLCuratedZoneTimeStamp] as 'DataRefreshDate'
from [tsEChecklist].[ChecklistActionsInvalidRecords]