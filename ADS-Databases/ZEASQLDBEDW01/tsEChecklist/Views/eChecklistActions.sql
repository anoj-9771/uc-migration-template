CREATE VIEW [EChecklist].[checklist_actions]
	AS SELECT * FROM [edw].[eChecklist_dbo_checklists_actions]
WHERE        (_RecordCurrent = 1) AND (_RecordDeleted = 0)
