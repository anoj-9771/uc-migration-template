CREATE   view [EChecklist].[EChecklistOfferingNewSummary]
as select top 100 percent 
	   [Region] = [wul].[WULPublicName]
	 , [eCheckList] = 'OFFERING-NEW (CLID='+cast([cl].[CheckListId] as varchar)+')'
	 , [Task No] = [task].[TaskNo]
	 , [Task Owner] = [task].[TaskOwnerLong]
	 , [Tasks Inflight] =
		  (
			  select 
				  count(1)
			  from [edw].[eChecklist_dbo_checklists_actions] as [act]
			  where [act].[TaskStatusId] = 2
					and [act].[TaskNo] = [task].[TaskNo]
					and [act].[InstanceId] in
											  (
												  select 
													  [inst].[InstanceId]
												  from [edw].[eChecklist_dbo_checklists_instances] as [inst]
												  where [cl].[CheckListId] = [inst].[CheckListId]
														and [inst].[HighestStatusId] = 1
														and [inst].[InstanceName] like '%~T3~2020~%~YES'
														and [inst].[_RecordCurrent]= 1
														and [inst].[_RecordDeleted]= 0
											  )
					and [act].[_RecordCurrent] = 1
					and [act].[_RecordDeleted] = 0
		  )
	 , [Tasks Completed] =
		  (
			  select 
				  count(1)
			  from [edw].[eChecklist_dbo_checklists_actions] as [act]
			  where [act].[TaskStatusId] = 1
					and [act].[TaskNo] = [task].[TaskNo]
					and [act].[InstanceId] in
											  (
												  select 
													  [inst].[InstanceId]
												  from [edw].[eChecklist_dbo_checklists_instances] as [inst]
												  where [cl].[CheckListId] = [inst].[CheckListId]
														and [inst].[HighestStatusId] in ( 1, 3 )
														and [inst].[InstanceName] like '%~T3~2020~%~YES'
														and [inst].[_RecordCurrent] = 1
														and [inst].[_RecordDeleted] = 0
											  )
					and [act].[_RecordCurrent] = 1
					and [act].[_RecordDeleted] = 0
		  )
	 , [Offerings By Task Inflight] = IsNull(
		  (
			  select 
				  sum(try_cast(replace(replace(replace(right([inst].[InstanceName], 6), 'YES', ''), 'NO', ''), '~', '') as int))
			  from [edw].[eChecklist_dbo_checklists_instances] as [inst]
			  where [inst].[HighestStatusId] = 1
					and [cl].[CheckListId] = [inst].[CheckListId]
					and [inst].[InstanceId] in
											   (
												   select 
													   [act].[InstanceId]
												   from [edw].[eChecklist_dbo_checklists_actions] as [act]
												   where [act].[TaskNo] = [task].[TaskNo]
														 and [act].[TaskStatusId] = 2
														 and [act].[_RecordCurrent] = 1
														 and [act].[_RecordDeleted] = 0
											   )
					and try_cast(replace(replace(replace(right([inst].[InstanceName], 6), 'YES', ''), 'NO', ''), '~', '') as int) is not null
					and [inst].[InstanceName] like '%~T3~2020~%~YES'
					and [inst].[_RecordCurrent] = 1
					and [inst].[_RecordDeleted] = 0
		  ), 0)
	 , [Offerings Inflight] =
		  (
			  select 
				  sum(try_cast(replace(replace(replace(right([inst].[InstanceName], 6), 'YES', ''), 'NO', ''), '~', '') as int))
			  from [edw].[eChecklist_dbo_checklists_instances] as [inst]
			  where [inst].[HighestStatusId] = 1
					and [cl].[CheckListId] = [inst].[CheckListId]
					and try_cast(replace(replace(replace(right([inst].[InstanceName], 6), 'YES', ''), 'NO', ''), '~', '') as int) is not null
					and [inst].[InstanceName] like '%~T3~2020~%~YES'
					and [inst].[_RecordCurrent] = 1
					and [inst].[_RecordDeleted] = 0
		  )
	 , [Offerings Completed] = IsNull(
		  (
			  select 
				  sum(try_cast(replace(replace(replace(right([inst].[InstanceName], 6), 'YES', ''), 'NO', ''), '~', '') as int))
			  from [edw].[eChecklist_dbo_checklists_instances] as [inst]
			  where [inst].[HighestStatusId] = 3
					and [cl].[CheckListId] = [inst].[CheckListId]
					and try_cast(replace(replace(replace(right([inst].[InstanceName], 6), 'YES', ''), 'NO', ''), '~', '') as int) is not null
					and [inst].[InstanceName] like '%~T3~2020~%~YES'
					and [inst].[_RecordCurrent] = 1
					and [inst].[_RecordDeleted] = 0
		  ), 0)
	 , [CheckLists Inflight] =
		  (
			  select 
				  count(1)
			  from [edw].[eChecklist_dbo_checklists_instances] as [inst]
			  where [inst].[HighestStatusId] = 1
					and [cl].[CheckListId] = [inst].[CheckListId]
					and [inst].[InstanceName] like '%~T3~2020~%~YES'
					and [inst].[_RecordCurrent] = 1
					and [inst].[_RecordDeleted] = 0
		  )
	 , [CheckLists Completed] =
		  (
			  select 
				  count(1)
			  from [edw].[eChecklist_dbo_checklists_instances] as [inst]
			  where [inst].[HighestStatusId] = 3
					and [cl].[CheckListId] = [inst].[CheckListId]
					and [inst].[InstanceName] like '%~T3~2020~%~YES'
					and [inst].[_RecordCurrent] = 1
					and [inst].[_RecordDeleted] = 0
		  )
	 , [Tasks Rejected] =
		  (
			  select 
				  count(1)
			  from [edw].[eChecklist_dbo_checklists_instance_history] as [hist]
			  where [hist].[Action] = 'REJECTED'
					and [task].[TaskNo] = [hist].[TaskNo]
					and [hist].[InstanceId] in
											   (
												   select 
													   [inst].[InstanceId]
												   from [edw].[eChecklist_dbo_checklists_instances] as [inst]
												   where [cl].[CheckListId] = [inst].[CheckListId]
														 and [inst].[HighestStatusId] in ( 1, 3 )
														 and [inst].[InstanceName] like '%~T3~2020~%~YES'
														 and [inst].[_RecordCurrent] = 1
														 and [inst].[_RecordDeleted] = 0
											   )
					and [hist].[_RecordCurrent] = 1
					and [hist].[_RecordDeleted] = 0
		  ),
		  cl.[_DLTrustedZoneTimeStamp] as 'DataRefreshDate'
   from [edw].[eChecklist_dbo_checklists_checklists] as [cl]
		inner join [edw].[eChecklist_dbo_checklists_tasks] as [task]
			on [cl].[CheckLIstId] = [task].[CheckListId]
		inner join [edw].[eChecklist_dbo_checklists_work_unit_locales] as [wul]
			on [cl].[CheckListWorkUnitLocale] = [wul].[WorkUnitLocaleId]
   where [cl].[CheckListId] in ( 160, 161, 162, 163, 164, 165 )
		 and [cl].[_RecordCurrent] = 1
		 and [cl].[_RecordDeleted] = 0
		 and [task].[_RecordCurrent] = 1
		 and [task].[_RecordDeleted] = 0
		 and [wul].[_RecordCurrent] = 1
		 and [wul].[_RecordDeleted] = 0
   order by 
	   [wul].[WULPublicName] asc
	 , [cl].[CheckListId] asc
	 , [task].[TaskNo] asc;