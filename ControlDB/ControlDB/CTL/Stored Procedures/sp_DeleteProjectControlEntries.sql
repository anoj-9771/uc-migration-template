


--[CTL].[sp_DeleteProjectControlEntries] 'XFORM'
CREATE PROC [CTL].[sp_DeleteProjectControlEntries] @projectName varchar(50)
AS

DROP TABLE IF EXISTS #Tasks
DROP TABLE IF EXISTS #Processes

BEGIN TRY
       BEGIN TRANSACTION
			  ALTER TABLE CTL.Task NOCHECK CONSTRAINT FK_Task_DataSet_Source
			  ALTER TABLE CTL.Task NOCHECK CONSTRAINT FK_Task_DataSet_Target
              Select ProcessId   
              INTO #Processes
              From [CTL].[Process]
              Where ProjectId = (select ProjectId from CTL.Project where [Name] = @projectName)
              
              Select * 
              INTO #Tasks 
              From [CTL].[Task]
              Where ProcessId in (SELECT ProcessId FROM #Processes)
              
              Delete From CTL.QueueMeta
              where Taskid in (select TaskId from #Tasks)
              
              Delete From CTL.TaskLog
              where Taskid in (select TaskId from #Tasks)

              Delete From [CTL].[Task]
              where TaskId in (select TaskId from #Tasks)

              Delete From [CTL].[DataSet]
              Where DataSetId in (Select SourceId from #Tasks)
              
              Delete From [CTL].[DataSet]
              Where DataSetId in (Select TargetId from #Tasks)
              
              Delete From [CTL].[Process]
              where ProcessId in (select ProcessId from #Processes)
              ALTER TABLE CTL.Task CHECK CONSTRAINT FK_Task_DataSet_Source
              ALTER TABLE CTL.Task CHECK CONSTRAINT FK_Task_DataSet_Target
       COMMIT

END TRY
BEGIN CATCH

              Rollback
              Declare @err_num  int = @@ERROR
              Declare @err_desc varchar(500) = ERROR_MESSAGE()
              raiserror(@err_desc,@err_num,1)

END CATCH

DROP TABLE IF EXISTS #Processes
DROP TABLE IF EXISTS #Tasks