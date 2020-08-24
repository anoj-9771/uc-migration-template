-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/10/2019
-- Description: Get a list of task to process every x mins as specified in ADF trigger
-- Dequeue 
-- Create initial log for all tasks
--==============================================
CREATE   Procedure [CTL].[sp_getTasksToProcess]  @ExecTime datetime, @BatchId bigint--, @ProjectId int
As
begin
	exec [CTL].[sp_Dequeue]
end
begin
	declare @Tasks table (
			Row_Id int,
			TaskId bigint,
			BatchId bigint,
			ExecTime datetime,
			StageName varchar(250),
			TaskType varchar(100), 
			ProcessId bigint,
			ProcessName varchar(100),
			ProcessType varchar(100),
			SourceType varchar(100),
			SourceDataStoreName varchar(100),
			SourceConnection varchar(100),
			SourceName varchar(100),
			SourceId bigint,
			SourceBlobName varchar(250),
			SourceFormat varchar(100),
			SourceLocation varchar(1000),
			SourceQuery varchar(max),
			TargetType varchar(100),
			TargetDataStoreName varchar(100),
			TargetConnection varchar(100),
			TargetName varchar(100),
			TargetId bigint,
			TargetBlobName varchar(250),
			TargetFormat varchar(20),
			TargetLocation varchar(250),
			Query varchar(max),
			Path varchar(1000),
			WorkerName varchar(100),
			WorkerType varchar(100),
			WorkerDetails varchar(250),
			DestinationTable varchar(100),
			MetaDataQuery varchar(max),
			Watermark varchar(max)
		);
	delete from @Tasks
	insert into @Tasks 
	select
			row_number() over(order by (select 1)) Row_Id,
			qm.TaskId,
			@BatchId BatchId,
			@ExecTime ExecTime,
			stg.Name StageName,
			tt.Name TaskType, 
			tsk.ProcessId,
			prc.Name ProcessName,
			t_prc.Name  ProcessType,
			t_src.Name SourceType,
			ds_src.Name SourceDataStoreName,
			ds_src.Connection SourceConnection,
			d_src.Name SourceName,
			tsk.SourceId SourceId,
			concat(d_src.Name, [CTL].[udf_getFileDateHierarchy](prc.Grain, @ExecTime), d_src.Name) SourceBlobName,
			t_src.[Format] SourceFormat,
			[CTL].[udf_split](d_src.Location,'|',0) SourceLocation,
			isnull(isnull([CTL].[udf_split](d_src.Location,'|',1),
				[CTL].[udf_split](d_trg.Location,'|',1))
					, '') SourceQuery,
			t_trg.Name TargetType,
			ds_trg.Name TargetDataStoreName,
			ds_trg.Connection TargetConnection,
			d_trg.Name TargetName,
			tsk.TargetId as TargetId,
			concat(d_trg.Name, [CTL].[udf_getFileDateHierarchy](prc.Grain, @ExecTime), d_trg.Name) TargetBlobName,
			t_trg.[Format] TargetFormat,
			[CTL].[udf_split](d_trg.Location,'|',0) TargetLocation,
			tsk.Query Query,
			[CTL].[udf_getFileDateHierarchy](prc.Grain, @ExecTime) Path,
			w.Name WorkerName,
			wt.Name WorkerType,
			w.Details WorkerDetails,
			isnull(case when stg.StageId < 3 then (
				select trg.Name 
				from ctl.Task t 
				join ctl.DataSet trg on t.TargetId = trg.DataSetId  
				where stageId = 3 
				and t.ProcessId = prc.ProcessId) else d_trg.Name end, 'No Load to DB/DW') DestinationTable,
			[CTL].[udf_getMetaDataQuery](t_src.Name,d_src.Location) MetaDataQuery,
			(select string_agg(wm.[Column], ',') from ctl.Watermark wm where wm.DatasetId = tsk.TargetId or wm.DatasetId = tsk.SourceId ) Watermark
		from ctl.QueueMeta qm
		join ctl.Task tsk on qm.TaskId = tsk.TaskId 
		join ctl.Stage stg on tsk.StageId = stg.StageId
		join ctl.Process prc on tsk.ProcessId = prc.ProcessId 
		join ctl.Project prj on prc.ProjectId = prj.ProjectId
		join ctl.DataSet d_src on tsk.SourceId = d_src.DataSetId
		join ctl.DataSet d_trg on tsk.TargetId = d_trg.DataSetId
		join ctl.DataStore ds_src on d_src.DataStoreId = ds_src.DataStoreId
		join ctl.DataStore ds_trg on d_trg.DataStoreId = ds_trg.DataStoreId
		join ctl.Type t_prc	on prc.TypeId =  t_prc.TypeId
		join ctl.Type t_src	on d_src.TypeId =  t_src.TypeId
		join ctl.Type t_trg	on d_trg.TypeId =  t_trg.TypeId
		join ctl.Type tt on tsk.TaskTypeId = tt.TypeId
		join ctl.Worker w on tsk.WorkerId = w.WorkerId
		join ctl.Type wt on wt.TypeId = w.TypeId
		where qm.BatchId = @BatchId
		--and prj.ProjectId = @ProjectId
		and qm.Status = 1
		order by qm.QueueId asc


	--Update status from 1 to 2

		declare @t_max int = (select count(*) from @Tasks),
			@t_count int = 1
		while @t_count <= @t_max 
		begin 
			declare @t int = (select TaskId from @Tasks where Row_Id = @t_count) 
			declare @FullBlobName varchar(250) = (select SourceBlobName from @Tasks where Row_Id = @t_count)
			declare @ProcessId int = (select ProcessId from @Tasks where Row_Id = @t_count)
			--declare @CurrentRank int = (select [Rank] from ctl.Task where TaskId = @t)
			--declare @TaskId_nxt bigint
			--declare @MaxRank int = (select max([Rank]) 
			--							from [CTL].[Task] t 
			--							where ProcessId = @ProcessId
			--							group by ProcessId) 
			begin
				exec [CTL].[sp_logTasks] @BatchId, @t, @FullBlobName
			end
			--if @CurrentRank = @MaxRank
			--begin
			--	delete from ctl.QueueMeta where TaskId = @t and BatchId = @BatchId
			--end
			--else
			begin
				set nocount off
				update qm 
				set status = 2
				from ctl.QueueMeta qm
				where qm.TaskId = @t
				and qm.BatchId = @BatchId
				set nocount on
			end

			set @t_count = @t_count + 1
		end
	select * from @Tasks 
end