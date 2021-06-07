-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/10/2019
-- Description: Creates and updates Processes, Datasets and Tasks from <Process>_Config.json
--==============================================
CREATE   proc [CTL].[sp_CreateProcessTask] @json varchar(max)
as
BEGIN
	SELECT N'Below script needs to be uncommented to work'
	--declare @projectCount int = (select count(*) from openjson(@json, '$.project')),
	--@countPrj int = 0
	--while @countPrj < @projectCount
	--begin
	--	declare 
	--		@prj_Name varchar(250) = json_value(@json, concat('$.project[', @countPrj , '].name')),
	--		@countPrc int = 0,
	--		@processCount int = (select count(*) from openjson(@json, concat('$.project[', @countPrj , '].process'))),
	--		@prj_Check binary

	--	set @prj_Check = (select count(*) from [CTL].[Project] where name = @prj_Name)
	--	print @countPrj
	--	if @prj_Check > 0
	--	begin
	--		while @countPrc < @processCount
	--		begin	
	--			declare	
	--				@taskCount int = (select count(*) from openjson(@json, concat('$.project[', @countPrj , '].process[', @countPrc , '].stage'))),
	--				@countTsk int = 0,
	--				@prc_Name varchar (250) = json_value(@json, concat('$.project[', @countPrj , '].process[', @countPrc , '].name')),
	--				@prc_Description varchar(500) = json_value(@json, concat('$.project[', @countPrj , '].process[', @countPrc , '].description')),
	--				@prc_Priority int = json_value(@json, concat('$.project[', @countPrj , '].process[', @countPrc , '].priority')),
	--				@prc_ProcessDependencies int = (select ProcessId from ctl.Process where name = json_value(@json, concat('$.project[', @countPrj , '].process[', @countPrc , '].ProcessDependencies'))),
	--				@prc_ProjectDependencies int = (select ProcessId from ctl.Process where name = json_value(@json, concat('$.project[', @countPrj , '].process[', @countPrc , '].ProjectDependencies'))),
	--				@prc_Enabled int = json_value(@json, concat('$.project[', @countPrj , '].process[', @countPrc , '].enabled')),
	--				@prc_Grain varchar(50) = json_value(@json, concat('$.project[', @countPrj , '].process[', @countPrc , '].grain')),
	--				@prc_TypeId int = (select TypeId from ctl.Type where name = json_value(@json, concat('$.project[', @countPrj , '].process[', @countPrc , '].type'))),
	--				@ProjectId int = (select ProjectId from ctl.Project where name = json_value(@json, concat('$.project[', @countPrj , '].name'))),
	--				@prc_Check binary

	--				--print @taskCount			
	--				--print @prc_Name			
	--				--print @prc_Description	
	--				--print @prc_Priority		
	--				--print @prc_Enabled			

	--			set @prc_Check = (select count(*) from [CTL].[Process] where name = @prc_Name)
		
					
	--			if @prc_Check = 0
	--			begin
	--				insert into [CTL].[Process] ([Name], [Description], [ProjectId], [Enabled], [Priority], [Grain], [TypeId], [ProcessDependency], [ProjectDependency])
	--				values (@prc_Name, @prc_Description, @ProjectId, 1, @prc_Priority, @prc_Grain, @prc_TypeId, @prc_ProcessDependencies, @prc_ProjectDependencies)
	--			end
	--			else
	--			begin
	--				update [CTL].[Process] set 
	--					[Description] = @prc_Description,
	--					[ProjectId] = @ProjectId,
	--					[Enabled] = @prc_Enabled,
	--					[Priority] = @prc_Priority,
	--					[Grain] = @prc_Grain,
	--					[TypeId] = @prc_TypeId,
	--					[ProcessDependency] = @prc_ProcessDependencies ,
	--					[ProjectDependency] = @prc_ProjectDependencies
	--				where [Name] =	@prc_Name
	--			end
	--			--print @countPrc		
	--			while @countTsk < @taskCount
	--			begin
	--				declare
	--					@ProcesId int = (select ProcessId from ctl.Process where name = @prc_Name),
	--					@stage int = json_value(@json, concat('$.project[', @countPrj , '].process[', @countPrc , '].stage[', @countTsk , '].id')),
	--					@rank int = json_value(@json, concat('$.project[', @countPrj , '].process[', @countPrc , '].stage[', @countTsk , '].rank')),
	--					@worker int = (select WorkerId from ctl.Worker where name = json_value(@json, concat('$.project[', @countPrj , '].process[', @countPrc , '].stage[', @countTsk , '].worker.name'))),
	--					@query varchar(8000) = json_value(@json, concat('$.project[', @countPrj , '].process[', @countPrc , '].stage[', @countTsk , '].query')),
	--					@TaskTypeId bigint = (select TypeId from ctl.Type where name = json_value(@json, concat('$.project[', @countPrj , '].process[', @countPrc , '].stage[', @countTsk , '].type'))),

	--					@src_Name varchar(250) = json_value(@json, concat('$.project[', @countPrj , '].process[', @countPrc , '].stage[', @countTsk , '].source.name')),
	--					@src_Description varchar(250) = json_value(@json, concat('$.project[', @countPrj , '].process[', @countPrc , '].stage[', @countTsk , '].source.description')),
	--					@src_DatastoreId int = (select DatastoreId from ctl.DataStore where name = json_value(@json, concat('$.project[', @countPrj , '].process[', @countPrc , '].stage[', @countTsk , '].source.dataStore'))), 
	--					@src_Location varchar(1000) = json_value(@json, concat('$.project[', @countPrj , '].process[', @countPrc , '].stage[', @countTsk , '].source.location')), 
	--					@src_TypeId bigint =  (select TypeId from ctl.Type where name = json_value(@json, concat('$.project[', @countPrj , '].process[', @countPrc , '].stage[', @countTsk , '].source.type'))),
	--					@src_Check binary,

	--					@trg_Name varchar(250) = json_value(@json, concat('$.project[', @countPrj , '].process[', @countPrc , '].stage[', @countTsk , '].target.name')),
	--					@trg_Description varchar(1000) = json_value(@json, concat('$.project[', @countPrj , '].process[', @countPrc , '].stage[', @countTsk , '].target.description')),
	--					@trg_DatastoreId int = (select DatastoreId from ctl.DataStore where name = json_value(@json, concat('$.project[', @countPrj , '].process[', @countPrc , '].stage[', @countTsk , '].target.dataStore'))), 
	--					@trg_Location varchar(1000) = json_value(@json, concat('$.project[', @countPrj , '].process[', @countPrc , '].stage[', @countTsk , '].target.location')), 
	--					@trg_TypeId bigint =  (select TypeId from ctl.Type where name = json_value(@json, concat('$.project[', @countPrj , '].process[', @countPrc , '].stage[', @countTsk , '].target.type'))),
	--					@trg_Check binary,
	--					@tsk_Check int

	--					set @src_Check = (select count(*) from [CTL].[Dataset] where Name = @src_Name and [Location] = @src_Location and DataStoreId = @src_DatastoreId)
	--					set @trg_Check = (select count(*) from [CTL].[Dataset] where Name = @src_Name and [Location] = @src_Location and DataStoreId = @trg_DatastoreId)

						
						
	--					----print @countTsk
	--					--print @stage			
	--					--print concat('WorkerId : ',  @worker)			
	--					--print @query			
	--					--print @TaskTypeId		
						
	--					--print @src_Name			
	--					--print @src_Description	
	--					--print concat('SourceDataStoreId: ', @src_DatastoreId)	
	--					--print @src_Location		
	--					--print @src_TypeId		
	--					--print @src_Check		
						
	--					--print @trg_Name			
	--					--print @trg_Description	
	--					--print concat('TargetDataStoreId: ', @trg_DatastoreId, ', ', @countPrj, ', ', @countPrc, ', ', @countTsk)	
	--					--print @trg_Location		
	--					--print @trg_TypeId		
	--					--print @trg_Check		

					

	--				begin 
	--					if @src_Check = 0
	--					begin
	--						insert into [CTL].[DataSet] ([Name], [Description], [DataStoreId], [Location])
	--						values (@src_Name, @src_Description, @src_DatastoreId, @src_Location)
	--					end
	--					else
	--					begin
	--						update [CTL].[DataSet] set 
	--							[Description] = @src_Description,
	--							[DataStoreId] =		@src_DatastoreId
	--						where [Name] =	@src_Name and [Location] =	@src_Location
	--					end	
					
	--					if @trg_Check = 0
	--					begin
	--						insert into [CTL].[DataSet] ([Name], [Description], [DataStoreId], [Location])
	--						values (@trg_Name, @trg_Description, @trg_DatastoreId, @trg_Location)
	--					end
	--					else
	--					begin
	--						update [CTL].[DataSet] set 
	--							[Description] = @trg_Description,
	--							[DataStoreId] =	@trg_DatastoreId
	--						where [Name] = @trg_Name and [Location] = @trg_Location
	--					end	
	--				end

	--				declare 
	--					@tsk_SourceId int  = (select top 1 DataSetId from ctl.DataSet where Name = @src_Name and [Location] = @src_Location and DataStoreId = @src_DatastoreId),
	--					@tsk_TargetId int = (select top 1 DataSetId  from ctl.DataSet where Name = @trg_Name and [Location] = @trg_Location and DataStoreId = @trg_DatastoreId),  
	--					@tsk_ProcessId int = (select top 1 ProcessId from ctl.Process where Name = @prc_Name)

	--				set @tsk_Check = (select count(*) from ctl.Task where SourceId = @tsk_SourceId and TargetId = @tsk_TargetId and ProcessId = @tsk_ProcessId)

	--				begin
	--					if @tsk_Check = 0
	--					begin
	--						insert into ctl.Task ([ProcessId], [SourceId], [TargetId], [StageId], [Rank], [WorkerId], [Query], [TaskTypeId])
	--						values(@tsk_ProcessId, @tsk_SourceId, @tsk_TargetId, @stage, @rank, @worker, @query, @TaskTypeId)
	--					end
	--					else 
	--					begin
	--						update [CTL].[Task] set 
	--							[StageId] =	@stage,
	--							[Rank] = @rank,
	--							[WorkerId] = @worker,
	--							[Query] = @query,
	--							[TaskTypeId] = @TaskTypeId
	--						where [ProcessId] = @tsk_ProcessId and [SourceId] = @tsk_SourceId and [TargetId] = @tsk_TargetId
	--					end 
	--				end

	--			set @countTsk = @countTsk + 1
	--			end
	--		set @countPrc = @countPrc + 1
	--		end
	--	end
	--	else
	--	begin
	--		raiserror('Project doese not exist! Please chenck to make sure the Project name is correct or create project.', 16, 1) 
	--	end
	--set @countPrj = @countPrj + 1
	--end
end