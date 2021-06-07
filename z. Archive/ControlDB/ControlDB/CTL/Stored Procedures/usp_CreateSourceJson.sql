create   proc ctl.usp_CreateSourceJson @json varchar(max)
as
begin
	declare @projectCount int = (select count(*) from openjson(@json, '$.project[0].stage'))
	declare @stageCount int = (select count(*) from openjson(@json, '$.project[0].stage'))
	declare @countP int = 0
	declare @countS int = 0
	while @countP < @projectCount
	begin
		while @countS < @stageCount
			begin
				set nocount on
				declare @SourceName Varchar(255) =	json_value(@json, concat('$.project[' , @countP , '].stage[' , @countS , '].source.name')),
					@SourceLocation Varchar(255) =	json_value(@json, concat('$.project[' , @countP , '].stage[' , @countS , '].source.location')),
					@SourceTypeId BigInt =			(select TypeId from ctl.ControlTypes where ControlType = json_value(@json, concat('$.project[' , @countP , '].stage[' , @countS , '].source.type'))),  
					@SourceServer varchar(255) =	json_value(@json, concat('$.project[' , @countP , '].stage[' , @countS , '].source.secret')),
					@Processor varchar(255) =		json_value(@json, concat('$.project[' , @countP , '].stage[' , @countS , '].processor')),
					@TargetName Varchar(255) =		json_value(@json, concat('$.project[' , @countP , '].stage[' , @countS , '].target.name')),
					@TargetLocation Varchar(255) =	json_value(@json, concat('$.project[' , @countP , '].stage[' , @countS , '].target.location')),
					@TargetTypeId BigInt =			(select TypeId from ctl.ControlTypes where ControlType = json_value(@json, concat('$.project[' , @countP , '].stage[' , @countS , '].target.type'))),
					@TargetServer varchar(255)=		json_value(@json, concat('$.project[' , @countP , '].stage[' , @countS , '].target.secret')),
					@StageId BigInt = (select ControlStageId from ctl.ControlStages where StageName = json_value(@json, concat('$.project[' , @countP , '].stage[' , @countS , '].name'))),
					@projectId BigInt = (select projectId from ctl.Controlprojects where projectName = json_value(@json, concat('$.project[' , @countP , '].name'))),
					@CommandType int = (select TypeId from ctl.ControlTypes where ControlType = json_value(@json, concat('$.project[' , @countP , '].stage[' , @countS , '].type'))),
					@Command varchar(max) = json_value(@json, concat('$.project[' , @countP , '].stage[' , @countS , '].command')),
					@Delta int = (select case when (select json_value(@json, concat('$.project[' , 0 , '].stage[' , 0 , '].delta'))) = 'No' then 0 else 1 end),
					@Grain varchar(30) = json_value(@json, concat('$.project[' , @countP , '].grain'))		
				
			end
			begin
				--print concat(@SourceName, @SourceLocation,@SourceTypeId,@SourceServer,@Processor, @TargetName, @TargetLocation, @TargetTypeId, @TargetServer, @StageId, @projectId, @CommandType, @Command , @Delta, @Grain)
				exec [CTL].[CreateTask] @SourceName, @SourceLocation,@SourceTypeId,@SourceServer,@Processor, @TargetName, @TargetLocation, @TargetTypeId, @TargetServer, @StageId, @projectId, @CommandType, @Command , @Delta, @Grain
			end
			set nocount off
			set @countS = @countS + 1
		set @countP = @countP + 1
	end
end