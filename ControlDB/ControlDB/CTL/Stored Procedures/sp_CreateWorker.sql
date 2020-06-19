-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/10/2019
-- Description: Creates and updates workers from VenuesWestWorkers_Config.json
--==============================================
CREATE proc ctl.sp_CreateWorker @json varchar(max)
as
begin
	declare @workerCount int = (select count(*) from openjson(@json, '$.worker'))
	declare @countW int = 0
	while @countW < @workerCount
	begin
		declare 
			@Name varchar(250) = json_value(@json, concat('$.worker[', @countW , '].name')),
			@Description varchar(1000) = json_value(@json, concat('$.worker[', @countW , '].description')), 
			@Limit int = json_value(@json, concat('$.worker[', @countW , '].limit')), 
			@Details varchar(500) = json_value(@json, concat('$.worker[', @countW , '].details')), 
			@TypeId bigint = (select TypeId from [CTL].[Type] where name = json_value(@json, concat('$.worker[', @countW , '].type'))),
			@Check binary 
			
			set @Check = (select count(*) from [CTL].[Worker] where name = @Name)
		--print @Name
		--print @description
		--print @limit
		--print @details
		--print @TypeId
		--print concat('workers : ', @workerCount, ' currentWorker: ', @countW)
		if @Check = 0
		begin
			insert into [CTL].[Worker] ([Name], [Description], [Limit], [Details], [TypeId])
			values (@Name, @Description, @Limit, @Details, @TypeId)
		end
		else
		begin
			update [CTL].[Worker] set 
				[Description] = @Description,
				[Limit] = @Limit,
				[Details] = @Details,
				[TypeId] = @TypeId
			where [Name] = @name
		end
		set @countW = @countW + 1
	end
end