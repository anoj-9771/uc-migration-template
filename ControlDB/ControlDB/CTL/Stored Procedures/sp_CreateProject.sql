-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/10/2019
-- Description: Creates and updates projects from VenuesWestProjects_Config.json
--==============================================
create   proc ctl.sp_CreateProject @json varchar(max)
as
begin
	declare @projectCount int = (select count(*) from openjson(@json, '$.project'))
	declare @countPrj int = 0
	while @countPrj < @projectCount
	begin
		declare 
			@Name varchar(250) = json_value(@json, concat('$.project[', @countPrj , '].name')),
			@Description varchar(1000) = json_value(@json, concat('$.project[', @countPrj , '].description')), 
			@Enabled int = json_value(@json, concat('$.project[', @countPrj , '].enabled')), 
			@Schedule varchar(500) = json_value(@json, concat('$.project[', @countPrj , '].schedule')), 
			@Priority bigint =  json_value(@json, concat('$.project[', @countPrj , '].priority')),
			@Check binary 
			
			set @Check = (select count(*) from [CTL].[Project] where name = @Name)
		--print @Description
		--print @Enabled
		--print @Schedule
		--print @Priority
		--print @Name
		--print concat('workers : ', @projectCount, ' currentWorker: ', @countPrj)
		if @Check = 0
		begin
			insert into [CTL].[Project] ([Name], [Description], [Enabled], [Schedule], [Priority])
			values (@Name, @Description, @Enabled, @Schedule, @Priority)
		end
		else
		begin
			update [CTL].[Project] set 
				[Description] = @Description,
				[Enabled] =		@Enabled,
				[Schedule] =	@Schedule,
				[Priority] =	@Priority
			where [Name] =		@Name
		end
		set @countPrj = @countPrj + 1
	end
end