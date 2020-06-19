-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/10/2019
-- Description: Creates and updates DataStores from VenuesWestDataStores_Config.json
--==============================================
create   proc ctl.sp_CreateDataStore @json varchar(max)
as
begin
	declare @projectCount int = (select count(*) from openjson(@json, '$.dataStore'))
	declare @countPrj int = 0
	while @countPrj < @projectCount
	begin
		declare 
			@Name varchar(250) = json_value(@json, concat('$.dataStore[', @countPrj , '].name')),
			@Description varchar(1000) = json_value(@json, concat('$.dataStore[', @countPrj , '].description')), 
			@Limit int = json_value(@json, concat('$.dataStore[', @countPrj , '].limit')), 
			@Connection varchar(500) = json_value(@json, concat('$.dataStore[', @countPrj , '].connection')), 
			@TypeId bigint =  (select TypeId from ctl.Type where name = json_value(@json, concat('$.dataStore[', @countPrj , '].type'))),
			@Check binary 
			
			set @Check = (select count(*) from [CTL].[DataStore] where name = @Name)
		--print @Description
		--print @Limit
		--print @Connection
		--print @TypeId
		--print @Name
		--print concat('workers : ', @projectCount, ' currentWorker: ', @countPrj)
		if @Check = 0
		begin
			insert into [CTL].[DataStore] ([Name], [Description], [Limit], [Connection], [TypeId])
			values (@Name, @Description, @Limit, @Connection, @TypeId)
		end
		else
		begin
			update [CTL].[DataStore] set 
				[Description] = @Description,
				[Limit] =		@Limit,
				[Connection] =	@Connection,
				[TypeId] =	@TypeId
			where [Name] =		@Name
		end
		set @countPrj = @countPrj + 1
	end
end