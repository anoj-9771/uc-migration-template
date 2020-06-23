create proc dbo.usp_BuildSchemaViews 
@schema varchar(50)
as 
-- Declare variables
declare @tbl_counter int
declare @max_table int
declare @schemaName varchar(50)
declare @tableName varchar(250)
declare @columnNames varchar(max)
declare @query varchar(max)

-- Declare tables
declare @tables table ([Index] int, SchemaName varchar(50), TableName varchar(250), ColumnNames varchar(max))

insert into @tables 
select row_number()over(order by (select 1)), 
	s.name,  t.name, string_agg(c.name, ',') within group (order by c.column_id)
	from sys.tables t
	join sys.schemas s on t.schema_id = s.schema_id
	join sys.columns c on t.object_id = c.object_id
	where s.name = @schema
	group by s.name, t.name, t.object_id
	order by t.object_id

select @max_table = count(*) from @tables

set @tbl_counter = 1 
while @tbl_counter <= @max_table
begin
	select @tableName = TableName from @tables where [Index] = @tbl_counter
	select @columnNames = ColumnNames from @tables where [Index] = @tbl_counter
	set @query =  'create view ' + @schemaName + '.vw_' + @tableName + ' as 
	select ' + @columnNames + ' from ' + @schemaName + '.' + @tableName
	print @query
	set @tbl_counter += 1
end