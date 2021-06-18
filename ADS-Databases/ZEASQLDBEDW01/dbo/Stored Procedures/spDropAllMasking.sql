CREATE procedure dbo.spDropAllMasking
as

IF @@SERVERNAME = 'zesqlbi01'
BEGIN

	PRINT 'Dropping masking from Production'

	--Declare a variable to build the SQL syntax to drop the masking
	DECLARE @SQL nvarchar(max) = '';
	
	--Get the drop mask syntax for all the tables and columns
	SELECT @SQL += CONCAT('ALTER TABLE ', schema_name, '.', table_name, ' ALTER COLUMN [', column_name, '] DROP MASKED;')
	FROM (

		SELECT c.name as column_name, s.name as schema_name, tbl.name as table_name, c.is_masked, c.masking_function
		FROM sys.masked_columns AS c  
		JOIN sys.tables AS tbl ON c.[object_id] = tbl.[object_id]  
		join sys.schemas s on tbl.schema_id = s.schema_id
		WHERE is_masked = 1
	) LstMasked

	--Execute the syntax to drop the masking
	EXEC sp_executesql @SQL
END