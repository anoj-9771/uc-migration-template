create     PROC [dbo].[usp_Upsert]
 @schema_src varchar(50), 
 @schema_trg varchar(50), 
 @source varchar(500),
 @target varchar(500),
 @key   varchar(500),
 @partition varchar(500) = null
 AS

Declare @cols varchar(max) = ''
Declare @srcCols varchar(max) = ''
Declare @tgtCols varchar(max) = ''
Declare @updCols varchar(max) = ''

Declare @sql varchar(max)
Declare @srcKeyvalue  varchar(500) = ''
Declare @tgtKeyvalue  varchar(500) = ''


Select @srcKeyvalue += 'Trim(cast(['+[value] +'] as varchar(100))),',  @tgtKeyvalue += 'Trim(cast(TGT.['+[value] +'] as varchar(100))),' from string_split(@key,',') 
Set @srcKeyvalue = substring(@srcKeyvalue,0,len(@srcKeyvalue)) 
Set @tgtKeyvalue = substring(@tgtKeyvalue,0,len(@tgtKeyvalue)) 

if charindex(',',@srcKeyvalue) > 0 Set @srcKeyvalue = 'Concat(' + @srcKeyvalue + ')'
if charindex(',',@tgtKeyvalue) > 0 Set @tgtKeyvalue = 'Concat(' + @tgtKeyvalue + ')'

Select @cols += '[' + COLUMN_NAME + '],', @srcCols += 'SRC.[' + COLUMN_NAME + '],', @tgtCols += 'TGT.[' + COLUMN_NAME + '],' 
FROM INFORMATION_SCHEMA.COLUMNS
where TABLE_SCHEMA = @schema_src
AND TABLE_NAME =  @source


set @cols = substring(@cols,0,len(@cols))
set @srcCols = substring(@srcCols,0,len(@srcCols))
set @tgtCols = substring(@tgtCols,0,len(@tgtCols))

Select @updCols += 'TGT.[' + COLUMN_NAME + '] = SRC.[' + COLUMN_NAME + '],' from
INFORMATION_SCHEMA.COLUMNS
where TABLE_SCHEMA = @schema_src
AND TABLE_NAME =  @source
AND COLUMN_NAME not in (@key)
set @updCols = substring(@updCols,0,len(@updCols))

Set @sql = 
'MERGE INTO ' + @schema_trg + '.' + @target + ' as TGT
 USING (Select ' + @cols + ',' + @srcKeyvalue + ' as [Key] from ( select row_number()over(partition by ' + @key +' order by ' + @partition +' desc) RowRank, ' + @cols + ' from [' + @schema_src + '].[' + @source + '])a where RowRank = 1) as SRC
 ON SRC.[Key] = ' + @tgtKeyvalue + 
 ' WHEN MATCHED THEN UPDATE SET ' + @updCols + 
 ' WHEN NOT MATCHED THEN INSERT(' + @Cols +') Values (' + @srcCols + ');'
 
 print @sql
 EXEC(@sql)