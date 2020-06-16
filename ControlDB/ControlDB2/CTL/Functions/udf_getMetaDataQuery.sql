-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 28/10/2019
-- Description: Function to create a statement to extract table schema from SQL server and Oracle
-- =============================================
CREATE    FUNCTION [CTL].[udf_getMetaDataQuery] (@SourceType varchar(250), @SourceName varchar(250))
RETURNS varchar(4000)
AS
BEGIN

return
(
	select Case when @SourceType like
				'%SQL%' then 
				'declare  
				  @object_name SYSNAME  
				, @object_id INT  

			Select 
				  @object_name = ''['' + OBJECT_SCHEMA_NAME(o.[object_id]) + ''].['' + OBJECT_NAME([object_id]) + '']''  
				, @object_id = [object_id]  
			from (select [object_id] = OBJECT_ID(''' + replace(replace(@SourceName,'[', ''),']','') + ''', ''U'')) o  
  
			select @object_name as Table_Name ,  ''['' + c.name + '']'' Column_Name ,    
				case when c.is_computed = 1  
					then  ''AS '' + OBJECT_DEFINITION(c.[object_id], c.column_id)  
					else   
						case when c.system_type_id != c.user_type_id   
							then  ''['' + SCHEMA_NAME(tp.[schema_id]) + ''].['' + tp.name + '']''   
							else ''['' + UPPER(tp.name) + '']''   
						end  +   
						case   
							WHEN tp.name IN (''varchar'', ''char'', ''varbinary'', ''binary'')  
								then  ''('' + case when c.max_length = -1   
												then  ''MAX''   
												else CAST(c.max_length AS VARCHAR(5))   
											end + '')''  
							WHEN tp.name IN (''nvarchar'', ''nchar'')  
								then  ''('' + case when c.max_length = -1   
												then  ''MAX''   
												else CAST(c.max_length / 2 AS VARCHAR(5))   
											end + '')''  
							WHEN tp.name IN (''datetime2'', ''time2'', ''datetimeoffset'')   
								then  ''('' + CAST(c.scale AS VARCHAR(5)) + '')''  
							WHEN tp.name = ''decimal''  
								then  ''('' + CAST(c.[precision] AS VARCHAR(5)) + '','' + CAST(c.scale AS VARCHAR(5)) + '')''  
							else ''''  
						end 
					end Column_DataType, 
				case when c.is_computed = 1  
					then  ''AS '' + OBJECT_DEFINITION(c.[object_id], c.column_id)  
					else    
						case when c.collation_name IS NOT NULL AND c.system_type_id = c.user_type_id   
							then  '' COLLATE '' + c.collation_name  
							else ''''  
						end 
					end Column_Collate,  
				case when c.is_computed = 1  
					then  ''AS '' + OBJECT_DEFINITION(c.[object_id], c.column_id)  
					else  
						case when c.is_nullable = 1   
							then  '' NULL''  
							else '' NOT NULL''  
						end    
				end  Column_Nullable
			from sys.columns c with(NOLOCK)  
			join sys.types tp with(NOLOCK) ON c.user_type_id = tp.user_type_id  
			where c.[object_id] = @object_id  
			order by c.column_id ' 

		when @SourceType like
				'%Oracle%' then 
				'select ''"'' ||owner || ''"."'' || table_name || ''"'' as "Table_Name, column_name as Column_Name, 
					case when data_type like ''%CHAR%'' then data_type || ''(''|| data_length || '')'' else data_type end as Column_DataType,DATA_LENGTH , 
					case when collation = ''USING_NLS_COMP'' then '' COLLATE SQL_Latin1_General_CP1_CI_AS'' else null end as Column_Collate,  
					case when nullable = ''Y'' then ''NULL'' else ''NOT NULL'' end Column_Nullable
				from "ALL_TAB_COLUMNS"
				where table_name = ' + @SourceName + ';'
		   End
)


END