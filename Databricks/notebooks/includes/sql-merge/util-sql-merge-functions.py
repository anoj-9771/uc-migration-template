# Databricks notebook source
def _GetSQLCollectiveColumnsFromColumnNames(columns, alias = "", func = "", column_qualifer = "`"):
  
  '''
  The function takes column names as the first argument and returns the value with comma separated column names
  The column names can be either string or a list
  Optionally, you can pass the table alias name as second argument
  And finally you can wrap the column names in a function name with the third argument
  '''

  #If a list is passed as argument, convert the list to comma separated string
  if type(columns) is list:
    columns_str = ", ".join(columns)
  else:
    columns_str = columns
  
  #Remove blank spaces
  columns_str = columns_str.replace(" ", "")
  
  #Convert the string to a list
  col_list = columns_str.split(',')
  
  #Start of Fix for Handling Null in Key Coulumns
  if alias == "SRC":
    col_list = [item + '-TX' for item in col_list] 
  #End of Fix for Handling Null in Key Coulumns
    
  #Add quotes to quality the column names
  col_list = [column_qualifer + item + column_qualifer for item in col_list]
    
  #If using alias names, append the alias name and a dot to the list elements
  if alias != "":
    src_col_list = [alias + "." + item for item in col_list]
  else:
    src_col_list = col_list
  
  #Convert the list to a string separated by comma. However, if we use CONCAT function then append a | to the concatenated column list
  sql = ", '|', ".join(src_col_list) if func.upper() == "CONCAT" else ", ".join(src_col_list)
  
  #If using a function then wrap the sql list in the function name
  if func != "" and len(src_col_list) > 1:
    sql = f"{func}({sql})"

  return sql     
    

# COMMAND ----------

def _GetSQLJoinConditionFromColumnNames(columns, src_alias, tgt_alias, join_type, seperator, column_qualifer = "`"):

  
  '''
  The function gets the SQL join conditions using the columns string
  The column names can be either string or a list
  The source and target alias needs to be specified
  The join_type is the join condition. Either = or <>
  Optionally, you can pass the table alias name as second argument
  The separator can have values like , or AND depending on join conditions
  '''
  
  #If a list is passed as argument, convert the list to comma separated string
  if type(columns) is list:
    columns_str = ", ".join(columns)
  else:
    columns_str = columns
  
  #Remove blank spaces
  columns_str = columns_str.replace(" ", "")

  #Convert the string to a list
  col_list = columns_str.split(',')
  
  #Add quotes to quality the column names
  col_list = [column_qualifer + item + column_qualifer for item in col_list]

  #Add Alias Names to columns
  src_col_list = [src_alias + "." + item for item in col_list]
  tgt_col_list = [tgt_alias + "." + item for item in col_list]
  
  #Add the join conditions (= <>)
  final_col_list = [src + " " + join_type + " " + tgt for src, tgt in zip(src_col_list, tgt_col_list)]
  
  #Add the seperator. e.g. AND OR ,
  sql = seperator.join(final_col_list)

  return sql     

# COMMAND ----------

def _GetExclusiveList(main_lst, exception_lst):
  updated_col_list = [item for item in main_lst if item not in exception_lst]
  
  return updated_col_list

  
