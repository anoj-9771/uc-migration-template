# Databricks notebook source
import random
import string
import pytz

# COMMAND ----------

#Please check the spelling mistake and the effect
CURRET_TIMEZONE = "Australia/NSW"

# COMMAND ----------

def GeneralGetBoolFromString(str_val):
  if str_val.upper() == "TRUE":
    val = True
  else:
    val = False
    
  return val

# COMMAND ----------

def GeneralGetUpdatedDeltaColumn(delta_column):

  lst = delta_column.split(",")
  lst = [item.strip() for item in lst]

  if len(lst) > 1:
    update_col_exist = any(col in lst for col in ADS_COLUMN_UPDATED)
    created_col_exist = any(col in lst for col in ADS_COLUMN_CREATED)
    
    if update_col_exist and created_col_exist:
      delta_column = ADS_COLUMN_TRANSACTION_DT
      
  return delta_column

# COMMAND ----------

def GeneralGetDataLakeMountPath(source_object, mount_point):
  
  source_system = source_object.split('_')[0]
  table_name = "_".join(source_object.split('_')[1:])
  print (source_system)
  print (table_name)
  
  delta_path = "dbfs:{mount}/{sourcesystem}/{sourcefile}/delta".format(mount=mount_point, sourcesystem = source_system.lower(), sourcefile = table_name.lower())
  
  return delta_path


# COMMAND ----------

def GeneralFileExists(file_url, mount):

  source_file_path = "dbfs:{m}/{s}".format(m=mount, s=file_url)
  
  file_name = source_file_path.split("/")[-1]

  folder = "/".join(source_file_path.split("/")[0:-1])
  
  try:
    file_list = dbutils.fs.ls(folder)
    
    for item in file_list:
      if file_name in item:
        return True
  except Exception as e:
    if "FileNotFoundException" in str(e):
      print ("Folder Not Found")
      return False
    else:
      raise

    
  return False

# COMMAND ----------

def GeneralGetSchema(file_url):
  schema = StructType()
  with open(schema_file_url, "r") as f_read:
    for cnt, line in enumerate(f_read):
      #print(line)
      cols = line.split(",")
      schema.add(cols[0], cols[1])
  
  return schema

# COMMAND ----------

def GeneralGetSQLColumnChange(file_url, dataframe, tsFormat = ""):
  
  #Convert the dataframe col name and type as a dictionary
  dict_df_cols = {x[0]: x[1] for x in dataframe.dtypes}

  #Read the file
  f_read = open(file_url, "r")
  
  #Loop through all the lines in the file
  for cnt, line in enumerate(f_read):
    
    #Split the lines into a list
    cols = line.split(",")
    cols = [i.strip() for i in cols]
    
    #If the column name starts with special chars (applicable for CDC) ignore the line
    if "__$" in cols[0]: pass
    
    #If the column name is present in the dataframe
    if cols[0] in dict_df_cols:
      #If the column type is decimal add scale and precision
      if cols[1] == "decimal":
        dict_df_cols[cols[0]] = f"{cols[1]} ({cols[3]}, {cols[4]})"
      #Else update the data type on the original dictionary
      else:
        dict_df_cols[cols[0]] = cols[1]

  #Start with an empty list
  expr = []
  
  #Loop through the dictionary
  for item in dict_df_cols.items():
    
    if tsFormat != "":
      if item[1].lower() == "date":
        str_cast = f"to_date (`{item[0]}`, '{tsFormat}') `{item[0]}`"
      elif item[1].lower() == "timestamp":
        str_cast = f"to_timestamp (`{item[0]}`, '{tsFormat}') `{item[0]}`"
      else:
        #Build the cast string
        str_cast = f"cast (`{item[0]}` as {item[1]}) `{item[0]}`"
    else:
      #Build the cast string
      str_cast = f"cast (`{item[0]}` as {item[1]}) `{item[0]}`"
    
    #Add the cast string to the list
    expr.append(str_cast)

  return expr

# COMMAND ----------

def GeneralAlignTableName(table_name):
  import re
  print ('Updating Table Names to remove special characters')
    
  renamed_table = d = re.sub('[-@ ,;{}()]', '_', table_name)
    
  return renamed_table

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC def GeneralAlignTableNameScala(table_name: String) : String = {
# MAGIC   
# MAGIC   val regex = "[;@ ,;{}()-]".r
# MAGIC   
# MAGIC   var updated_table_name = regex.replaceAllIn(table_name, "_")
# MAGIC   
# MAGIC   return updated_table_name
# MAGIC   
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC def GeneralGetBoolFromStringScala(str_val: String) : Boolean = {
# MAGIC   
# MAGIC   if (str_val.toUpperCase() == "TRUE")
# MAGIC   {
# MAGIC     return true
# MAGIC   }
# MAGIC   else
# MAGIC   {
# MAGIC     return false
# MAGIC   }
# MAGIC }

# COMMAND ----------

def GeneralToPascalCase(word):
    return ''.join(x.capitalize() or '_' for x in word.split('_'))

# COMMAND ----------

def GeneralToTidyCase(word):
  return ' '.join(x.capitalize() or '_' for x in word.split('_'))

# COMMAND ----------

def GeneralAliasDataFrameColumns(dataFrame, prefix):
  df = dataFrame.select(*(col(x).alias(prefix + x) for x in dataFrame.columns))
  return df

# COMMAND ----------

def GeneralRandomString(stringLength):
  letters = string.ascii_letters
  return ''.join(random.choice(letters) for i in range(stringLength))

# COMMAND ----------

def GeneralWriteFixedWidthFile(dataframe, dict_col_size, file_path):
  
  from pyspark.sql import functions as F
  
  #Convert all columns to string 
  for col, size in dict_col_size.items():
    dataframe = dataframe.withColumn(col, dataframe[col].cast(StringType()))

  #Right pad the columns with space on right based on dict size
  for col, size in dict_col_size.items():
    dataframe = dataframe.withColumn(col, F.rpad(dataframe[col], size, ' '))
    
  #Convert all null columns to empty string
  dataframe = dataframe.fillna("")
  
  #Get List of Columns from the Dictonary
  col_list = dict_col_size.keys()
  
  #Cocatenate all columns to a single column
  dataframe_all_cols = dataframe.withColumn("allcolumns", concat(*col_list)).select("allcolumns")
  
  #Write the file
  dataframe_all_cols.toPandas().to_csv(file_path, header=False, index=False)

# COMMAND ----------

def GeneralIsValidDate(date, fmt = "", IsNullDatesValid = True):
  
  #This function validates the date
  from datetime import datetime
  
  if date is None: return IsNullDatesValid

  date = str(date)
  
  date_formats = ["%d%y%m", "%d%m%Y", "%Y%m%d", "%d-%m-%Y", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]
  
  if fmt != "" : 
    date_formats = list(eval(fmt))
    if len(date_formats[0]) == 1: 
      date_formats = list(eval(fmt + ", " + fmt))

  for format in date_formats:
    try:
      datetime.strptime(date, format)
      
      if datetime.strptime(date, format).year < 1800 or datetime.strptime(date, format).year > 2200:
        return False
      return True
    except ValueError:
      pass
  
  return False

#Register UDF - This method allows UDF to be used with Saprk SQL
#%sql SELECT ID, IsValidDate(DATECOL) FROM TABLE
spark.udf.register("IsValidDate", GeneralIsValidDate)

from pyspark.sql.types import BooleanType
#Register the UDF - This method allows UDF to be used with DataFrames
#df.filter(IsValidDate_udf(df["StartDate"]) == True)
IsValidDate_udf = udf(GeneralIsValidDate, BooleanType())

# COMMAND ----------

def GeneralLocalDateTime():
  import pytz
  from datetime import datetime
  return datetime.now(pytz.timezone(CURRET_TIMEZONE))

# COMMAND ----------

def GeneralGetAESTCurrent():
  import pytz
  from datetime import datetime
  utc_now = pytz.utc.localize(datetime.utcnow())

  aest_now = utc_now.astimezone(pytz.timezone('Australia/Sydney'))
  return aest_now

# COMMAND ----------

def GeneralSpecialCharacterReplace(column, list):
  for l in list:
     column = regexp_replace(column, l[0], l[1])
  return column

# COMMAND ----------

def GeneralSaveQueryAsTempTable(query, table_name):
  from pyspark.sql import Row
  r = Row(id='1', sql=query)
  df_py_scala_var = spark.createDataFrame([r])
  df_py_scala_var.write.mode(ADS_WRITE_MODE_OVERWRITE).saveAsTable(table_name)


# COMMAND ----------

def GeneralGetDataLoadMode(truncate, upsert, append):
  '''
  Get the Data Load Mode using the params
  '''
  
  if truncate:
    mode = ADS_WRITE_MODE_OVERWRITE
  elif upsert:
    mode = ADS_WRITE_MODE_MERGE
  elif append:
    mode = ADS_WRITE_MODE_APPEND
  else:
    mode = ADS_WRITE_MODE_OVERWRITE
    
  return mode

# COMMAND ----------

def GeneralToValidDateTime(dateIn, colType ="Optional", fmt = "" ):

    #This function validates the date
    from dateutil import parser
    from datetime import datetime
    from dateutil.tz import gettz
    import pytz
    from pytz import timezone, utc
    
    SydneyTimes = {'AEDT': gettz('Australia/NSW'), 'AET': 11*60*60}
    
    lowDate = parser.parse('1900-01-01 00:00:00 AET', tzinfos=SydneyTimes)
    lowDatePlain = parser.parse('1900-01-01 01:00:00 AET', tzinfos=SydneyTimes)
#     highNullDate = parser.parse('9999-12-30 23:00:00 AEDT', tzinfos=SydneyTimes)
    
    dateStr = str(dateIn)
    
    #check if length zero and mandatory, else add time to it if not present so the parser works nicely
    if len(dateStr) == 0 and colType.upper() == "MANDATORY":
        return lowDatePlain
    
    if len(dateStr) <= 10:
        dateStr += ' 00:00:00'

    try:
        dateOut = parser.parse(dateStr + ' AEDT', tzinfos=SydneyTimes)
        
        if dateOut < lowDate:
            return lowDatePlain
        else:
            return dateOut
    except:
        dateOut = None
        
    if colType.upper() == "MANDATORY" and (dateIn is None or dateOut is None):
        return lowDate   
    elif colType.upper() != "MANDATORY" and (dateIn is None or dateOut is None):
        return

from pyspark.sql.types import TimestampType, DateType

#Register UDF - This method allows UDF to be used with Saprk SQL
#%sql SELECT ID, ToValidDate(DATECOL) FROM TABLE
spark.udf.register("ToValidDate", GeneralToValidDateTime,DateType())
#%sql SELECT ID, ToValidDateTime(DATETIMECOL) FROM TABLE
spark.udf.register("ToValidDateTime", GeneralToValidDateTime,TimestampType())

#Register the UDF - This method allows UDF to be used with DataFrames
#DateCol = df.ToValidDate_udf(df["StartDate"]))
ToValidDate_udf = udf(GeneralToValidDateTime, DateType())
#DateTimeCol = df.ToValidDateTime_udf(df["StartDateTime"]))
ToValidDateTime_udf = udf(GeneralToValidDateTime, TimestampType())
