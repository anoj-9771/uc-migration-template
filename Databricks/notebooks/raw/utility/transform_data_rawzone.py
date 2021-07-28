# Databricks notebook source
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import concat, col, lit, substring, to_utc_timestamp, from_utc_timestamp, datediff, countDistinct, count, greatest
from pyspark.sql import functions as F

import re


# COMMAND ----------

def transform_raw_dataframe(dataframe, Params):
  source_object = Params[PARAMS_SOURCE_NAME].lower()
  source_system = Params[PARAMS_SOURCE_GROUP].lower()
  print ("Starting : transform_raw_dataframe for : " + source_object)
  
  is_cdc = Params[PARAMS_CDC_SOURCE]
  is_delta_extract = Params[PARAMS_DELTA_EXTRACT]
  delta_column = GeneralGetUpdatedDeltaColumn(Params[PARAMS_WATERMARK_COLUMN])
  
  if Params[PARAMS_SOURCE_TYPE] == "Flat File":
    dataframe = transform_raw_dataframe_trim(dataframe)
  
  if is_cdc:
    #If we are doing CDC load, then some additional transformations to be done for CDC column
    dataframe = transform_raw_dataframe_cdc(dataframe)
  else:
    dataframe = dataframe
    
  #If Schema file is available, update the column types based on schema file
  dataframe = transform_update_column_data_type_from_schemafile(source_system, source_object, dataframe, Params)
    
  #Remove spaces from column names
  dataframe = transform_update_column_name(dataframe)
  
  #Custom changes for MySQL as the datetime delta columns are stored as number
  if source_system == "lms" or source_system == "cms":
    dataframe = transform_custom_mysql_lms_update_delta_col(dataframe, Params)

  #Make sure the delta columns are stored as TimestampType
  ts_format = Params[PARAMS_SOURCE_TS_FORMAT]
  dataframe = transform_raw_update_datatype_delta_col(dataframe, delta_column, ts_format)

  #Add a Generic Transaction Date column to the dataframe
  #This is helpful when there are multiple extraction columns e.g. CREATED_DATE and UPDATED_DATE
  dataframe = transform_raw_add_col_transaction_date(dataframe)
  
  curr_time = str(datetime.now())
  dataframe = dataframe.withColumn(COL_DL_RAW_LOAD, from_utc_timestamp(F.lit(curr_time).cast(TimestampType()), ADS_TZ_LOCAL))
  
  #If it is a Delta/CDC add year, month, day partition columns so that the Delta Table can be partitioned on YMD
  dataframe = transform_raw_add_partition_cols(dataframe, source_object, is_delta_extract, delta_column, is_cdc)

  # Call the specific routime based on the source type
  #if source_object == "sales_orders".upper():
  #  dataframe = transform_raw_sales_orders(dataframe)
  #else:
  #  dataframe = dataframe

  return dataframe

# COMMAND ----------

def transform_update_column_data_type_from_schemafile(data_lake_folder, source_object, dataframe, Params):
  
  mount_point = DataLakeGetMountPoint(ADS_CONTAINER_RAW)
  #Get the URL for Schema file from the data_lake_folder and source_object variable
  schema_file = "{folder}/schema/{file}.schema".format(folder = data_lake_folder, file=source_object)
  print(schema_file)
  
  tsFormat = Params["SourceTimeStampFormat"]

  #Check if the Schema File exists
  if GeneralFileExists(schema_file, mount_point):
    #We need to map the file before we read
    #The way we map the file to read for file system is slightly different than normal
    schema_file_url = "/dbfs{mount}/{file}".format(mount=mount_point, file = schema_file)
    print(schema_file_url)
    #Get the Data Type cast expression from the file
    #Pass the dataframe so that columns present in dataframe can be compared and schema file generated only for relevant columns
    file_expr = GeneralGetSQLColumnChange(schema_file_url, dataframe, tsFormat)
    
    print(file_expr)

    #Apply the cast expression we got in the previous step
    #If the schema file is not present, then the below step will fail letting us know that the schema file was missing
    df = dataframe.selectExpr(file_expr)
  else:
    df = dataframe
    
  return df

# COMMAND ----------

def transform_update_column_name(df):
  #Align the column names with removing space/special chars with underscore
  
  print ('Updating Column Names to remove spaces with underscore')
  
  #Loop all columns and replace space with underscore
  for col in df.columns:
    df = df.withColumnRenamed(col, col.replace(" ", "_"))
  
  #Create a dynamic function and use regular expression to remove special characters with underscore
  def canonical(x): return re.sub("[ ,;{}()\n\t=]+", '_', x)
  
  #Get a list with new columns
  renamed_cols = [canonical(c) for c in df.columns]
  
  #Apply the new names to the dataframe
  df = df.toDF(*renamed_cols)
  
  expr = []
  for col in df.columns:
    str_cast = "`" + col + "` as `" + col + "`"
    expr.append(str_cast)
  df = df.selectExpr(expr)
    
  return df

# COMMAND ----------

def transform_raw_update_datatype_delta_col(dataframe, delta_column, ts_format):
  #Sometimes when the date columns are blank, databricks treats them as string and thus there is a datatype mismatch
  #So, we explicitly convert them to TimestampType
    
  #Convert the existing dataframe column names to upper so that it is easier to compare
  df_cols_upper = [x.upper() for x in dataframe.columns]

  #Convert the delta column to a list. If there are more than one delta column then a list will help (and remove any whitespaces)
  lst_delta_cols = [col.strip() for col in delta_column.split(",")]

  for col in lst_delta_cols:
    if col.upper() in df_cols_upper:
      if ts_format != "":
        print (f"Applying timestamp format {ts_format} to the column {col}")
        dataframe = dataframe.withColumn(col, to_timestamp(dataframe[col], ts_format))

      print("Updating data type for delta column " + col)
      dataframe = dataframe.withColumn(col, dataframe[col].cast(TimestampType()))

  print ("Finished updating delta columns")
  
  return dataframe


# COMMAND ----------

def _GetDeltaDateColumnFromDataFrame(dataframe, ads_columns):
  #Routine to check if the Delta Date column from our config list is present in the dataframe and then return the column name
  
  #Convert the list of dataframe columns to dictionary
  df_cols_dict = {x.upper(): x for x in dataframe.columns}
  
  #Convert the ads_columns list to a dictionary
  ads_column_dict = {x.upper(): x for x in ads_columns}
  
  #Get a new list with a list of matching columns. If the data frame contained the list of delta columns configured, we would see a match here
  match_col_list = [item for item in ads_column_dict if item in df_cols_dict]
  
  #If a match was found
  if len(match_col_list) > 0:
    #Get the date column from the dictionary (as we do not want to return the upper cased name)
    return ads_column_dict[match_col_list[0]]
  else:
    #Else return blank
    return ""
    
  

# COMMAND ----------

def transform_raw_add_partition_cols(dataframe, source_object, is_delta_extract, delta_column, is_cdc):
  #Add Partition Columns Year, Month, Day if it is a Delta Load
  
  print ("Starting : transform_raw_add_partition_cols. delta_column : " + delta_column)
  
  is_partitioned = True #if DeltaTablePartitioned(f"{ADS_DATABASE_RAW}.{source_object}") or is_delta_extract else False
  
  if is_cdc:
    print ("Adding Partition Columns for CDC Load")
    df_updated = dataframe \
      .withColumn('year', F.year(dataframe.rowtimestamp).cast(StringType())) \
      .withColumn('month', F.month(dataframe.rowtimestamp).cast(StringType())) \
      .withColumn('day', F.dayofmonth(dataframe.rowtimestamp).cast(StringType()))

  elif is_partitioned:
    partition_column = COL_DL_RAW_LOAD if delta_column == "" else delta_column
    print (f"Adding Partition Column for Delta Load using {partition_column}")
    df_updated = dataframe \
      .withColumn('year', F.year(dataframe[partition_column]).cast(StringType())) \
      .withColumn('month', F.month(dataframe[partition_column]).cast(StringType())) \
      .withColumn('day', F.dayofmonth(dataframe[partition_column]).cast(StringType()))

  if is_partitioned:
    #Add leading 0 to day and month column if it is a single digit
    print ("Adding leading 0 to day and month")
    df_updated = df_updated \
      .withColumn('day', F.when(F.length('day') == 1, F.concat(F.lit('0'), F.col('day'))).otherwise(F.col('day'))) \
      .withColumn('month', F.when(F.length('month') == 1, F.concat(F.lit('0'), F.col('month'))).otherwise(F.col('month')))
  else:
    print ("No partition columns added as not Delta or CDC load.")
    df_updated = dataframe
  
  return df_updated


# COMMAND ----------

def transform_raw_add_col_transaction_date(dataframe):
  #If the Source Extraction is based on 2 date columns (Created Date and Updated Date), then add a new column Transaction Date 
  #If not copy the other delta table column to the Transaction Date
  #The Transaction Date column can be used for filter conditions instead of multiple columns
  
  #Get the created and updated date column from the config list and check for match in the dataframe
  #If found then they will have to be handled specially with a new column called _transaction_date created
  created_date_col = _GetDeltaDateColumnFromDataFrame(dataframe, ADS_COLUMN_CREATED)
  updated_date_col = _GetDeltaDateColumnFromDataFrame(dataframe, ADS_COLUMN_UPDATED)

  if created_date_col != "" and updated_date_col != "":
    print ("Adding new column for Combined Date " + ADS_COLUMN_TRANSACTION_DT )
    df_updated = dataframe.withColumn(ADS_COLUMN_TRANSACTION_DT, greatest(dataframe[created_date_col], dataframe[updated_date_col]))
  elif created_date_col != "":
    print ("Adding new column for Created Date " + ADS_COLUMN_TRANSACTION_DT )
    df_updated = dataframe.withColumn(ADS_COLUMN_TRANSACTION_DT, dataframe[created_date_col])
  elif updated_date_col != "":
    print ("Adding new column for Updated Date " + ADS_COLUMN_TRANSACTION_DT )
    df_updated = dataframe.withColumn(ADS_COLUMN_TRANSACTION_DT, dataframe[updated_date_col])
  else:
    print ("No columns to add for " + ADS_COLUMN_TRANSACTION_DT )
    df_updated = dataframe

  #If OneEBS has the updated transaction date, then use it for the _transaction_date column 
  df_updated = transform_custom_oneebs_transaction_date(df_updated)
    
  return df_updated

# COMMAND ----------

def transform_raw_dataframe_trim(dataframe):
  
  print ("Applying trim to all columns in dataframe")
  #For TEXT files
  #Loop through all the columns and trim all columns
  for col in dataframe.columns:
    dataframe.withColumn (col, trim(col))
    
  return dataframe

# COMMAND ----------

def transform_raw_dataframe_cdc(dataframe):

  # These data type change are valid for all source data, which are on CDC
  # Historically there are values as characters as well, thus making sure teh column is stored as String
  print("Applying CDC transformations")
  dataframe = dataframe \
    .withColumn("__$start_lsn", dataframe["__$start_lsn"].cast(StringType())) \
    .withColumn("__$seqval", dataframe["__$seqval"].cast(StringType())) \
    .withColumn("__$operation", dataframe["__$operation"].cast(StringType())) \
    .withColumn("__$update_mask", dataframe["__$update_mask"].cast(StringType())) \
    .withColumn("rowtimestamp", dataframe.rowtimestamp.cast(StringType())) \

  # Spark does not do very good with the $ sign on the column name. So let's rename it right away
  dataframe = dataframe \
    .withColumnRenamed('__$operation', 'cdc_operation') \
    .withColumnRenamed('__$start_lsn', 'start_lsn') \
    .withColumnRenamed('__$seqval', 'seqval') \
    .withColumnRenamed('__$update_mask', 'update_mask') \
  
    
  # If rows are updated in a batch, their LSN will be same, however the sequence number will be different in the order of change
  # Thus, let's create a cmbined field called uniq_lsnn
  # Convert the rowtimestamp to utc time
  dataframe = dataframe \
    .withColumn('uniq_lsn', concat('start_lsn','seqval')) \
    .withColumn('rowtimestamptzutc',to_utc_timestamp(F.col('rowtimestamp'),'UTC').cast(TimestampType())) 
  
  return dataframe

# COMMAND ----------

def transform_custom_mysql_lms_update_delta_col(dataframe, Params):
  
  lst_date_cols = Params[PARAMS_ADDITIONAL_PROPERTY].split(",")
  lst_date_cols = [item.strip() for item in lst_date_cols]

  for date_col in lst_date_cols:
    if date_col in dataframe.columns:
      print("Updating to datetime from unixtime for : " + date_col)
      dataframe = dataframe.withColumn(date_col, when(col(date_col) == "0", None).otherwise(from_unixtime(dataframe[date_col])))
      dataframe = dataframe.withColumn(date_col, dataframe[date_col].cast(TimestampType()))
      dataframe = dataframe.withColumn(date_col, from_utc_timestamp(dataframe[date_col], ADS_TZ_LOCAL)) #Convert from UTC to Local TZ
    
  return dataframe

# COMMAND ----------

def transform_custom_oneebs_transaction_date(dataframe):
  
  #If OneEBS source file has the additional column based on Audit table, the use this column for _transaction_date and drop the extra column
  
  if COL_ONEEBS_UPDATED_TIMESTAMP in dataframe.columns:
    print("Using the OneEBS updated transaction date")
    
    #If there are any miliseconds on the timestamp, round the data upto seconds only
    dataframe = dataframe.withColumn(COL_ONEEBS_UPDATED_TIMESTAMP, date_trunc("second", to_timestamp(dataframe[COL_ONEEBS_UPDATED_TIMESTAMP])))
    
    #Take the Greatest of Updated Date, Created Date, Audit TimeStamp as the _transaction_date column
    dataframe = dataframe.withColumn(ADS_COLUMN_TRANSACTION_DT, greatest(dataframe[ADS_COLUMN_TRANSACTION_DT], dataframe[COL_ONEEBS_UPDATED_TIMESTAMP]))
    
    #Drop the Source Updated Date column as we do not need this anymore
    dataframe = dataframe.drop(COL_ONEEBS_UPDATED_TIMESTAMP)
    
  return dataframe


  
