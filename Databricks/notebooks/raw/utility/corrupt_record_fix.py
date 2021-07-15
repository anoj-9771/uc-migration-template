# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import regexp_replace
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, StringType
import math

# COMMAND ----------

def fix_corrupt_record(corrupt_records):
  print ("Starting : fix_corrupt_record")
  #Sometimes there can be data loading issues while loading data from flat file to parquet
  #The invalid columns are then stored in the Databricks internal _corrupt_record column
  #The below routine will fix this and save them back as separate columns
  
  real_col_names = {}
  sample_record = corrupt_records.select('_corrupt_record').limit(1).collect()[0][0]
  record = sample_record.split("\",\"")
  for j in range(len(record)):
    index = record[j].find("\":\"")
    key = record[j][:index]
    if '{"' in key: key = key[2:]
    real_col_names['tmp_col_' + str(j)] = str(key)
    last_splitted_column = str(key)
   
  corrupt_records_splitted = None
  
  split_col = F.split(corrupt_records['_corrupt_record'], '","')
  
  corrupt_records_splitted = corrupt_records.withColumn('col_' + str(0), split_col.getItem(0))
  for i in range(1, len(real_col_names)):
    corrupt_records_splitted = corrupt_records_splitted.withColumn('col_' + str(i), split_col.getItem(i))
  
  split_col = F.split(corrupt_records_splitted['col_0'], '":"')
  corrupt_records_splitted = corrupt_records_splitted.withColumn('tmp_col_0', split_col.getItem(1))
  corrupt_records_splitted = corrupt_records_splitted.withColumnRenamed('tmp_col_0', real_col_names['tmp_col_0'])
  for i in range(1, len(real_col_names)):
    split_col = F.split(corrupt_records_splitted['col_' + str(i)], '":"')
    corrupt_records_splitted = corrupt_records_splitted.withColumn('tmp_col_' + str(i), split_col.getItem(1))
    corrupt_records_splitted = corrupt_records_splitted.withColumnRenamed('tmp_col_' + str(i), real_col_names['tmp_col_' + str(i)])
  
  # print (last_splitted_column)
  corrupt_records_splitted = corrupt_records_splitted.withColumn(last_splitted_column, regexp_replace(last_splitted_column, '"}', ''))
  
  cols_should_drop = [col for col in corrupt_records_splitted.columns if col.startswith('col_')]
  for i in range(len(cols_should_drop)):
    corrupt_records_splitted = corrupt_records_splitted.drop(cols_should_drop[i])
  
  corrupt_records_splitted = corrupt_records_splitted.drop('_corrupt_record')
  
  print ("Corrupt Records Splitted: " + str(corrupt_records_splitted.count()))

  corrupt_records_splitted = corrupt_records_splitted.select(sorted(corrupt_records_splitted.columns))
  
  print ("Finishing : fix_corrupt_record")
  return corrupt_records_splitted
