# Databricks notebook source
# MAGIC %md
# MAGIC * Build Dataframe based on sourceType

# COMMAND ----------

import json
import pandas as pn
import io
import csv
import gzip

def CreateDataFrame(srcBlobPath, srcFormat, query, fullBlobName):
    if srcFormat in ['.csv', '.csv.gz']:
        df = CreateCSVDataFrame(srcBlobPath,  srcFormat)
        return df
    elif srcFormat == '.json':
        df = CreateJSONDataFrame(srcBlobPath, srcFormat) 
        return df
    elif srcFormat in ['.xlsx', '.xlx']:
        df = CreateEXCELDataFrame(srcBlobPath, srcFormat, query, fullBlobName)
        return df
#     if srcFormat == '.xlx':        
    elif srcFormat == '.parquet':
        df = CreatePARQUETDataFrame(fullPath)
        return df

# COMMAND ----------

def CreateCSVDataFrame(srcBlobPath, srcFormat):
    fullPath = srcBlobPath + srcFormat
    df = spark.read \
    .option("header","true") \
    .option("inferSchema","true") \
    .option("delimiter","|") \
    .option("parserLib","UNIVOCITY") \
    .option("multiLine", "true") \
    .option("ignoreLeadingWhiteSpace","true") \
    .option("ignoreTrailingWhiteSpace","true") \
    .option("comment","+") \
    .csv(fullPath)
    return df

def CreateJSONDataFrame(srcBlobPath, srcFormat):
    fullPath = srcBlobPath + srcFormat
    df = spark.read.option("multiline", "true").json(fullPath)
    return df

def CreateEXCELDataFrame(srcBlobPath, srcFormat, query, fullBlobName):
    fullPath = srcBlobPath + srcFormat
    location = "tmp/Excel/%s"%(fullBlobName)
    tmp_location_w = 'dbfs:/%s'%(location)
    tmp_location_r = '/dbfs/%s'%(location)
    xl = json.loads(query)
    dbutils.fs.cp(fullPath, "%s%s"%(tmp_location_w, srcFormat))
    pd_df = pn.read_excel(io = "%s%s"%(tmp_location_r, srcFormat), \
      sheet_name = xl["sheet_name"], \
      header = xl["header"], \
      names = xl["names"], \
      index_col = xl["index_col"], \
      usecols = xl["usecols"], \
      squeeze = xl["squeeze"], \
      dtype = xl["dtype"], \
      engine = xl["engine"], \
      converters = xl["converters"], \
      true_values = xl["true_values"], \
      false_values = xl["false_values"], \
      skiprows = xl["skiprows"], \
      nrows = xl["nrows"], \
      na_values = xl["na_values"], \
      keep_default_na = xl["keep_default_na"], \
      verbose = xl["verbose"], \
      parse_dates = xl["parse_dates"], \
      date_parser = xl["date_parser"], \
      thousands = xl["thousands"], \
      comment = xl["comment"], \
      skip_footer = xl["skip_footer"], \
      convert_float = xl["convert_float"], \
      mangle_dupe_cols = xl["mangle_dupe_cols"])#, \
     # kwds = xl["kwds"] or None)  
    pd_df.columns = pd_df.columns.str.replace(' ', '_')
#     csv_io = io.StringIO()
#     pd_df.to_csv('%s.csv.gz'%(tmp_location_r),
#       sep='|',
#       header=True,
#       index=False,
# #       quoting=csv.QUOTE_ALL,
#       compression='gzip',
#       quotechar='"',
#       doublequote=True,
#       line_terminator='\r\n')
#     df =   CreateCSVDataFrame(tmp_location_w, '.csv.gz')
    df = spark.createDataFrame(pd_df.astype(str))
    df = df.replace('nan', None)
    df.coalesce(1).write.mode('overwrite') \
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec") \
      .option("header","true") \
      .option("delimiter","|") \
      .option("comment","+") \
      .csv(srcBlobPath + '.csv.gz')
    df =   CreateCSVDataFrame(srcBlobPath, '.csv.gz')
    #dbutils.fs.cp(tmp_location_w, '%s.csv.gz'%(srcBlobPath))
    #dbutils.fs.rm('%s.csv.gz'%(tmp_location_w))
    return df

def CreatePARQUETDataFrame(srcBlobPath, srcFormat):
  return None
