# Databricks notebook source
# MAGIC %md
# MAGIC * Import Table metadata blob if exists
# MAGIC * Build create table query
# MAGIC * Execute query

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import col, max as sparkMax, length, when
from pyspark.sql.functions import monotonically_increasing_id
import math
import numpy as np
from pyspark.sql import Window
import re


o_lookup = {
    "^BFILE$":"[VARBINARY](MAX)",\
    "^BLOB$":"[VARBINARY](MAX)",\
    "^CHAR$":"[CHAR]",\
    "^CLOB$":"[VARCHAR](MAX)",\
    "^DATE$":"[DATETIME]",\
    "^FLOAT$":"[FLOAT]",\
    "^INT$":"[NUMERIC](38)",\
    "^INTERVAL$":"[DATETIME]",\
    "^LONG$":"[BIGINT]",\
    "^LONG\sRAW$":"[IMAGE]",\
    "^NCHAR$":"[NCHAR]",\
    "^NCLOB$":"[NVARCHAR](MAX)",\
    "^NUMBER$":"[FLOAT]",\
    "^NUMBER$":"[NUMERIC]",\
    "^NVARCHAR2$":"[NVARCHAR]",\
    "^RAW$":"[VARBINARY]",\
    "^REAL$":"[FLOAT]",\
    "^ROWID$":"[CHAR](18)",\
    "^TIMESTAMP$":"[DATETIME]",\
    "^TIMESTAMP[\(](.*?)[\)]$":"[DATETIME]",\
    "^TIMESTAMP[\(](.*?)[\)]\sWITH\sTIME\sZONE$":"[VARCHAR](37)",\
    "^TIMESTAMP[\(](.*?)[\)]\sWITH\sLOCAL\sTIME\sZONE$":"[VARCHAR](37)",\
    "^UROWID$":"[CHAR](18)",\
    "^VARCHAR2$":"[VARCHAR]"
  }


def recastTable(df_rc, schm):
    for c in np.array(schm.select("Column_Name","Column_DataType").sort(col("Row_Id").asc()).collect()) :
        cn = c[0].replace(']','').replace('[','')
        dt = re.sub('(\((.*?)\))', '',c[1].lower().replace(']','').replace('[',''))
        if dt in 'varchar char nvarchar text nchar ntext':
          df_rc = df_rc.withColumn(cn, df_rc[cn].cast(StringType()))
        if dt == 'bit':
          df_rc = df_rc.withColumn(cn, df_rc[cn].cast(BooleanType()))
        if dt == 'int':
          df_rc = df_rc.withColumn(cn, df_rc[cn].cast(IntegerType()))
        if dt == 'tinyin':
          df_rc = df_rc.withColumn(cn, df_rc[cn].cast(ByteType()))
        if dt == 'smallint':
          df_rc = df_rc.withColumn(cn, df_rc[cn].cast(ShortType()))
        if dt in 'decimal':
          df_rc = df_rc.withColumn(cn, df_rc[cn].cast(dt))
#           df_rc = df_rc.withColumn(cn,df_rc[cn].cast(DecimalType()))
        if dt == 'money':
          df_rc = df_rc.withColumn(cn, df_rc[cn].cast(DecimalType(10,2)))
        if dt in 'numeric float real':
          df_rc = df_rc.withColumn(cn, df_rc[cn].cast(DoubleType()))
#           df = df.withColumn(cn, df[cn].cast("float"))
        if dt == 'date':
          df_rc = df_rc.withColumn(cn, df_rc[cn].cast(DateType()))
        if dt == 'datetime':
          df_rc = df_rc.withColumn(cn, df_rc[cn].cast(TimestampType()))
        if dt == 'time':
          df_rc = df_rc.withColumn(cn, df_rc[cn].cast(TimestampType()))
        if dt == 'timestamp':
          df_rc = df_rc.withColumn(cn, df_rc[cn].cast(TimestampType()))
        if dt in 'binary vbinary image':
          df_rc = df_rc.withColumn(cn, df_rc[cn].cast(BinaryType()))
    return df_rc

def createTable(df, dstTableName, s_type):
#     df_utc = sqlContext.createDataFrame([(dstTableName, '[DSS_UPDATE_TIME]','[DATETIME2]','','NOT NULL')])
#     df = df.union(df_utc)
#     concat_udf = F.udf(lambda cols: " ".join([x if x is not None else "" for x in cols]), StringType())
#     c = df.withColumn("query", concat_udf(F.array('Column_Name', 'Column_DataType', 'Column_Collate', 'Column_Nullable')))
#     order_c = c.orderBy(['Row_Id'],ascending = True)
    view = dstTableName.replace('.', '_')
    rn = 1
    columns = ''
#     #Map Oracle data types to SQL
#     if s_type == 'Oracle':
#         for o_dt in df.select("Column_DataType").collect():
#           for sql_dt in (o_lookup[key] for key in o_lookup if re.match(key, o_dt[0])):  
#               df = df.withColumn("Column_DataType", \
#                       when(col("Column_DataType") == o_dt[0], sql_dt).otherwise(col("Column_DataType")))
    
    df.createOrReplaceTempView(view)
    
    for row in df.collect():
        query =spark.sql("select concat(Column_Name, ' ',  \
        Column_DataType, ' ', \
        coalesce(Column_Nullable, '')) from %s where Row_Id = %s"%(view, rn)).collect()[0][0]
        if rn == 1:
          columns = columns + query
        else: 
          columns = columns + ', ' + query
        rn = rn + 1
    query = 'create table %s (%s, [DSS_UPDATE_TIME] [DATETIME] NOT NULL)'%(dstTableName, columns.replace('NOT NULL', 'NULL'))
    if query in ['[GEOGRAPHY]']:
        query.replace('[GEOGRAPHY]', '[VARCHAR](250)')
    
    return query

def mergeTable(df, dstTableName):
#     df_utc = sqlContext.createDataFrame(dstTableName, '[DSS_UPDATE_TIME]','[DATETIME2]','','NOT NULL')
#     df = df.union(df_utc)
    tempTable = 'tmp.' + dstTableName.replace('.', '_')
    df_schema.createOrReplaceTempView('df')
    concat = spark.sql("select Column_Name, concat('convert(' , Column_DataType, ', ', Column_Name, ')') as Column_Convert from df")
    concat_udf = F.udf(lambda cols: " ".join([x if x is not None else "" for x in cols]), StringType())
    insert = concat.withColumn("query", concat_udf(F.array('Column_Name')))
    select = concat.withColumn("query", concat_udf(F.array('Column_Convert')))
    columns = insert.agg(F.concat_ws(", ", F.collect_list(insert.query))).collect()[0][0]
    convert = select.agg(F.concat_ws(", ", F.collect_list(select.query))).collect()[0][0]
    query = 'insert into %s (%s) select %s from %s'%(dstTableName, columns, convert, tempTable)
    print(query)

  
def vRound(x, base=5):
  return base * int(math.ceil(x/base))
  
def createSchemaDataFrame(df, dstSchemaName, dstTableName):
    schema = StructType([StructField('Table_Name', StringType(), False),\
                         StructField('Column_Name', StringType(), False),\
                         StructField('Column_DataType', StringType(), False),\
                         StructField('Column_Collate', StringType(), True),\
                         StructField('Column_Nullable', StringType(), True)])
    df0 = sqlContext.createDataFrame(sc.emptyRDD(), schema)
    tableName = '%s.%s'%(dstSchemaName, dstTableName)
    rn = 1
    for t in df.dtypes:
        df_add = sqlContext.createDataFrame(sc.emptyRDD(), schema)
        if t[1].lower() == 'string':
            rc = df.agg(sparkMax(length(col(t[0]))).cast(IntegerType()).alias('RowCount')).collect()[0].asDict()['RowCount']
            if rc:
              if (rc * 1.2) < 100:
                  df_add = spark.createDataFrame([(tableName, t[0],'[VARCHAR](%i)'%vRound(rc * 1.2, 100), '', '')])
              elif (rc * 1.2) < 8000 and (rc * 1.2) > 100:  
                  df_add = spark.createDataFrame([(tableName, t[0],'[VARCHAR](%i)'%vRound(rc * 1.2,50), '', '')])
              elif (rc * 1.2) > 8000:  
                  df_add = spark.createDataFrame([(tableName, t[0],'[VARCHAR](MAX)', '', '')])
            else: df_add = spark.createDataFrame([(tableName, t[0],'[VARCHAR](MAX)', '', '')])
        elif t[1].lower() in ['long', 'bigint']:
            df_add = spark.createDataFrame([(tableName, t[0],'[BIGINT]', '', '')])
        elif t[1].lower() in ['byte', 'short', 'int']:
            df_add = spark.createDataFrame([(tableName, t[0],'[INT]', '', '')])
        elif t[1].lower() in 'float double decimal':
            df_add = spark.createDataFrame([(tableName, t[0],'[FLOAT]', '', '')])
        elif t[1].lower() == 'binary':
            df_add = spark.createDataFrame([(tableName, t[0],'[BINARY]' '', '')])
        elif t[1].lower() == 'boolean':
            df_add = spark.createDataFrame([(tableName, t[0],'[BIT]', '', '')])
        elif t[1].lower() in ['datetime','timestamp']:
            df_add = spark.createDataFrame([(tableName, t[0],'[DATETIME]', '', '')])
        elif t[1].lower() in 'array maptype structfield':
            df_add = spark.createDataFrame([(tableName, t[0],'[VARCHAR](MAX)', '', '')])
        df0 = df0.union(df_add)
    df1 = df0.withColumn('Row_Order', monotonically_increasing_id())
    window = Window.orderBy(F.col('Row_Order'))
    df2 = df1.withColumn('Row_Id', F.row_number().over(window))
    return df2.select("Row_Id", "Table_Name", "Column_Name", "Column_DataType", "Column_Collate", "Column_Nullable")

# COMMAND ----------

# from pyspark.sql import types 
# for t in ['BinaryType', 'BooleanType', 'ByteType', 'DateType', 
#           'DecimalType(5,2)', 'DoubleType', 'FloatType', 'IntegerType', 
#            'LongType', 'ShortType', 'StringType', 'TimestampType']:
#     print(f"{t}: {getattr(types, t)().simpleString()}")
