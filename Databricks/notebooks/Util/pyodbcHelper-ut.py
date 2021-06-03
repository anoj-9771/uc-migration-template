# Databricks notebook source
# DBTITLE 1,Create bash for pyodbc requirements. Use for initial cluster creation.
# dbutils.fs.put("dbfs:/databricks/pyodbc/insight.sh","""
# #!/bin/bash
# pip list | egrep 'thrift-sasl|sasl'
# pip install --upgrade thrift
# dpkg -l | egrep 'thrift_sasl|libsasl2-dev|gcc|python-dev'
# sudo apt-get -y install unixodbc-dev libsasl2-dev gcc python-dev
# curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# sudo apt-get update
# sudo ACCEPT_EULA=Y apt-get -q -y install msodbcsql17
# """,True)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, BooleanType, FloatType, LongType
import pyodbc
  
def connect_db(kvSecret):
    DB_CONFIG = {}
    key = kvSecret
    connection = dbutils.secrets.get(scope='insight-etlframework-akv',key=key)
    cons = connection.split(';')
    cons = cons
    for con in cons:
        value = con.split('=')
        try:
          DB_CONFIG[value[0]] = value[1]
        except : False
    server = DB_CONFIG['data source']
    database = DB_CONFIG['initial catalog']
    username = DB_CONFIG['user id']
    password = DB_CONFIG['password']
    driver= '{ODBC Driver 17 for SQL Server}'
    cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    return cursor

def get_data_profile(kvSecret, dataset_id):
    table = []
    query = "{call usp_GetDataProfile ('%s', %s, %s, %s)}"%(parameters['dstTableName'], parameters['batchId'], parameters['taskId'], dataset_id)
    cursor = connect_db(kvSecret)
    cursor.execute(query)
    data = cursor.fetchall()
    
#     Build Profile schema
    schema = StructType([
        StructField('batchid', LongType(), True),
        StructField('taskId', LongType(), True),
        StructField('datasetId', LongType(), True),
        StructField('schemaName', StringType(), True),
        StructField('tableName', StringType(), True),
        StructField('columnName', StringType(), True),
        StructField('rowCount', LongType(), True),
        StructField('size', FloatType (), True),
        StructField('type', StringType(), True),
        StructField('columnLength', IntegerType(), True),
        StructField('min', StringType(), True),
        StructField('max', StringType(), True),
        StructField('avg', FloatType(), True),
        StructField('stdev', FloatType(), True),
        StructField('nullCount', LongType(), True),
        StructField('distinctCount', LongType(), True),
        StructField('isPK', BooleanType(), True)
    ])
  
    # Convert pyodbc output to list
    for row in data:
        table.append([x for x in row])
        
    # Convert list to RDD
    rdd = spark.sparkContext.parallelize(table)

    # Create data frame
    return spark.createDataFrame(rdd,schema)
