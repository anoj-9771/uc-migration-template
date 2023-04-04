# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

dftbl = sqlContext.sql("show tables from cleansed")
dftbl2 = dftbl.where(col('tableName').like('%z309%'))

tmplist = []
for row in dftbl2.collect():
    try:
        tmp = 'select count(*) myrowcnt from ' + row['database'] + '.' + row['tableName']
        tmpdf = sqlContext.sql(tmp)
        myrowcnt= tmpdf.collect()[0]['myrowcnt'] 
        tmplist.append((row['database'], row['tableName'],myrowcnt))
    except:
        tmplist.append((row['database'], row['tableName'],-1))

columns =  ['database', 'tableName', 'rowCount']     
df = spark.createDataFrame(tmplist, columns)    
display(df)

# COMMAND ----------

display(dftbl)

# COMMAND ----------


