# Databricks notebook source
def DeleteDirectoryRecursive(dirname):
    files=dbutils.fs.ls(dirname)
    for f in files:
        if f.isDir():
            DeleteDirectoryRecursive(f.path)
        dbutils.fs.rm(f.path, recurse=True)
    dbutils.fs.rm(dirname, True)
    
def CleanTable(tableNameFqn):
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {tableNameFqn}").collect()[0]
        DeleteDirectoryRecursive(detail.location)
    except:    
        pass
    try:
        spark.sql(f"DROP TABLE {tableNameFqn}")
    except:
        pass

# COMMAND ----------

CleanTable('{ADS_DATABASE_CLEANSED}.access.facilityTimeslice')
dbutils.notebook.run("./facilityTimeslice_Fix", 60*60)

# COMMAND ----------

CleanTable('{ADS_DATABASE_CLEANSED}.access.meterTimeslice')
dbutils.notebook.run("./meterTimeslice_Fix", 60*60)

# COMMAND ----------

CleanTable('{ADS_DATABASE_CLEANSED}.access.propertyAddressTimeslice')
dbutils.notebook.run("./propertyAddressTimeslice", 60*60)

# COMMAND ----------

CleanTable('{ADS_DATABASE_CLEANSED}.access.propertyLotTimeslice')
dbutils.notebook.run("./propertyLotTimeslice_Fixed", 60*60)

# COMMAND ----------

CleanTable('{ADS_DATABASE_CLEANSED}.access.propertyTimeslice')
dbutils.notebook.run("./propertyTimeslice_Fixed", 60*60)

# COMMAND ----------

CleanTable('{ADS_DATABASE_CLEANSED}.access.propertyTypeTimeslice')
dbutils.notebook.run("./propertyTypeTimeslice_Fixed", 60*60)
