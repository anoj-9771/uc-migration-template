# Databricks notebook source
# MAGIC %run ./global-variables-python

# COMMAND ----------

def AzSqlGetDBConnectionURL():
  
  DB_SERVER_FULL = ADS_DB_SERVER + ".database.windows.net"
  DB_USER_NAME = ADS_DATABASE_USERNAME + "@" + ADS_DB_SERVER

  #SQL Database related settings
  DB_PASSWORD = dbutils.secrets.get(scope = ADS_KV_ACCOUNT_SCOPE, key = ADS_KV_DB_PWD_SECRET_KEY)
  JDBC_PORT =  "1433"
  JDBC_EXTRA_OPTIONS = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
  SQL_URL = "jdbc:sqlserver://" + DB_SERVER_FULL + ":" + JDBC_PORT + ";database=" + ADS_DATABASE_NAME + ";user=" + DB_USER_NAME+";password=" + DB_PASSWORD + ";" + JDBC_EXTRA_OPTIONS
  SQL_URL_SMALL = "jdbc:sqlserver://" + DB_SERVER_FULL + ":" + JDBC_PORT + ";database=" + ADS_DATABASE_NAME + ";user=" + DB_USER_NAME+";password=" + DB_PASSWORD
  
  return SQL_URL

# COMMAND ----------

def AzSqlLoadDataToDB(dataframe, tablename, writemode):
  print("Starting : AzSqlLoadDataToDB : Writing data to table " + tablename + " with mode " + writemode)
  
  SQL_URL = AzSqlGetDBConnectionURL()

  print(SQL_URL)
  print("Writing data to table " + tablename + " with mode " + writemode)
  
  dataframe.write \
  .mode(writemode) \
  .jdbc(SQL_URL, tablename, mode=writemode)

  print("Finished AzSqlLoadDataToDB : writing data to table " + tablename + " with mode " + writemode)

# COMMAND ----------

def AzSqlExecTSQL(query):
  
  #Execute the Scala notebook to run the Query on SQLEDW
  dbutils.notebook.run("/build/includes/scala-executors/exec-tsql-edw", 0, {"query":query})

# COMMAND ----------

def AzSqlGetData(query):
  
  #Method to return data from SQL Server EDW
  #Query can be either table name or a SELECT Query
  
  #Trim the query at the beginning in case there are blank spaces
  query = query.strip()
  
  if query.upper().startswith("SELECT "):
    #If it is a SQL Query then wrap the query in a bracket and alias it as per SQL JDBC requirement
    query = "({}) qry".format(query)
  
  #Get the SQL JDBC URL
  SQL_URL = AzSqlGetDBConnectionURL()
  print(SQL_URL)
  
  print(query)
  
  #Run the Query  
  df = spark.read.jdbc(url=SQL_URL, table=query)
  
  return df

# COMMAND ----------


