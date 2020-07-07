# Databricks notebook source
class DbHelper:
  def __init__(self,kvSecret):
    self.kvSecret = kvSecret
  
  
  global DB_CONFIG 
  DB_CONFIG = {}
  

  ###
  ### Summary: Connects to SQL DB and returns the JDBC URL
  ###
  ### Parameters
  ### database: The database that we are requesting to connect to
  ###
  def connect_db(self):
    key = self.kvSecret
    connection = dbutils.secrets.get(scope='insight-etlframework-akv',key=key)
    cons = connection.split(';')
    cons = cons
    for con in cons:
        value = con.split('=')
        try:
          DB_CONFIG[value[0]] = value[1]
        except : False
    #jdbcHostname = dbutils.secrets.get(scope="KeyVaultAccess", key="HostAddress--AOSDB--Database")
    jdbcDatabase = DB_CONFIG['initial catalog']
    jdbcPort = 1433
    configJdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format( DB_CONFIG['data source'], jdbcPort, jdbcDatabase, DB_CONFIG['user id'], DB_CONFIG['password'])
    return configJdbcUrl
  

  ###
  ### Summary: Inserts a dataframe to SQL server table
  ###
  ### Parameters
  ### table: The table that we are inserting into
  ### df: Dataframe containing the values to insert
  ###
  def append_table(self,table,df):
    configJdbcUrl = self.connect_db()
    df.write\
    .format("jdbc")\
    .option("url",configJdbcUrl)\
    .option("numPartitions", 10)\
    .option("dbtable",table)\
    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
    .mode('append')\
    .save()
    print("Done inserting Into "+table + "...")

  ###
  ### Summary: Inserts a dataframe to SQL server table (Truncate)
  ###
  ### Parameters
  ### table: The table that we are inserting into
  ### df: Dataframe containing the values to insert
  ###
  def overwrite_table(self,table,df):
    configJdbcUrl = self.connect_db()
    df.write\
    .format("jdbc")\
    .option("url",configJdbcUrl)\
    .option("numPartitions", 10)\
    .option("dbtable",table)\
    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
    .mode('overwrite')\
    .save()
    print("Done inserting Into "+table + "...")
    
    
  ###
  #:param str table_name: Database table name 
  #:param str schema: Database Schema name
  #Get data from DB 
  #Return Dataframe
  ###
  def get_table(self,schema,table_name):
    jdbcUrl = self.connect_db()
    view_df = spark.read.jdbc(url=jdbcUrl, table='{0}.{1}'.format(schema,table_name), numPartitions=50)
    print('{0}.{1}'.format(schema,table_name), "Retrieved Successfully !")
    return view_df

# COMMAND ----------

# MAGIC %scala
