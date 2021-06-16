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
    connection = (dbutils.secrets.get(scope='KeyVault',key=key))
    cons = connection.split(';')
    cons = cons
    for con in cons:
        value = con.split('=')
        try:
          DB_CONFIG[value[0].lower()] = value[1]
        except : False
    #jdbcHostname = dbutils.secrets.get(scope="KeyVaultAccess", key="HostAddress--AOSDB--Database")
    jdbcDatabase = DB_CONFIG['initial catalog']
    jdbcPort = 1433
    configJdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format( DB_CONFIG['server'], jdbcPort, jdbcDatabase, DB_CONFIG['user id'], DB_CONFIG['password'])
    return configJdbcUrl
