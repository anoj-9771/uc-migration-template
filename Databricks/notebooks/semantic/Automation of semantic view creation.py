# Databricks notebook source
# DBTITLE 1,Create Semantic schema if it doesn't exist
database_name = 'semantic'
query = "CREATE DATABASE IF NOT EXISTS {0}".format(database_name)
spark.sql(query)


# COMMAND ----------

# DBTITLE 1,Lists all dimensions and facts in Curated layer, and create Semantic views in the Target environment
import re
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").getOrCreate()

#List tables in curated layer
for table in spark.catalog.listTables("curated"):    
    #list of dimensions and formatting the table names
  if table.name != '' and "dim" in table.name and not table.name.startswith("vw"):
    table_name_seperated = ' '.join(re.sub( r"([A-Z])", r" \1", table.name).split())
    table_name_formatted = table_name_seperated[0:1].capitalize() + table_name_seperated[1:100]
    sql_statement = "create or replace view semantic." + table.name + " as select "
    #list columns of the table selected
    for column in spark.catalog.listColumns(table.name, "curated"):
        #formatting column names ignoring metadata columns and SK column
      if "_" not in column.name and "SK" not in column.name and "LGA" not in column.name:
        col_name = column.name.replace("ID","Id")
        col_name_seperated = ' '.join(re.sub( r"([A-Z])", r" \1", col_name).split())
        col_name_formatted = col_name_seperated[0:1].capitalize() + col_name_seperated[1:100].replace(" Id"," ID")
        #constructing the sql statement
        sql_statement = sql_statement + "  , " + column.name + " as `" + col_name_formatted + "`"
      elif "_" not in column.name and "SK" in column.name:
        s = column.name
        sql_statement = sql_statement + "   " + s + " as " + s
      elif "_" not in column.name:
        s = column.name
        sql_statement = sql_statement + "  , " + s + " as `" + s + "`"    
    sql_statement = sql_statement + " from curated." + table.name + "; " 
    print(table.name)
    #executing the sql statement on spark creating the semantic view
    df = sqlContext.sql(sql_statement)
    #list of facts and formatting the table names
  elif table.name != '' and "fact" in table.name:
    table_name_seperated = ' '.join(re.sub( r"([A-Z])", r" \1", table.name).split())
    table_name_formatted = table_name_seperated[0:1].capitalize() + table_name_seperated[1:100]
    sql_statement = "create or replace view semantic." + table.name + " as select "
    #indexing the column
    curr_column = 0
    #list columns of the table selected
    for column in spark.catalog.listColumns(table.name, "curated"):
        curr_column = curr_column + 1
               
        #formatting column names ignoring metadata columns and SK column
        if "_" not in column.name and "SK" not in column.name:
            col_name = column.name.replace("ID","Id")
            col_name_seperated = ' '.join(re.sub( r"([A-Z])", r" \1", col_name).split())
            col_name_formatted = col_name_seperated[0:1].capitalize() + col_name_seperated[1:100].replace(" Id"," ID")
            if curr_column == 1:
                sql_statement = sql_statement + " " + column.name + " as `" + col_name_formatted + "`" 
            elif curr_column != 1:
                sql_statement = sql_statement + " , " + column.name + " as `" + col_name_formatted + "`" 
                 
        elif "_" not in column.name and "SK" in column.name and "fact" in column.name:
            s = column.name
            sql_statement = sql_statement + "  " + s + " as " + s
        elif "_" not in column.name:
            s = column.name 
            sql_statement = sql_statement + "  , " + s + " as " + s
    sql_statement = sql_statement + "  from curated." + table.name + ";"
    print(table.name)
    #executing the sql statement on spark creating the semantic view
    df = sqlContext.sql(sql_statement)

# COMMAND ----------

# DBTITLE 1,For Clean-up of views created in the environment
#for table in spark.catalog.listTables("semantic"):
#    if table.name.startswith("dim") or table.name.startswith("fact"):
#        print(table.name)
#        sqlContext.sql("DROP VIEW IF EXISTS "+database_name+"."+table.name)
