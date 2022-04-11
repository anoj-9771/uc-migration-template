# Databricks notebook source
# DBTITLE 1,Create Semantic schema if it doesn't exist
database_name = 'semantic'
query = "CREATE DATABASE IF NOT EXISTS {0}".format(database_name)
spark.sql(query)


# COMMAND ----------

# DBTITLE 1,Create views in Semantic layer for all dimensions, facts, bridge tables and views in Curated layer
import re
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").getOrCreate()

#List tables in curated layer
for table in spark.catalog.listTables("curated"):
    if table.name != '' and not table.name.startswith("vw") and table.name.startswith(("brg", "meter", "view", "dim", "fact")):
        table_name_seperated = ' '.join(re.sub( r"([A-Z])", r" \1", table.name).split())
        table_name_formatted = table_name_seperated[0:1].capitalize() + table_name_seperated[1:100]
        sql_statement = "create or replace view " + database_name + "." + table.name + " as select "
        #indexing the column
        curr_column = 0
        #list columns of the table selected
        for column in spark.catalog.listColumns(table.name, "curated"):
            #formatting column names ignoring metadata columns
            if not column.name.startswith("_"):
                curr_column = curr_column + 1
                if "SK" not in column.name:
                    col_name = column.name.replace("GUID","Guid").replace("ID","Id").replace("SCAMP","Scamp").replace("LGA", "Lga")
                    col_name_seperated = ' '.join(re.sub( r"([A-Z])", r" \1", col_name).split())
                    col_name_formatted = col_name_seperated[0:1].capitalize() + col_name_seperated[1:100].replace(" Guid"," GUID").replace(" Id"," ID")
                    col_name_formatted = col_name_formatted.replace("Scamp","SCAMP").replace("Lga","LGA")
                elif "SK" in column.name:
                    col_name_formatted = column.name
                if curr_column == 1:
                    sql_statement = sql_statement + " " + column.name + " as `" + col_name_formatted + "`"
                elif curr_column != 1:
                    sql_statement = sql_statement + " , " + column.name + " as `" + col_name_formatted + "`"
        sql_statement = sql_statement + "  from curated." + table.name + ";"
        #print(sql_statement)
        print(table.name)
        #executing the sql statement on spark creating the semantic view
        df = sqlContext.sql(sql_statement)

# COMMAND ----------

# DBTITLE 1,For Clean-up of views created in the environment
#for table in spark.catalog.listTables("semantic"):
#    if table.name.startswith("dim") or table.name.startswith("fact"):
#        print(table.name)
#        sqlContext.sql("DROP VIEW IF EXISTS "+database_name+"."+table.name)
