# Databricks notebook source
# DBTITLE 1,Create Semantic schema if it doesn't exist
# database_name = 'semantic'
# query = "CREATE DATABASE IF NOT EXISTS {0}".format(database_name)
# spark.sql(query)

# COMMAND ----------

import re
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").getOrCreate()

# COMMAND ----------

def is_uc():
    """check if the current databricks environemnt is Unity Catalog enabled"""
    try:
        dbutils.secrets.get('ADS', 'databricks-env')
        return True
    except Exception as e:
        return False

# COMMAND ----------

if is_uc():
    env = dbutils.secrets.get('ADS', 'databricks-env')
    source_catalog = f"{env}curated"
    target_catalog = f"{env}semantic"
    schema_list = []
    schemas = spark.sql(f"show schemas in {source_catalog}")
    for schema in schemas.collect():
        spark.sql(f"CREATE schema IF NOT EXISTS {target_catalog}.{schema.databaseName}")
        schema_list.append(schema.databaseName)
else:
    spark.sql("CREATE DATABASE IF NOT EXISTS semantic")
    schema_list = ['curated']

table_list = []
for schema in schema_list:
    if is_uc():
        qry = f"show tables in {source_catalog}.{schema}"
    else:
        qry = f"show tables in {schema}"
    tables = spark.sql(qry)
    for table in tables.collect():
        table_list.append(table)

# COMMAND ----------

# DBTITLE 1,Create views in Semantic layer for all dimensions, facts, bridge tables and views in Curated layer
#List tables in curated layer
# for table in spark.catalog.listTables("curated"):
for table in table_list:
    if table.tableName != '' and not table.tableName.startswith("vw") and table.tableName.startswith(("brg", "meter", "view", "dim", "fact")):
        table_name_seperated = ' '.join(re.sub( r"([A-Z])", r" \1", table.tableName).split())
        table_name_formatted = table_name_seperated[0:1].capitalize() + table_name_seperated[1:100]
        if is_uc:
            sql_statement = "CREATE OR REPLACE VIEW " + target_catalog + "." + table.database + "." + table.tableName + " as select "
        else:
            sql_statement = "CREATE OR REPLACE VIEW semantic." + table.tableName + " as select "
        #indexing the column
        curr_column = 0
        #list columns of the table selected
        if is_uc:
            qry = f"show columns in {source_catalog}.{table.database}.{table.tableName}"
        else:
            qry = f"show columns in curated.{table.tableName}"
        column_list = spark.sql(qry)
        for column in column_list.collect():
            # formatting column names ignoring metadata columns
            if not column.col_name.startswith("_") or column.col_name.startswith("_effective"):
                curr_column = curr_column + 1
                if "SK" not in column.col_name and not "_effective" not in column.col_name:
                    col_name = column.col_name.replace("GUID","Guid").replace("ID","Id").replace("SCAMP","Scamp").replace("LGA", "Lga")
                    col_name_seperated = ' '.join(re.sub( r"([A-Z])", r" \1", col_name).split())
                    col_name_formatted = col_name_seperated[0:1].capitalize() + col_name_seperated[1:100].replace(" Guid"," GUID").replace(" Id"," ID")
                    col_name_formatted = col_name_formatted.replace("Scamp","SCAMP").replace("Lga","LGA")
                elif "SK" in column.col_name or "_effective" in column.col_name:
                    col_name_formatted = column.col_name
                if curr_column == 1:
                    sql_statement = sql_statement + " " + column.col_name + " as `" + col_name_formatted + "`"
                elif curr_column != 1:
                    sql_statement = sql_statement + " , " + column.col_name + " as `" + col_name_formatted + "`"
        if is_uc:
            sql_statement = sql_statement + "  from " + source_catalog + "." + table.database + "." + table.tableName + ";"
            sql_statement = sql_statement.replace("CREATE OR REPLACE VIEW", "ALTER VIEW" if spark.sql(f"SHOW VIEWS FROM {target_catalog}.{table.database} LIKE '{table.tableName}'").count() == 1 else "CREATE OR REPLACE VIEW")
        else:
            sql_statement = sql_statement + "  from curated." + table.tableName + ";"
            sql_statement = sql_statement.replace("CREATE OR REPLACE VIEW", "ALTER VIEW" if spark.sql(f"SHOW VIEWS FROM {table.database} LIKE '{table.tableName}'").count() == 1 else "CREATE OR REPLACE VIEW")
        #executing the sql statement on spark creating the semantic view
        df = sqlContext.sql(sql_statement)

# COMMAND ----------

# # DBTITLE 1,Create views in Semantic layer for all dimensions, facts, bridge tables and views in Curated layer
# Commented the content of this cell as part of the UC migration code refactoring and added the updated code in the cell above.
# import re
# from pyspark.sql import SQLContext
# sqlContext = SQLContext(sc)

# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("test").getOrCreate()

# #List tables in curated layer
# for table in spark.catalog.listTables("curated"):
#     if table.name != '' and not table.name.startswith("vw") and table.name.startswith(("brg", "meter", "view", "dim", "fact")):
#         table_name_seperated = ' '.join(re.sub( r"([A-Z])", r" \1", table.name).split())
#         table_name_formatted = table_name_seperated[0:1].capitalize() + table_name_seperated[1:100]
#         sql_statement = "CREATE OR REPLACE VIEW " + database_name + "." + table.name + " as select "
#         #indexing the column
#         curr_column = 0
#         #list columns of the table selected
#         for column in spark.catalog.listColumns(table.name, "curated"):
#             #formatting column names ignoring metadata columns
#             if not column.name.startswith("_"):
#                 curr_column = curr_column + 1
#                 if "SK" not in column.name:
#                     col_name = column.name.replace("GUID","Guid").replace("ID","Id").replace("SCAMP","Scamp").replace("LGA", "Lga")
#                     col_name_seperated = ' '.join(re.sub( r"([A-Z])", r" \1", col_name).split())
#                     col_name_formatted = col_name_seperated[0:1].capitalize() + col_name_seperated[1:100].replace(" Guid"," GUID").replace(" Id"," ID")
#                     col_name_formatted = col_name_formatted.replace("Scamp","SCAMP").replace("Lga","LGA")
#                 elif "SK" in column.name:
#                     col_name_formatted = column.name
#                 if curr_column == 1:
#                     sql_statement = sql_statement + " " + column.name + " as `" + col_name_formatted + "`"
#                 elif curr_column != 1:
#                     sql_statement = sql_statement + " , " + column.name + " as `" + col_name_formatted + "`"
#         sql_statement = sql_statement + "  from curated." + table.name + ";"
#         #print(sql_statement)
#         sql_statement = sql_statement.replace("CREATE OR REPLACE VIEW", "ALTER VIEW" if spark.sql(f"SHOW VIEWS FROM {database_name} LIKE '{table.name}'").count() == 1 else "CREATE OR REPLACE VIEW")
#         print(table.name)
#         #executing the sql statement on spark creating the semantic view
#         df = sqlContext.sql(sql_statement)

# COMMAND ----------

# DBTITLE 1,For Clean-up of views created in the environment
#for table in spark.catalog.listTables("semantic"):
#    if table.name.startswith("dim") or table.name.startswith("fact"):
#        print(table.name)
#        sqlContext.sql("DROP VIEW IF EXISTS "+database_name+"."+table.name)
