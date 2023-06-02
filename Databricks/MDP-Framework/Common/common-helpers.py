# Databricks notebook source
import json
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

def ExpandTable(df, includeParentNames = False, sep = "_", excludeColumns = "", originalDataFrame = True):
    newDf = df
    replacementToken = "$original$"
    
    #rename original columns that are not structure types to avoid column renaming where the same column name exists at the top level of a json object and within a sub json column 
    if originalDataFrame:
        for i in df.dtypes:
            columnName = i[0]
            if not "struct" in i[1]:
                newDf = newDf.withColumnRenamed(columnName, f"{columnName}{replacementToken}")
    
    for i in df.dtypes:
        columnName = i[0]
        
        #skip explosion if column name in comma delimitted exclude column list
        if columnName.lower() in excludeColumns.lower().split(","):
            continue
        
        #if the column is a structure data type loop through and explode appending the parent column name to the root object
        if i[1].startswith("struct"):
            newDf = newDf.selectExpr("*", f"`{columnName}`.*")
            if includeParentNames:
                for c in newDf.selectExpr(f"`{columnName}`.*").columns:
                    newDf = newDf.withColumnRenamed(c, f"{columnName}{sep}{c}".replace("__", "_"))
            newDf = newDf.drop(columnName)
            return ExpandTable(newDf, includeParentNames, sep, excludeColumns, False)
        
        if i[1].startswith("array") and "struct" in i[1]:
            explodedDf = newDf.withColumn(f"{columnName}", expr(f"explode(`{columnName}`)"))
            newDf = explodedDf.selectExpr("*", f"`{columnName}`.*")
            for c in explodedDf.selectExpr(f"`{columnName}`.*").columns:
                newDf = newDf.withColumnRenamed(c, f"{columnName}{sep}{c}".replace("__", "_"))
            newDf = newDf.drop(columnName, columnName)
            return ExpandTable(newDf, includeParentNames, sep, excludeColumns, False)
    
    #fix the colum renaming after initial explosion has happened
    for i in newDf.dtypes:
        columnName = i[0]
        if replacementToken in columnName:
            newDf = newDf.withColumnRenamed(columnName, columnName.replace(replacementToken,""))
    return newDf

# COMMAND ----------

def LoadJsonFile(path):
    f = open(path)
    data = json.load(f)
    f.close()
    return data

# COMMAND ----------

def DataFrameFromFilePath(path):
    fsSchema = StructType([
        StructField('path', StringType()),
        StructField('name', StringType()),
        StructField('size', LongType()),
        StructField('modificationTime', LongType())
    ])
    list = dbutils.fs.ls(path)
    df = spark.createDataFrame(list, fsSchema).withColumn("modificationTime", expr("from_unixtime(modificationTime / 1000)"))
    return df


# COMMAND ----------

csv_path = "/mnt/datalake-raw/cleansed_csv/curated_mapping.csv"

# COMMAND ----------

def is_uc():
    """check if the current databricks environemnt is Unity Catalog enabled"""
    try:
        dbutils.secrets.get('ADS', 'databricks-env')
        return True
    except Exception as e:
        return False

# COMMAND ----------
  
def lookup_curated_namespace(env:str, current_database_name: str, current_table_name: str, csv_path:str) -> str:
    """looks up the target table namespace based on the current_table_name provided. note that this function assumes that there are no duplicate 'current_table_name' entries in the excel sheet."""
    future_namespace = {}
    try:
        p_df = pd.read_csv(csv_path)
        future_database_name = p_df[(p_df['current_table_name'] == current_table_name) & (p_df['current_database_name'].str.contains('curated'))]['future_database_name'].tolist()[0]
        future_table_name = p_df[(p_df['current_table_name'] == current_table_name) & (p_df['current_database_name'].str.contains('curated'))]['future_table_name'].tolist()[0]
        future_namespace['database_name'] = future_database_name
        future_namespace['table_name'] = future_table_name
    except Exception as e:
        future_namespace['database_name'] = 'dim' if 'dim' in current_table_name else 'fact' if 'fact' in current_table_name else 'uncategorized'
        future_namespace['table_name'] = current_table_name.replace('dim', '') if 'dim' in current_table_name else current_table_name.replace('fact', '') if 'fact' in current_table_name else current_table_name
        print (f'Warning! Issue occurred while looking up the future namespace for table: {current_database_name}.{current_table_name}')
    return future_namespace

# COMMAND ----------

def get_table_name(layer:str, j_schema: str, j_table: str) -> str:
    """gets correct table namespace based on the UC migration/databricks-env secret being available in keyvault."""
    try:
        env = dbutils.secrets.get('ADS', 'databricks-env')
        return f"{env}{layer}.{j_schema}.{j_table}"
    except Exception as e:
        return f"{layer}.{j_schema}_{j_table}"

# COMMAND ----------
#    
def get_table_namespace(layer:str, table: str) -> str:
    """gets correct table namespace based on the UC migration/databricks-env secret being available in keyvault, used primarily for pipelines other than raw and cleansed ETL"""
    if is_uc():
        env = dbutils.secrets.get('ADS', 'databricks-env')
        if layer == 'raw' or layer == 'cleansed':
            #use pattern to convert raw.source_tablename to raw.source.table_name
            catalog_name = f'{env}{layer}'
            db_name = table.split('_')[0]
            table_name = '_'.join(table.split('_')[1:])
            return f'{catalog_name}.{db_name}.{table_name}'
        elif 'curated' in layer or 'semantic' in layer:
            #use lookup_curated_namespace to find the target database and table based on mapping sheet
            catalog_name = f'{env}{layer}'
            db_name = lookup_curated_namespace(env, layer, table, csv_path)['database_name']
            table_name = lookup_curated_namespace(env, layer, table, csv_path)['table_name']
            return f'{catalog_name}.{db_name}.{table_name}'
        elif layer == 'datalab':
            #datalab schemas are named after environment except for prepord and prod
            trimmed_env = env.replace('_', '')
            catalog_name = layer
            db_name = 'swc' if trimmed_env == '' else 'preprod' if trimmed_env == 'ppd' else f'{trimmed_env}' 
            return f'{layer}.{db_name}.{table}'
        else:
            #stage/rejected tables follow slightly different database format to datalab
            trimmed_env = env.replace('_', '')
            return f'{layer}.{trimmed_env}.{table}'
    else:
         return f"{layer}.{table}"
    

# COMMAND ----------

def ConvertBlankRecordsToNull(df):
    for i in df.dtypes:
        if "struct" not in i[1]:
            df = df.withColumn(i[0],when(col(i[0])=="" ,None).otherwise(col(i[0])))
    return df
