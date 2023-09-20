# Databricks notebook source
#Function: verifyTableSchema(table as string, new structure definition as StructType)<br />
#<p>
#This function will allow you to verify that the structure definition in the code matches the table definition. 
#If it doesn't it will generate and execute ALTER statements to update the table schema
#
#Fix 1.1 :- The schema output is different between DBR10.4 and DBR12.2, to accommodate the changed schema output this function had to be fixed
#Schema output in DBR10.4 : [StructField(<col1>,<datatype>,<true/false>), StructField(<col2>,<datatype>,<true/false>)]
#Schema output in DBR12.2 : [StructField('<col1>', <datatype>(), <True/False>), StructField('<col2>', <datatype>(), <True/False>)]
#

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# MAGIC %run ./global-variables-python

# COMMAND ----------

csv_path = "/mnt/datalake-raw/cleansed_csv/curated_mapping.csv"
    

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
        future_namespace['database_name'] = 'dim' if 'dim' in current_table_name else 'fact' if 'fact' in current_table_name else 'bridge' if 'bridge' in current_table_name else 'uncategorized'
        future_namespace['table_name'] = current_table_name.replace('dim', '') if 'dim' in current_table_name else current_table_name.replace('fact', '') if 'fact' in current_table_name else current_table_name
        print (f'Warning! Issue occurred while looking up the future namespace for table: {current_database_name}.{current_table_name}')
    return future_namespace

# COMMAND ----------

def get_table_namespace(layer:str, table: str) -> str:
    """gets correct table namespace based on the UC migration/databricks-env secret being available in keyvault, used primarily for pipelines other than raw and cleansed ETL"""
    env = ADS_DATABRICKS_ENV
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
    
    

# COMMAND ----------

# print('verifyTableSchema(table as string, new structure definition as StructType)')
# print('\tThis function will verify nullable flags in schema against table definition and generate/run ALTER statements as required')

# COMMAND ----------

def verifyTableSchema(table, newSchema):
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, BooleanType, FloatType, DecimalType, DateType, LongType
    
    #This function is not relevant for tables in the raw layer or the staged step of the cleansed layer
    if table.split('.')[0].split('_')[-1] == 'raw' or (table.split('.')[0].split('_')[-1] == 'cleansed' and table.split('.')[1][:4] == 'stg_'):
        return
    
    dfStruct = []
    desiredStruct = []
    alterStmts = []
    
    #build list of struture type elements
    def buildStruct(schema):
        struct = []
        # for ix, fld in enumerate(str(schema).split('StructField')): #commented as part of Fix1.1
        for ix, fld in enumerate(str(schema.fields).split('StructField')):
            if ix == 0:
                continue
            # flds = fld.strip('(').strip('),').split(',') #commented as part of Fix1.1
            flds = fld.replace("'", "").replace("()", "").replace(" ", "").lower().strip("(").strip("),").split(",")
            if flds[1][:11] == 'DecimalType':
                flds[1] += ',' + flds[2]
                flds[2] = flds[3]
            struct.append([flds[0], flds[1], flds[2]])
        return(struct)

    df = spark.sql("select * from {0} limit 0".format(table))
    currentStruct = buildStruct(df.schema)
    desiredStruct = buildStruct(newSchema)
    
    #assert len(currentStruct) == len(desiredStruct), f'table contains {len(currentStruct)} columns and schema contains {len(desiredStruct)} columns'
    
    for ix, elem in enumerate(currentStruct):
        #make sure we are looking at the same element in both places
        #assert elem[0] == desiredStruct[ix][0], f'Column {elem[0]} on table does not match {desiredStruct[ix][0]} in schema definition'
        #should we also change data types here? It seems to work from standard code anywayand would need a conversion from Python to SQL type for it to work here
        #if elem[1] != desiredStruct[ix][1]:
        #    alterStmts.append(f'ALTER TABLE {table} ALTER COLUMN {elem[0]} TYPE {desiredStruct[ix][1]};')
        #if the nullable flag is not the same on both sides, generate an ALTER statement to update the table
        try:
            # if element matches
            assert elem[0] == desiredStruct[ix][0], f'Column {elem[0]} on table does not match {desiredStruct[ix][0]} in schema definition'
            
            if elem[2] != desiredStruct[ix][2]:
                alterStmts.append(f'ALTER TABLE {table} ALTER COLUMN {elem[0]} {"SET NOT NULL" if desiredStruct[ix][2] == "false" else "DROP NOT NULL"};')
        except IndexError:
            if elem[0][:1] == '_': #system field
                # must be False
                if elem[2] != 'false':
                    alterStmts.append(f'ALTER TABLE {table} ALTER COLUMN {elem[0]} SET NOT NULL;')
            else:
                raise
                
    #execute the ALTER statements
    for alterStmt in alterStmts:
        print(alterStmt)
        spark.sql(alterStmt)
