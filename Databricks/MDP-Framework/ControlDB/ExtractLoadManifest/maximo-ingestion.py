# Databricks notebook source
import json

# COMMAND ----------

# MAGIC %run ../common-controldb

# COMMAND ----------

## check for maximo records in manifest that are currently enabled
(
    spark.table("controldb.dbo_extractloadmanifest")
    .filter("SourceSchema = 'maximo'")
    .filter("Enabled = True")
    .display()
)

# COMMAND ----------

## list the maximo tables here
mapping_df = spark.read.option('header', True).csv('/mnt/datalake-raw/cleansed_csv/maximo_cleansed.csv')
tables = (
    mapping_df
    .withColumnRenamed('Maximo Extractor', 'MaximoExtractor')
    .select('MaximoExtractor')
    .filter("MaximoExtractor is Not Null")   #to avoid blank data coming in and causing issues when sorting
    .distinct()
    .rdd.map(lambda x: x.MaximoExtractor)
    .collect()
)

# tables = ['A_LOCATIONSPEC','ADDRESS','ALNDOMAIN','ASSET','ASSETATTRIBUTE','ASSETMETER','ASSETSPEC', 'CLASSIFICATION','CLASSSTRUCTURE','CONTRACT','DOCLINKS','FAILURECODE','FAILUREREPORT','FINCNTRL','JOBLABOR','JOBPLAN','LABOR','LABTRANS','LOCANCESTOR','LOCATIONMETER','LOCATIONS','LOCATIONSPEC','LOCHIERARCHY','LOCOPER','LONGDESCRIPTION','MATUSETRANS','MAXINTMSGTRK','MULTIASSETLOCCI','PERSON', 'PERSONGROUP', 'PERSONGROUPTEAM', 'PHONE','PM','RELATEDRECORD','ROUTES','ROUTE_STOP','SERVRECTRANS','SWCBUSSTATUS','SWCHIERARCHY','SWCLGA','SWCPROBLEMTYPE','SWCWOEXT','TICKET','TOOLTRANS','WOANCESTOR','WORKLOG','WORKORDER','WORKORDERSPEC','WOSTATUS', 'SYNONYMDOMAIN', 'A_ASSETSPEC', 'A_ASSET', 'A_FAILUREREPORT', 'A_GROUPUSER', 'A_JOBPLAN', 'A_INVENTORY', 'A_LONGDESCRIPTION', 'A_LOCOPER', 'A_PM', 'A_ROUTE_STOP', 'A_PO', 'A_SWCCLAIM', 'A_ROUTES', 'A_SWCCONACCESS', 'A_SWCCRSECURITY', 'A_SWCVALIDATIONRULE']

tables = sorted(set(tables)) # to get rid of duplicates and make the list alphabetical (so it's easier to read)

# COMMAND ----------

def get_env_name():
    """get the name of the current environment"""
    cluster_config = spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags")
    cluster_config = json.loads(cluster_config)
    for x in cluster_config:
        if x['key'] == 'Environment':
            return (x['value'])

# COMMAND ----------

def get_business_key(table_name: str) -> list or None:
    """for each given table, return the contents of BusinessKeyColumns in the mapping csv in this format: ['col1','col2','col3']. return None when no BusinessKeyColumn is present in the mapping csv"""
    mapping_df = spark.read.option('header', True).csv('/mnt/datalake-raw/cleansed_csv/maximo_cleansed.csv')
    try:
        business_key = (
         mapping_df
         .filter(mapping_df.UniqueKey == 'Y')
         .filter(mapping_df.RawTable == f'raw.maximo_{table_name.lower()}')
         .toPandas()['CleansedColumnName'].tolist()          
            )
        if len(business_key) > 0:
            businesss_key = business_key
        else:
            business_key = None
    except Exception as e:
        print (f"error {e} occurred")
        business_key = None
    
    return business_key

# COMMAND ----------

def inject_to_controldb() -> None or str:
    """inject the list of pending_tables into the dbo.extractloadmanifest with some pre-determined maximo specific configuration."""
    base_query = """    WITH _Base AS (
      SELECT 'maximo' SystemCode, 'maximo' SourceSchema, 'daf-oracle-maximo-connectionstring' SourceKeyVaultSecret, '' SourceQuery, 'oracle-load'    SourceHandler, '' RawFileExtension, 'raw-load-delta' RawHandler, '' ExtendedProperties, 'cleansed-load-delta' CleansedHandler
    )"""
    tables_in_controldb = list(spark.table('controldb.dbo_extractloadmanifest').filter("SystemCode = 'maximo'").select('SourceTableName').distinct().toPandas()['SourceTableName'])
    tables_not_in_controldb = list(set(tables) - set(tables_in_controldb))
    
    if tables_not_in_controldb:
        for table in tables_not_in_controldb:
            base_query += (f"SELECT '{table}' SourceTableName, * FROM _Base \n UNION \n")

        print (base_query[:-7])
        df = spark.sql(f"{base_query[:-5]}") 
        # to remove the last UNION that gets added to base_query
#         return df
        for i in df.rdd.collect():
            ExecuteStatement(f"""
            DECLARE @RC int
            DECLARE @SystemCode varchar(max) = NULLIF('{i.SystemCode}','')
            DECLARE @Schema varchar(max) = NULLIF('{i.SourceSchema}','')
            DECLARE @Table varchar(max) = NULLIF('{i.SourceTableName}','')
            DECLARE @Query varchar(max) = NULLIF('{i.SourceQuery}','')
            DECLARE @WatermarkColumn varchar(max) = NULL
            DECLARE @SourceHandler varchar(max) = NULLIF('{i.SourceHandler}','')
            DECLARE @RawFileExtension varchar(max) = NULLIF('{i.RawFileExtension}','')
            DECLARE @KeyVaultSecret varchar(max) = NULLIF('{i.SourceKeyVaultSecret}','')
            DECLARE @ExtendedProperties varchar(max) = NULLIF('{i.ExtendedProperties}','')
            DECLARE @RawHandler varchar(max) = NULLIF('{i.RawHandler}','')
            DECLARE @CleansedHandler varchar(max) = NULLIF('{i.CleansedHandler}','')

            EXECUTE @RC = [dbo].[AddIngestion] 
               @SystemCode
              ,@Schema
              ,@Table
              ,@Query
              ,@WatermarkColumn
              ,@SourceHandler
              ,@RawFileExtension
              ,@KeyVaultSecret
              ,@ExtendedProperties
              ,@RawHandler
              ,@CleansedHandler
            """)  
        else:
            print ('Error! Table likely already has records in controldb!')            
            return 'error'

# COMMAND ----------

def enable_records(table_list: list) -> None:
    """update extractloadmanifest table entries for given tables; mark Enabled as 'true' and BusinessKeyColumn to output of get_business_key function. also mark all the other tables as disabled."""
    ExecuteStatement("""
    UPDATE controldb.dbo.extractloadmanifest
    SET Enabled = 'false', WatermarkColumn = 'CAST(rowstamp as INT)'
    WHERE SystemCode = 'maximo'
    """)
    
    for table in table_list:
        ExecuteStatement (f"""
                UPDATE controldb.dbo.extractloadmanifest
                SET Enabled = 'true'
                WHERE SourceTableName = '{table}'
                """)

# COMMAND ----------

def update_business_key(table_list: list) -> None:
    """update extractloadmanifest table entries for given tables; mark Enabled as 'true' and BusinessKeyColumn to output of get_business_key function. also mark all the other tables as disabled."""

    for table in table_list:
        business_key = get_business_key(f'{table}') 
        if get_business_key(table) != None:
             ExecuteStatement (f"""
                UPDATE controldb.dbo.extractloadmanifest
                SET BusinessKeyColumn = '{','.join(business_key)}'
                WHERE SourceTableName = '{table}'
                """)
        else:
            ExecuteStatement (f"""
                UPDATE controldb.dbo.extractloadmanifest
                SET BusinessKeyColumn = null
                WHERE SourceTableName = '{table}'
                """)
            
        print (f"Business key updated for table {table} to: {business_key}")

# COMMAND ----------

def update_watermark_column(table_list: list, watermark_column: str = None) -> None:
    """update watermark column for given tables in extractloadmanifest table to null (default) or with the given value."""
    for table in table_list:
        if watermark_column is None:
            ExecuteStatement (f"""
            UPDATE controldb.dbo.extractloadmanifest
            SET WatermarkColumn = null
            WHERE SourceTableName = '{table}'
            """)
        else:
             ExecuteStatement (f"""
             UPDATE controldb.dbo.extractloadmanifest
             SET WatermarkColumn = {watermark_column}
             WHERE SourceTableName = '{table}'
             """)
        print (f"WatermarkColumn updated for table {table} to: {watermark_column}")            

# COMMAND ----------

def nullify_cleansed_handler(table_list: list) -> None:
    """null the CleansedHandler values for given set of tables."""
    for table in table_list:
        ExecuteStatement (f"""
                UPDATE controldb.dbo.extractloadmanifest
                SET CleansedHandler = null
                WHERE SourceTableName = '{table}'
                """)
            
        print (f"CleansedHandler nullified for table: {table}")

# COMMAND ----------

def update_extended_properties(table_list:list, property:str) -> None:
    """update the ExtendedProperties values for gien set of tables."""
    for table in table_list:
        query = f"""
                UPDATE controldb.dbo.extractloadmanifest
                SET ExtendedProperties = '{property}'
                WHERE SourceTableName = '{table}'
                """
        ExecuteStatement (query)        

# COMMAND ----------

## identify tables that are yet to be setup
pending_tables = []
for table in list(sorted(set(tables))):
    try:
        (spark.table(f'raw.maximo_{table}').count() > 1)
#         print (f'Table setup complete for {table}')
    except:
        print (f'Table setup not complete for {table}')
        pending_tables.append(table)

print (f'The number of tables pending : {len(pending_tables)}/{len(tables)}. \n {pending_tables}')


# COMMAND ----------

inject_to_controldb()

# COMMAND ----------

# update the business_key_columns in controldb_extractloadmanifest for the pending_tables
update_business_key(pending_tables)

# COMMAND ----------

if get_env_name == 'PRD':
    nullify_cleansed_handler(pending_tables)
else:
    pass

# COMMAND ----------

# there are some tables where the watermarkColumn is not the default column ; ROWSTAMP. run code below to remove the watermarkColumn for those tables
update_watermark_column(['SWCFMISWOMATPROCESSING','SWCCCAUDIT','SWCEXTFMISINVOICE','SWCINVOICESENT','SWCXLLOADPROCESSING'])

# COMMAND ----------

tables_with_duplicate_entries = ['labor', 'locoper', 'locations', 'longdescription', 'jobplan', 'assetspec']
update_extended_properties(tables_with_duplicate_entries, '{"GroupOrderBy" : "rowStamp Desc"}')

# COMMAND ----------

# enable the pending tables in controldb_extractloadmanifest. 
enable_records(pending_tables)

# COMMAND ----------

# check what tables have been enabled in controldb_extractloadmanifest
(
    spark.table("controldb.dbo_extractloadmanifest")
    .filter("SourceSchema = 'maximo'")
    .filter("Enabled = 'true'")
    .display()
)

# COMMAND ----------

# # this section can be uncommented out and run in order to check the status of the tables (once the pipelines have been run)
# for table_name in pending_tables:
#     try:
#         print (f'displaying contents of raw.maximo_{table_name} and cleansed.maximo_{table_name}')
#         spark.table(f'raw.maximo_{table_name}').limit(5).display()
#         spark.table(f'cleansed.maximo_{table_name}').limit(5).display()
#     except Exception as e:
#         print (f'error occurred {e}')
