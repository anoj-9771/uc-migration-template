# Databricks notebook source
# MAGIC %run ./helper_functions

# COMMAND ----------

def assert_table_counts_post_migration(env:str, layer:str) -> None:
    target_catalog = f'{env}{layer}' if layer in  ['raw', 'cleansed', 'curated', 'curated_v2', 'curated_v3', 'semantic'] else layer
    print (f'checking table count in hive_metastore.{layer} vs table count in {target_catalog}\n')
    hive_metastore_count = spark.sql(f'show tables in hive_metastore.{layer}').count()
    uc_catalog_count = spark.table(f'{target_catalog}.information_schema.tables').filter(F.col('table_schema') != 'information_schema').count()
    # if ('curated' in layer): #or ('semantic' in layer):
    #     p_df = read_run_sheet(excel_path, f'{env}curated_mapping')
    #     uc_catalog_count = len(p_df)
    # else:
    #     uc_catalog_count = spark.table(f'{target_catalog}.information_schema.tables').filter(F.col('table_schema') != 'information_schema').count()
    try:
        assert hive_metastore_count == uc_catalog_count
    except Exception as e:
        print (f'Mismatch encountered : \n hive_metastore_count = {hive_metastore_count} \n uc_catalog_count = {uc_catalog_count}')
        print (get_diff_tables(env, layer)) if layer in ['raw', 'cleansed'] else print ('')
        #inserting line break for prettier UI output
        print ('\n')

# COMMAND ----------

def assert_row_counts_tables(layer:str, random_table_name:str):
    db_name = random_table_name.split('_')[0]
    table_name = '_'.join(random_table_name.split('_')[1:])
    target_namespace = get_target_namespace(env, layer, random_table_name)['new_namespace']
    print (f'checking row count of hive_metastore.{layer}.{random_table_name} vs row count of {target_namespace}')  
    assert spark.table(f'hive_metastore.{layer}.{random_table_name}').count() == spark.table(target_namespace).count()

# COMMAND ----------

def assert_row_counts_random_tables(layer:str) -> None:
    tables = spark.sql(f'SHOW TABLES IN hive_metastore.{layer}').select('tableName').collect()
    table_list = [table.asDict()['tableName'] for table in tables]
    table_list = [table for table in table_list if not ('vw' in table or 'view' in table)]
    random_table = random.choice(table_list)
    print (f'random table picked: {random_table}')
    assert_row_counts_tables(layer, random_table)
    
