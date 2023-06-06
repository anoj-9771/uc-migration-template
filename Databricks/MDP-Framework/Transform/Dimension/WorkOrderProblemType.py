# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ---------- 

# MAGIC %run ../../Common/common-helpers 
# COMMAND ---------- 


# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    
    df = GetTable(get_table_name(f"{SOURCE}","maximo","swcProblemType")).select("problemTypeId","problemType","description","rowstamp","leakbreak") \
    .withColumn("sourceValidToTimestamp",lit(expr(f"CAST('{DEFAULT_END_DATE}' AS TIMESTAMP)"))) \
    .withColumn("sourceRecordCurrent",expr("CAST(1 AS INT)"))
    df = load_sourceValidFromTimeStamp(df)
   
    # ------------- JOINS ------------------ #
    
    
    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"problemType {BK}"
        ,"problemTypeId workOrderProblemTypeId"
        ,"problemType workOrderProblemTypeName"
        ,"description workOrderProblemTypeDescription"
        ,"rowstamp workOrderProblemTypeRowstamp"
        ,"leakbreak workOrderProblemTypeLeakBreakValue"
        ,"sourceValidFromTimestamp"
        ,"sourceValidToTimestamp"
        ,"sourceRecordCurrent"
        ,"problemType sourceBusinessKey"

       
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
    # Updating Business SCD columns for existing records
    try:
        # Select all the records from the existing curated table matching the new records to update the business SCD columns - sourceValidToTimestamp,sourceRecordCurrent.
        existing_data = spark.sql(f"""select * from {get_table_namespace(f'{DEFAULT_TARGET}', f'{TableName}')}""") 
        matched_df = existing_data.join(df.select("sourceBusinessKey",col("sourceValidFromTimestamp").alias("new_change_date")),"problemType","inner")\
        .filter("_recordCurrent == 1").filter("sourceRecordCurrent == 1")

        matched_df =matched_df.withColumn("sourceValidToTimestamp",expr("new_change_date - INTERVAL 1 SECOND")) \
        .withColumn("sourceRecordCurrent",expr("CAST(0 AS INT)"))

        df = df.unionByName(matched_df.selectExpr(df.columns))
    except Exception as exp:
        print(exp)
#     display(df)
    # CleanSelf()
    Save(df)
    #DisplaySelf()
pass
Transform()

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view {get_table_namespace('curated', 'dimWorkOrderProblemType')} AS (select * from {get_table_namespace('curated', 'dimWorkOrderProblemType')})
