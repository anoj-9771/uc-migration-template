# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

#  CleanSelf()

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    business_date = 'rowstamp'
    target_date = 'workOrderProblemTypeRowstamp'
    df = get_recent_records(f"{SOURCE}","maximo_swcProblemType",business_date,target_date)\
        .select("problemTypeId","problemType","description","rowstamp","leakbreak") \
        .withColumn("sourceValidToTimestamp",lit(expr(f"CAST('{DEFAULT_END_DATE}' AS TIMESTAMP)"))) \
        .withColumn("sourceRecordCurrent",expr("CAST(1 AS INT)"))
    
    df = load_sourceValidFromTimeStamp(df)
    
    #-----------ENSURE NO DUPLICATES---------#
    windowSpec  = Window.partitionBy("problemType")
    df = df.withColumn("rank",rank().over(windowSpec.orderBy(col("rowStamp").desc()))).filter("rank == 1").drop("rank")
   
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
   
#     display(df)
    Save(df)
    #DisplaySelf()
pass
Transform()
