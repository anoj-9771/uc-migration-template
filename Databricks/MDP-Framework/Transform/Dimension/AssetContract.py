# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ---------- 

# MAGIC %run ../../Common/common-helpers 
# COMMAND ---------- 


# COMMAND ----------

# CleanSelf()

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    business_date = "changedDate"
    target_date = "assetContractChangedTimestamp"
    df = get_recent_cleansed_records(f"{SOURCE}","maximo","contract",business_date, target_date)
    df = df \
    .withColumn("sourceBusinessKey",concat_ws('|', df.contract, df.revision)) \
    .withColumn("sourceValidToTimestamp",lit(expr(f"CAST('{DEFAULT_END_DATE}' AS TIMESTAMP)"))) \
    .withColumn("sourceRecordCurrent",expr("CAST(1 AS INT)")) 
    df = load_sourceValidFromTimeStamp(df,business_date)
    
    # ------------- JOINS ------------------ #

    # ------------- TRANSFORMS ------------- #
   
    _.Transforms = [
        f"sourceBusinessKey {BK}"
        ,"contract assetContractNumber"
        ,"revision assetContractRevisionNumber"
        ,"contractManager assetContractManagerCode"
        ,"changedBy assetContractChangedByUserName"
        ,"description assetContractDescription"
        ,"status assetContractStatusCode"
        ,"statusDate assetContractStatusDate"
        ,"startDate assetContractStartDate"
        ,"endDate assetContractEndDate"
        ,"latestContractAmount assetContractLatestContractAmount"
        ,"contractOriginalAmount assetContractOriginalAmount"
        ,"vendor assetContractVendorCode"
        ,"revisionComments assetContractRevisionCommentDescription"
        ,"renewalDate assetContractRenewalDate"
        ,"totalExpenditureToDate assetContractTotalExpenditureToDate"
        ,"primaryVendorPerson assetContractPrimaryVendorName"
        ,"totalBaseCost assetContractTotalBaseAmount"
        ,"totalCost assetContractTotalAmount"
        ,"vendorTermAllowed assetContractVendorTermAllowedCode"
        ,"inspectionRequired assetContractInspectionRequiredIndicator"
        ,"rowStamp assetContractRowSequenceNumber"
        ,"changedDate assetContractChangedTimestamp"
        ,"sourceValidFromTimestamp"
        ,"sourceValidToTimestamp"
        ,"sourceRecordCurrent"
        ,"sourceBusinessKey"
        
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #
    df.schema['assetContractNumber'].nullable = False 
    df.schema['assetContractRevisionNumber'].nullable = False 

    # Updating Business SCD columns for existing records
    try:
        # Select all the records from the existing curated table matching the new records to update the business SCD columns - sourceValidToTimestamp,sourceRecordCurrent.
        existing_data = spark.sql(f"""select * from {get_table_namespace(f'{DEFAULT_TARGET}', f'{TableName}')}""") 
        matched_df = existing_data.join(df.select("assetContractNumber", "assetContractRevisionNumber",col("sourceValidFromTimestamp").alias("new_changed_date")),["assetContractNumber", "assetContractRevisionNumber"],"inner")\
        .filter("_recordCurrent == 1").filter("sourceRecordCurrent == 1")

        matched_df =matched_df.withColumn("sourceValidToTimestamp",expr("new_changed_date - INTERVAL 1 SECOND")) \
        .withColumn("sourceRecordCurrent",expr("CAST(0 AS INT)"))

        df = df.unionByName(matched_df.selectExpr(df.columns))
    except Exception as exp:
        print(exp)

    # ------------- SAVE ------------------- #
#     display(df)
    Save(df)
    #DisplaySelf()
pass
Transform()

# COMMAND ----------

# MAGIC %sql
# MAGIC Select assetContractSK, count(1) from {get_table_namespace('curated', 'dimAssetContract')} group by assetContractSK having count(1) >1

# COMMAND ----------

# MAGIC %sql
# MAGIC drop view if exists {get_table_namespace('curated', 'dimAssetContract')}

# COMMAND ----------

spark.sql(f"""
          create or replace view {get_table_namespace('curated', 'dimAssetContract')} As (select * from {get_table_namespace('curated', 'dimAssetContract')})
          """)

# COMMAND ----------


