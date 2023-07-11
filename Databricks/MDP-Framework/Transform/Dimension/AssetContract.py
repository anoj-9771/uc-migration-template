# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

# CleanSelf()

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    business_date = "changedDate"
    target_date = "assetContractChangedTimestamp"
    df = get_recent_records(f"{SOURCE}","maximo_contract",business_date, target_date)
    df = df \
    .withColumn("sourceBusinessKey",concat_ws('|', df.contract, df.revision)) \
    .withColumn("sourceValidToTimestamp",lit(expr(f"CAST('{DEFAULT_END_DATE}' AS TIMESTAMP)"))) \
    .withColumn("sourceRecordCurrent",expr("CAST(1 AS INT)")) 
    df = load_sourceValidFromTimeStamp(df,business_date)
    #-----------ENSURE NO DUPLICATES---------#
    windowSpec  = Window.partitionBy("contract","revision")
    df = df.withColumn("rank",rank().over(windowSpec.orderBy(col("rowStamp").desc()))).filter("rank == 1").drop("rank")
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

    # ------------- SAVE ------------------- #
#     display(df)
    Save(df)
    #DisplaySelf()
pass
Transform()
