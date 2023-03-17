# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{SOURCE}.maximo_contract").withColumn("assetContractTypeFK",lit(''))
   
    # ------------- JOINS ------------------ #

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"contract {BK}"
        ,"contractManager contractManagerFK"
        ,"changedBy changedByFK"
        ,"assetContractTypeFK assetContractTypeFK"
        ,"contract contractNumber"
        ,"description contractDescription"
        ,"status contractStatus"
        ,"statusDate contractStatusDate"
        ,"startDate contractStartDate"
        ,"endDate contractEndDate"
        ,"latestContractAmount latestContractAmount"
        ,"contractOriginalAmount contractOriginalAmount"
        ,"vendor vendor"
        ,"revision revision"
        ,"revisionComments revisionComments"
        ,"renewalDate renewalDate"
        ,"totalExpenditureToDate totalExpenditureToDate"
        ,"primaryVendorPerson primaryVendorPerson"
        ,"totalBaseCost totalBaseCost"
        ,"totalCost totalCost"
        ,"vendorTermAllowed vendorTermAllowed"
        ,"inspectionRequired inspectionRequired"
#         ,"changeDate changeDate"
        ,"rowStamp rowStamp"
        
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    # CleanSelf()
    Save(df)
    #DisplaySelf()
pass
Transform()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimAssetContract

# COMMAND ----------


