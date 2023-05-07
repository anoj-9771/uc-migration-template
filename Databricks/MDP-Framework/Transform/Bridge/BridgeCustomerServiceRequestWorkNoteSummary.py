# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df    
 
    # ------------- TABLES ----------------- #
    worknote_df = GetTable(f"{TARGET}.factcustomerServiceWorknote").alias('WN')
    factservicerequest_df = GetTable(f"{TARGET}.factcustomerservicerequest").alias('SR')   
                                
    # ------------- JOINS ------------------ #
    wn_summary_df = (
        factservicerequest_df
          .join(worknote_df,expr("SR.customerServiceRequestID = WN.customerServiceObjectID and SR.customerServiceRequestSnapshotTimestamp >= WN.customerServiceWorkNoteCreatedTimeStamp and SR._recordCurrent = 1 AND WN._recordCurrent = 1"))
          .where("WN.customerServiceWorkNoteTypeName = 'Summary' and WN.customerServiceObjectTypeCode = 'BUS2000223'")
          .selectExpr("SR.customerServiceRequestSK as customerServiceRequestFK", "WN.customerServiceWorknoteSK as customerServiceWorknoteFK", "SR.customerServiceRequestID customerServiceRequestId", "WN.customerServiceWorkNoteId customerServiceRequestWorkNoteId", 
                      "WN.customerServiceObjectTypeName customeServiceRequestObjectTypeName ", "WN.customerServiceWorkNoteTypeName customerServiceRequestWorkNoteTypeName", "'Service Request -Summary Work Note' as relationshipType")
    )    
    
    
    df = wn_summary_df
          
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"customerServiceRequestFK||'|'||customerServiceWorknoteFK {BK}"
        ,"customerServiceRequestFK"
        ,"customerServiceWorknoteFK"
        ,"customerServiceRequestId"
        ,"customerServiceRequestWorkNoteId customerServiceRequestWorkNoteId"
        ,"customeServiceRequestObjectTypeName customeServiceRequestObjectTypeName"
        ,"customerServiceRequestWorkNoteTypeName customerServiceRequestWorkNoteTypeName"        
        ,"relationshipType customerServiceRequestRelationshipTypeName"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
#   CleanSelf()
    Save(df)
#     DisplaySelf()
pass
Transform()
