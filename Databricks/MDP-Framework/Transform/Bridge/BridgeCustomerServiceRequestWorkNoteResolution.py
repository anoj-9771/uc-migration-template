# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ---------- 

# MAGIC %run ../../Common/common-helpers 
# COMMAND ---------- 


# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df   
    
   # ------------- TABLES ----------------- #
    worknote_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'factcustomerserviceworknote')}").alias('WN')
    factservicerequest_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'factcustomerservicerequest')}").alias('SR')    
    
                                
    # ------------- JOINS ------------------ #
    wn_resolution_df = (
        factservicerequest_df
          .join(worknote_df,expr("SR.customerServiceRequestID = WN.customerServiceObjectID and SR.customerServiceRequestSnapshotTimestamp >= WN.customerServiceWorkNoteCreatedTimestamp and SR._recordCurrent = 1 AND WN._recordCurrent = 1"))
          .where("WN.customerServiceWorkNoteTypeName = 'Resolution' and WN.customerServiceObjectTypeCode = 'BUS2000223'")        
          .selectExpr("SR.customerServiceRequestSK as customerServiceRequestFK", "WN.customerServiceWorknoteSK as customerServiceWorknoteFK", "SR.customerServiceRequestID customerServiceRequestId", "WN.customerServiceWorkNoteId customerServiceRequestWorkNoteId", 
                      "WN.customerServiceObjectTypeName customerServiceRequestObjectTypeName", "WN.customerServiceWorkNoteTypeName customerServiceRequestWorkNoteTypeName", "'Service Request -Resolution Work Note' as relationshipType")    
    )

    
    
    df = wn_resolution_df
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"customerServiceRequestFK||'|'||customerServiceWorknoteFK {BK}"
        ,"customerServiceRequestFK"
        ,"customerServiceWorknoteFK"
        ,"customerServiceRequestId customerServiceRequestId"
        ,"customerServiceRequestWorkNoteId customerServiceRequestWorkNoteId"
        ,"customerServiceRequestObjectTypeName  customerServiceRequestObjectTypeName"
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
