# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df   
    
   # ------------- TABLES ----------------- #
    worknote_df = GetTable(f"{TARGET}.factWorknote").alias('WN')
    factservicerequest_df = GetTable(f"{TARGET}.factservicerequest").alias('SR')    
    
                                
    # ------------- JOINS ------------------ #
    wn_resolution_df = (
        factservicerequest_df
          .join(worknote_df,expr("SR.serviceRequestID = WN.objectID and SR.changeDateTime >= WN.createdTimeStamp and SR._recordCurrent = 1 AND WN._recordCurrent = 1"))
          .where("WN.workNoteType = 'Resolution' and WN.objectTypeCode = 'BUS2000223'")        
          .selectExpr("SR.serviceRequestSK as serviceRequestFK", "WN.worknoteSK as worknoteFK", "SR.serviceRequestID serviceRequestId", "WN.workNoteId workNoteId", 
                      "WN.objectType objectType", "WN.workNoteType workNoteType", 'Service Request -Resolution Work Note' as relationshipType")    
    )

    
    
    df = wn_resolution_df
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"serviceRequestFK||'|'||worknoteFK {BK}"
        ,"serviceRequestFK"
        ,"worknoteFK"
        ,"serviceRequestId"
        ,"workNoteId"
        ,"objectType"
        ,"workNoteType"        
        ,"relationshipType"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
#     CleanSelf()
    Save(df)
#     DisplaySelf()
pass
Transform()
