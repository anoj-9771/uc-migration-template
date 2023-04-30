# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df
    
    # ------------- TABLES ----------------- #
    worknote_df = GetTable(f"{TARGET}.factWorkNote").alias('WN')
    factinteraction_df = GetTable(f"{TARGET}.factinteraction").alias('IR') 
                                
    # ------------- JOINS ------------------ #
    
    interaction_wn_df = (
        factinteraction_df
          .join(worknote_df,expr("IR.interactionID = WN.objectID and IR.createdDate >= CAST(WN.createdTimeStamp AS DATE) and IR._recordCurrent = 1 AND WN._recordCurrent = 1"))
          .where("WN.workNoteType = 'Summary' and WN.objectTypeCode = 'BUS2000126'")            
          .selectExpr("IR.interactionSK as interactionFK","WN.worknoteSK as worknoteFK","IR.interactionId as interactionId", "WN.workNoteId as workNoteId", "'Interaction -Summary Work Note' as relationshipType")    
    )  
    
    df = interaction_wn_df
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"interactionFK||'|'||worknoteFK {BK}"
        ,"interactionFK"
        ,"worknoteFK"
        ,"interactionId"
        ,"workNoteId"
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
