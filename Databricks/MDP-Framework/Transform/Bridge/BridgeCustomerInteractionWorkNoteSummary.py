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
    factinteraction_df =GetTable(f"{get_table_namespace(f'{TARGET}', 'factcustomerinteraction')}").alias('IR') 
                                
    # ------------- JOINS ------------------ #
    
    interaction_wn_df = (
        factinteraction_df
          .join(worknote_df,expr("IR.customerInteractionID = WN.customerServiceObjectID and IR.customerInteractionCreatedTimestamp >= CAST(WN.customerServiceWorkNoteCreatedTimestamp AS DATE) and IR._recordCurrent = 1 AND WN._recordCurrent = 1"))
          .where("WN.customerServiceWorkNoteTypeName = 'Summary' and WN.customerServiceObjectTypeCode = 'BUS2000126'")            
          .selectExpr("IR.customerInteractionSK as customerInteractionFK","WN.customerServiceWorknoteSK as customerServiceRequestWorknoteFK","IR.customerInteractionId as customerInteractionId", "WN.customerServiceWorkNoteId as customerServiceWorkNoteId", "'Interaction -Summary Work Note' as relationshipType")    
    )  
    
    df = interaction_wn_df
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"customerInteractionFK||'|'||customerServiceWorkNoteId {BK}"
        ,"customerInteractionFK"
        ,"customerServiceWorkNoteId"
        ,"customerInteractionId customerInteractionId"
        ,"customerServiceWorkNoteId customerInteractionWorkNoteId"
        ,"relationshipType customerInteractionRelationshipTypeName"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    #CleanSelf()
    Save(df)
#     DisplaySelf()
pass
Transform()
