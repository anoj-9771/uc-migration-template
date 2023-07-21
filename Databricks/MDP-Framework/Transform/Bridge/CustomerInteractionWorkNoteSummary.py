# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

#####Determine Load #################
###############################
driverTable1 = 'curated.fact.customerinteraction'   

if not(TableExists(_.Destination)):
    isDeltaLoad = False
    #####Table Full Load #####################
    derivedDF1 = GetTable(f"{getEnv()}{driverTable1}").withColumn("_change_type", lit(None))
else:
    #####CDF for eligible tables#####################
    isDeltaLoad = True
    derivedDF1 = getSourceCDF(driverTable1, None, False)
    if derivedDF1.count() == 0:
        print("No delta to be  processed")
        #dbutils.notebook.exit(f"no CDF to process for table for source {driverTable1} and {driverTable2} -- Destination {_.Destination}") 

# COMMAND ----------

def Transform():
    global df
    
    # ------------- TABLES ----------------- #
    worknote_df = GetTable(f"{getEnv()}curated.fact.customerserviceworknote").alias('WN')
    factinteraction_df = derivedDF1.alias('IR') 
                                
    # ------------- JOINS ------------------ #
    
    interaction_wn_df = (
        factinteraction_df
          .join(worknote_df,expr("IR.customerInteractionID = WN.customerServiceObjectID and IR.customerInteractionCreatedTimestamp >= CAST(WN.customerServiceWorkNoteCreatedTimestamp AS DATE) and IR._recordCurrent = 1 AND WN._recordCurrent = 1"))
          .where("WN.customerServiceWorkNoteTypeName = 'Summary' and WN.customerServiceObjectTypeCode = 'BUS2000126'")            
          .selectExpr("IR.customerInteractionSK as customerInteractionFK","WN.customerServiceWorknoteSK as customerServiceRequestWorknoteFK","IR.customerInteractionId as customerInteractionId", "WN.customerServiceWorkNoteId as customerServiceWorkNoteId", "'Interaction -Summary Work Note' as relationshipType", "_change_type")    
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
        ,"_change_type"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    #CleanSelf()
    SaveWithCDF(df, 'APPEND')
#     DisplaySelf()
pass
Transform()
