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
    if derivedDF1.count == 0:
        print("No delta to be  processed")
        dbutils.notebook.exit(f"no CDF to process for table for source {driverTable1} and {driverTable2} -- Destination {_.Destination}") 

# COMMAND ----------

def Transform():
    global df   
    global crm_crmd_brelvonae_df
    global dimemailheader_df   
    global factinteraction_df

    # ------------- TABLES ----------------- #
    crm_crmd_brelvonae_df = GetTable(f"{getEnv()}cleansed.crm.crmd_brelvonae").where("objectTypeA = 'BUS2000126' and objectTypeB = 'SOFM'").alias('B')
    dimemailheader_df = GetTable(f"{getEnv()}curated.dim.customerserviceemailheader").alias('H')    
    factinteraction_df = derivedDF1.alias('IR')    

    # ------------- JOINS ------------------ #
    interaction_email_df = (
        factinteraction_df
           .join(crm_crmd_brelvonae_df,expr("IR.customerInteractionGUID = B.objectKeyA and IR._recordCurrent = 1"))
           .join(dimemailheader_df,expr("trim(B.objectKeyB) = H.customerServiceEmailID and H._recordCurrent = 1"))
          #From Spec - no filter on recordStart nor recordEnd
#           .join(dimemailheader_df,expr("trim(B.objectKeyB) = H.customerServiceEmailID"))
#           .join(factservicerequest_df,expr("SR.serviceRequestGUID = B.objectKeyA"))
          #Will propose - since there doesn't seem to be any date field in B table, use _recordCurrent instead
          
         
          .selectExpr("IR.customerInteractionSK as customerInteractionFK","H.customerServiceEmailHeaderSK as customerServiceEmailHeaderFK", "IR.customerInteractionId as customerInteractionId", "H.customerServiceEmailID customerServiceEmailID", "'Interaction - Email' as relationshipType", "_change_type")    
    )    
    
    df = interaction_email_df
           
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"customerInteractionFK||'|'||customerServiceEmailHeaderFK {BK}"
        ,"customerInteractionFK"
        ,"customerServiceEmailHeaderFK"
        ,"customerInteractionId customerInteractionId"
        ,"customerServiceEmailId customerServiceEmailId"
        ,"relationshipType customerInteractionRelationshipTypeName"
        , "_change_type"
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
