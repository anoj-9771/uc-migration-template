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
    global factservicerequest_df
    global crm_crmd_brelvonae_df    
    global factinteraction_df
    # ------------- TABLES ----------------- #

    factservicerequest_df = GetTable(f"{getEnv()}curated.fact.customerservicerequest").alias('SR')
    crm_crmd_brelvonae_df = GetTable(f"{getEnv()}cleansed.crm.crmd_brelvonae").where("objectTypeB = 'BUS2000126' and objectTypeA = 'BUS2000223'").alias('B')    
    factinteraction_df = derivedDF1.alias('IR')    
                                
    # ------------- JOINS ------------------ #    
    intern_servReq_df = (
        factinteraction_df
          .join(crm_crmd_brelvonae_df,expr("IR.customerInteractionGUID = B.objectKeyB"), "Inner") 
          .join(factservicerequest_df,expr("(SR.customerserviceRequestGUID = B.objectKeyA) "),"Inner") 
          .filter(expr("B.objectKeyB <> B.objectKeyA"))  
          .selectExpr("IR.customerInteractionSK as customerInteractionFK","SR.customerServiceRequestSK as customerServiceRequestFK","IR.customerInteractionId as customerInteractionId", "SR.customerServiceRequestId as customerServiceRequestId", "'Interaction - Service Request' as relationshipType", "_change_type")
    )  

    df = intern_servReq_df        
       
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"customerInteractionFK||'|'||customerServiceRequestFK {BK}"
        ,"customerInteractionFK"
        ,"customerServiceRequestFK"
        ,"customerInteractionId customerInteractionId"
        ,"customerServiceRequestId customerServiceRequestId"
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
