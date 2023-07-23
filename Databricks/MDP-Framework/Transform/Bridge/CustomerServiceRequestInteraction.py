# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

#####Determine Load #################
###############################
driverTable1 = 'curated.fact.customerservicerequest'   

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

    factservicerequesta_df = derivedDF1.alias('SRA')    
    factinteraction_df = GetTable(f"{getEnv()}curated.fact.customerinteraction").alias('IR') 
    crm_crmd_brelvonae_df = GetTable(f"{getEnv()}cleansed.crm.crmd_brelvonae").where("objectTypeA = 'BUS2000223' and objectTypeB = 'BUS2000126'").alias('B')  
   
    # ------------- JOINS ------------------ #    
    servReq_intern_df = (
        factservicerequesta_df
          .join(crm_crmd_brelvonae_df,expr("SRA.customerServiceRequestGUID = B.objectKeyA"),"Inner")  
          .join(factinteraction_df,expr("(IR.customerInteractionGUID = B.objectKeyB) and (SRA._recordStart between IR._recordStart and IR._recordEnd)"),"Inner")
          .filter(expr("B.objectKeyB <> B.objectKeyA"))
          .selectExpr("SRA.customerServiceRequestSK as customerServiceRequestFK","IR.customerInteractionSK as customerInteractionFK", "IR.customerInteractionID customerInteractionId", "SRA.customerServiceRequestID customerServiceRequestId", "'Service Request - Interaction' as relationshipType", "_change_type")
    )  
    
    df = servReq_intern_df   
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"customerServiceRequestFK||'|'||customerInteractionFK {BK}"
        ,"customerServiceRequestFK"
        ,"customerInteractionFK"
        ,"customerInteractionId     customerInteractionId"
        ,"customerServiceRequestId  customerServiceRequestId"
        ,"relationshipType  customerServiceRelationshipType"
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
