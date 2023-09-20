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
    #derivedDF1.createOrReplaceTempView("derivedDF1Table") 
    if derivedDF1.count() == 0:
        print("No delta to be  processed")
        #dbutils.notebook.exit(f"no CDF to process for table for source {driverTable1} and {driverTable2} -- Destination {_.Destination}") 

# COMMAND ----------

def Transform():
    global df
    
    # ------------- TABLES ----------------- #    
    factservicerequesta_df = derivedDF1.alias('SRA')
    factservicerequest_df = GetTable(f"{getEnv()}curated.fact.customerservicerequest").alias('SR')
    crm_crmd_brelvonae_df = GetTable(f"{getEnv()}cleansed.crm.crmd_brelvonae").alias('B')
 
    # ------------- JOINS ------------------ #
    servReq_servReq_df = (
        crm_crmd_brelvonae_df.where("B.objectTypeA = 'BUS2000223' and B.objectTypeB = 'BUS2000223'") 
          .join(factservicerequesta_df,expr("SRA.customerServiceRequestGUID = B.objectKeyA"),"Inner")  
          .join(factservicerequest_df,expr("(SR.customerServiceRequestGUID = B.objectKeyB) and (SRA._recordStart between SR._recordStart and SR._recordEnd)"),"Inner") 
          .filter(expr("B.objectKeyB <> B.objectKeyA"))  
          .selectExpr("SRA.customerServiceRequestSK as customerServiceRequestPrimaryFK","SR.customerServiceRequestSK as customerServiceRequestSecondaryFK", "SRA.customerServiceRequestID as customerServiceRequestPrimaryId", 
                      "SR.customerServiceRequestID as customerServiceRequestSecondaryId", "'Service Request - Service Request' as relationshipType", "_change_type")
    )
    
    df = servReq_servReq_df.distinct()
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"customerServiceRequestPrimaryFK||'|'||customerServiceRequestSecondaryFK {BK}"
        ,"customerServiceRequestPrimaryFK"
        ,"customerServiceRequestSecondaryFK"
        ,"customerServiceRequestPrimaryId customerServiceRequestPrimaryId"
        ,"customerServiceRequestSecondaryId customerServiceRequestSecondaryId"
        ,"relationshipType customerServiceRequestRelationshipTypeName"
        ,"_change_type"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    #CleanSelf()
    SaveWithCDF(df, 'APPEND') #Save(df)
#     DisplaySelf()
pass
Transform()
