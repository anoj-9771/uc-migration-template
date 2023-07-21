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
    global factservicerequest_df
    global crm_crmd_brelvonae_df
    global dimemailheader_df
        
    # ------------- TABLES ----------------- #
   
    factservicerequest_df = derivedDF1.alias('SR')    
    crm_crmd_brelvonae_df = GetTable(f"{getEnv()}cleansed.crm.crmd_brelvonae").where("objectTypeA = 'BUS2000223' and objectTypeB = 'SOFM'").alias('B')
    dimemailheader_df = GetTable(f"{getEnv()}curated.dim.customerserviceemailheader").alias('H')   
 
    # ------------- JOINS ------------------ #     
    email_df = (
        factservicerequest_df
        .join(crm_crmd_brelvonae_df,expr("SR.customerServiceRequestGUID = B.objectKeyA and SR._recordCurrent = 1")) 
        .join(dimemailheader_df,expr("trim(B.objectKeyB) = H.customerServiceEmailID and H._recordCurrent = 1"))
        .selectExpr("SR.customerServiceRequestSK as customerServiceRequestFK","H.customerServiceEmailHeaderSK as customerServiceEmailHeaderFK", "SR.customerServiceRequestId as customerServiceRequestId", "H.customerServiceEmailID customerServiceEmailID", "'Service Request - Email' as relationshipType", "_change_type")    
    )   
   
 
    
    df = email_df
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"customerServiceRequestFK||'|'||customerServiceEmailHeaderFK {BK}"
        ,"customerServiceRequestFK"
        ,"customerServiceEmailHeaderFK"
        ,"customerServiceRequestId customerServiceRequestId"
        ,"customerServiceEmailID customerServiceEmailId"
        ,"relationshipType customerServiceRequestRelationshipTypeName"
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
