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
    worknote_df = GetTable(f"{getEnv()}curated.fact.customerserviceworknote").alias('WN')
    factservicerequest_df = derivedDF1.alias('SR')    
    
                                
    # ------------- JOINS ------------------ #
    wn_resolution_df = (
        factservicerequest_df
          .join(worknote_df,expr("SR.customerServiceRequestID = WN.customerServiceObjectID and SR.customerServiceRequestSnapshotTimestamp >= WN.customerServiceWorkNoteCreatedTimestamp and SR._recordCurrent = 1 AND WN._recordCurrent = 1"))
          .where("WN.customerServiceWorkNoteTypeName = 'Resolution' and WN.customerServiceObjectTypeCode = 'BUS2000223'")        
          .selectExpr("SR.customerServiceRequestSK as customerServiceRequestFK", "WN.customerServiceWorknoteSK as customerServiceWorknoteFK", "SR.customerServiceRequestID customerServiceRequestId", "WN.customerServiceWorkNoteId customerServiceRequestWorkNoteId", 
                      "WN.customerServiceObjectTypeName customerServiceRequestObjectTypeName", "WN.customerServiceWorkNoteTypeName customerServiceRequestWorkNoteTypeName", "'Service Request -Resolution Work Note' as relationshipType"
                      ,"_change_type")    
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
