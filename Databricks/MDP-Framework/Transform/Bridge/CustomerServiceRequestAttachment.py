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
    if derivedDF1.count == 0:
        print("No delta to be  processed")
        dbutils.notebook.exit(f"no CDF to process for table for source {driverTable1} and {driverTable2} -- Destination {_.Destination}") 

# COMMAND ----------

def Transform():
    global df 

    # ------------- TABLES ----------------- #    
    factservicerequest_df = derivedDF1.alias('SR')    
    crm_crmorderphio_df = GetTable(f"{getEnv()}cleansed.crm.crmorderphio").alias('O')
    dimattachmentinfo_df = GetTable(f"{getEnv()}curated.dim.customerserviceattachmentinfo").alias('A')
    crm_skwg_brel_df = GetTable(f"{getEnv()}cleansed.crm.skwg_brel").alias('B')
    
                                
    # ------------- JOINS ------------------ #
    servReq_attachment_df = (
        factservicerequest_df
          .join(crm_skwg_brel_df,expr("SR.customerServiceRequestGUID = B.instanceIDA"),"Inner")  
          .join(crm_crmorderphio_df,expr("O.loidID = right(B.instanceIDB,32)"),"Inner")
          .join(dimattachmentinfo_df,expr("A.customerServiceAttachmentDocumentId = O.documentID"),"Inner")
          .filter(expr("SR.customerServiceRequestSnapshotTimestamp >=O.creationDatetime"))
          .selectExpr("SR.customerServiceRequestSK as customerServiceRequestFK","A.customerServiceattachmentInfoSK as customerServiceattachmentInfoFK", "SR.customerserviceRequestId as customerServiceRequestId", "A.customerServiceAttachmentDocumentId as customerServiceAttachmentDocumentId", "'Service Request - Attachment' as relationshipType", "_change_type")
    )
     
    df = servReq_attachment_df   
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"customerServiceRequestFK||'|'||customerServiceattachmentInfoFK {BK}"
        ,"customerServiceRequestFK"
        ,"customerServiceattachmentInfoFK"
        ,"customerServiceRequestId  customerServiceRequestId"
        ,"customerServiceAttachmentDocumentId        customerServiceAttachmentDocumentId"
        ,"relationshipType                           customerServiceRelationshipTypeName"
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
