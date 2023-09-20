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
    factinteraction_df = derivedDF1.alias('IR')
    crm_skwg_brel_df = GetTable(f"{getEnv()}cleansed.crm.skwg_brel").alias('B')  
    crm_crmorderphio_df = GetTable(f"{getEnv()}cleansed.crm.crmorderphio").alias('O')
    dimattachmentinfo_df = GetTable(f"{getEnv()}curated.dim.customerserviceattachmentinfo").alias('A')
      
                                
    # ------------- JOINS ------------------ #
    intern_attachment_df = (
        factinteraction_df
            .join(crm_skwg_brel_df,expr("IR.customerInteractionGUID = B.instanceIDA"),"Inner")  
            .join(crm_crmorderphio_df,expr("O.loidID = right(B.instanceIDB,32)"),"Inner")
            .join(dimattachmentinfo_df,expr("A.customerServiceAttachmentDocumentId = O.documentID"),"Inner")
            .filter(expr("IR.customerInteractionCreatedTimestamp >=O.creationDatetime"))
            .selectExpr("IR.customerInteractionSK as customerInteractionFK",
                        "A.customerServiceAttachmentInfoSK as customerServiceAttachmentInfoFK",
                        "IR.customerInteractionId as customerInteractionId", 
                        "A.customerServiceAttachmentDocumentId", 
                        "'Interaction - Attachment' as relationshipType"
                        ,"_change_type")
    )
    
    
    df = intern_attachment_df        
       
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"customerInteractionFK||'|'||customerServiceAttachmentInfoFK {BK}"
        ,"customerInteractionFK"
        ,"customerServiceAttachmentInfoFK customerServiceAttachmentInfoFK"
        ,"customerInteractionId customerInteractionId"
        ,"customerServiceAttachmentDocumentId customerServiceAttachmentDocumentId"
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
#   DisplaySelf()
pass
Transform()
