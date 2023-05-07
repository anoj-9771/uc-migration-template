# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df 

    # ------------- TABLES ----------------- #    
    factservicerequest_df = GetTable(f"{TARGET}.factcustomerservicerequest").alias('SR')    
    crm_crmorderphio_df = GetTable(f"{SOURCE}.crm_crmorderphio").alias('O')
    dimattachmentinfo_df = GetTable(f"{TARGET}.dimcustomerserviceattachmentinfo").alias('A')
    crm_skwg_brel_df = GetTable(f"{SOURCE}.crm_skwg_brel").alias('B')
    
                                
    # ------------- JOINS ------------------ #
    servReq_attachment_df = (
        crm_skwg_brel_df
          .join(factservicerequest_df,expr("B.instanceIDA = SR.customerServiceRequestGUID"),"Inner")  
          .join(crm_crmorderphio_df,expr("O.loidID = right(B.instanceIDB,32)"),"Inner")
          .join(dimattachmentinfo_df,expr("A.customerServiceAttachmentDocumentId = O.documentID"),"Inner")
          .filter(expr("SR.customerServiceRequestSnapshotTimestamp >=O.creationDatetime"))
          .selectExpr("SR.customerServiceRequestSK as customerServiceRequestFK","A.customerServiceattachmentInfoSK as customerServiceattachmentInfoFK", "SR.customerserviceRequestId as customerServiceRequestId", "A.customerServiceAttachmentDocumentId as customerServiceAttachmentDocumentId", "'Service Request - Attachment' as relationshipType")
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
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
#     CleanSelf()
    Save(df)
#     DisplaySelf()
pass
Transform()
