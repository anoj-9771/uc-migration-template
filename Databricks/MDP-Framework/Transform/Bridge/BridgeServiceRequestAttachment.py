# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df 

    # ------------- TABLES ----------------- #    
    factservicerequest_df = GetTable(f"{TARGET}.factservicerequest").alias('SR')    
    crm_crmorderphio_df = GetTable(f"{SOURCE}.crm_crmorderphio").alias('O')
    dimattachmentinfo_df = GetTable(f"{TARGET}.dimattachmentinfo").alias('A')
    crm_skwg_brel_df = GetTable(f"{SOURCE}.crm_skwg_brel").alias('B')
    
                                
    # ------------- JOINS ------------------ #
    servReq_attachment_df = (
        crm_skwg_brel_df
          .join(factservicerequest_df,expr("B.instanceIDA = SR.serviceRequestGUID"),"Inner")  
          .join(crm_crmorderphio_df,expr("O.loidID = right(B.instanceIDB,32)"),"Inner")
          .join(dimattachmentinfo_df,expr("A.documentID = O.documentID"),"Inner")
          .filter(expr("SR.changeDateTime >=O.creationDatetime"))
          .selectExpr("SR.serviceRequestSK as serviceRequestFK","A.attachmentInfoSK as attachmentInfoFK", "SR.serviceRequestId as serviceRequestId", "A.documentID as documentId", "'Service Request - Attachment' as relationshipType")
    )
     
    df = servReq_attachment_df   
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"serviceRequestFK||'|'||attachmentInfoFK {BK}"
        ,"serviceRequestFK"
        ,"attachmentInfoFK"
        ,"serviceRequestId"
        ,"documentId"
        ,"relationshipType"
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
