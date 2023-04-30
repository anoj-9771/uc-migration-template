# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df
    
    # ------------- TABLES ----------------- #
    factinteraction_df = GetTable(f"{TARGET}.factInteraction").alias('IR')
    crm_skwg_brel_df = GetTable(f"{SOURCE}.crm_skwg_brel").alias('B')
    crm_crmorderphio_df = GetTable(f"{SOURCE}.crm_crmorderphio").alias('O')
    dimattachmentinfo_df = GetTable(f"{TARGET}.dimattachmentinfo").alias('A')
    
                                
    # ------------- JOINS ------------------ #
    intern_attachment_df = (
        crm_skwg_brel_df
          .join(factinteraction_df,expr("B.instanceIDA = IR.interactionGUID"),"Inner")  
          .join(crm_crmorderphio_df,expr("O.loidID = right(B.instanceIDB,32)"),"Inner")
          .join(dimattachmentinfo_df,expr("A.documentID = O.documentID"),"Inner")
          .filter(expr("IR.createdDate >=O.creationDatetime"))
          .selectExpr("IR.InteractionSK as InteractionFK","A.attachmentInfoFK as attachmentInfoFK","IR.interactionId as interactionId", "A.documentID as documentId", "'Interaction - Attachment' as relationshipType")
    )
    
    
    df = intern_attachment_df        
       
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"InteractionFK||'|'||attachmentInfoFK {BK}"
        ,"InteractionFK"
        ,"attachmentInfoFK"
        ,"interactionId"
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
