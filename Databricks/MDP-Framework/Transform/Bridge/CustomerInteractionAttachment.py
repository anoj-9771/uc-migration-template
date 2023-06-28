# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

_.Destination
# _.EntityName
#_.Database

# COMMAND ----------

get_table_namespace("curated", "brgcustomerinteractionattachment")

# COMMAND ----------

# MAGIC %run ../../Common/common-helpers 

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df
    
    # ------------- TABLES ----------------- #
    factinteraction_df =GetTable(f"{get_table_namespace(f'{TARGET}', 'factcustomerinteraction')}").alias('IR')
    crm_skwg_brel_df = GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_skwg_brel')}").alias('B')
    crm_crmorderphio_df = GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_crmorderphio')}").alias('O')
    dimattachmentinfo_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimcustomerserviceattachmentinfo')}").alias('A')
    
                                
    # ------------- JOINS ------------------ #
    intern_attachment_df = (
        crm_skwg_brel_df
            .join(factinteraction_df,expr("B.instanceIDA = IR.customerInteractionGUID"),"Inner")  
            .join(crm_crmorderphio_df,expr("O.loidID = right(B.instanceIDB,32)"),"Inner")
            .join(dimattachmentinfo_df,expr("A.customerServiceAttachmentDocumentId = O.documentID"),"Inner")
            .filter(expr("IR.customerInteractionCreatedTimestamp >=O.creationDatetime"))
            .selectExpr("IR.customerInteractionSK as customerInteractionFK","A.customerServiceAttachmentInfoSK as customerServiceAttachmentInfoFK","IR.customerInteractionId as customerInteractionId", "A.customerServiceAttachmentDocumentId", "'Interaction - Attachment' as relationshipType")
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
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    #CleanSelf()
    Save(df)
#   DisplaySelf()
pass
Transform()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ppd_curated.brg.CustomerServiceRequestWorkNoteSummary

# COMMAND ----------


