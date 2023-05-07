# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df    
    global factservicerequest_df
    global crm_crmd_brelvonae_df    
    global factinteraction_df
    # ------------- TABLES ----------------- #

    factservicerequest_df = GetTable(f"{TARGET}.factcustomerservicerequest").alias('SR')
    crm_crmd_brelvonae_df = GetTable(f"{SOURCE}.crm_crmd_brelvonae").alias('B')    
    factinteraction_df = GetTable(f"{TARGET}.factcustomerinteraction").alias('IR')    
                                
    # ------------- JOINS ------------------ #    
    intern_servReq_df = (
        crm_crmd_brelvonae_df.where("B.objectTypeA = 'BUS2000126' and B.objectTypeB = 'BUS2000223'") 
          .join(factinteraction_df,expr("IR.customerInteractionGUID = B.objectKeyA"), "Inner") 
          .join(factservicerequest_df,expr("(SR.customerserviceRequestGUID = B.objectKeyB) and (IR._recordStart between SR._recordStart and SR._recordEnd)"),"Inner") 
          .filter(expr("B.objectKeyB <> B.objectKeyA"))  
          .selectExpr("IR.customerInteractionSK as customerInteractionFK","SR.customerServiceRequestSK as customerServiceRequesFK","IR.customerInteractionGUID as customerInteractionGUID", "SR.customerServiceRequestId as customerServiceRequestId", "'Interaction - Service Request' as relationshipType")
    )  

    df = intern_servReq_df        
       
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"customerInteractionFK||'|'||customerServiceRequesFK {BK}"
        ,"customerInteractionFK"
        ,"customerServiceRequesFK"
        ,"customerInteractionGUID customerInteractionId"
        ,"customerServiceRequestId customerServiceRequestId"
        ,"relationshipType customerInteractionRelationshipTypeName"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
#    CleanSelf()
    Save(df)
#     DisplaySelf()
pass
Transform()
