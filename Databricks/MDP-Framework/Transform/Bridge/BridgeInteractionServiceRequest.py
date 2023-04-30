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

    factservicerequest_df = GetTable(f"{TARGET}.factservicerequest").alias('SR')
    crm_crmd_brelvonae_df = GetTable(f"{SOURCE}.crm_crmd_brelvonae").alias('B')    
    factinteraction_df = GetTable(f"{TARGET}.factinteraction").alias('IR')    
                                
    # ------------- JOINS ------------------ #    
    intern_servReq_df = (
        crm_crmd_brelvonae_df.where("B.objectTypeA = 'BUS2000126' and B.objectTypeB = 'BUS2000223'") 
          .join(factinteraction_df,expr("IR.interactionGUID = B.objectKeyA"), "Inner") 
          .join(factservicerequest_df,expr("(SR.serviceRequestGUID = B.objectKeyB) and (IR._recordStart between SR._recordStart and SR._recordEnd)"),"Inner") 
          .filter(expr("B.objectKeyB <> B.objectKeyA"))  
          .selectExpr("IR.interactionSK as interactionFK","SR.serviceRequestSK as serviceRequesFK","IR.interactionId as interactionId", "SR.serviceRequestId as serviceRequestId", "'Interaction - Service Request' as relationshipType")
    )  

    df = intern_servReq_df        
       
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"interactionFK||'|'||serviceRequesFK {BK}"
        ,"interactionFK"
        ,"serviceRequesFK"
        ,"interactionId"
        ,"serviceRequestId"
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
