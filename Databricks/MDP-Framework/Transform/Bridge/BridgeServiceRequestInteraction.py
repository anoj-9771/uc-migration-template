# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df
  
    # ------------- TABLES ----------------- #

    factservicerequesta_df = GetTable(f"{TARGET}.factservicerequest").alias('SRA')    
    factinteraction_df = GetTable(f"{TARGET}.factinteraction").alias('IR') 
    crm_crmd_brelvonae_df = GetTable(f"{SOURCE}.crm_crmd_brelvonae").alias('B')  
   
    # ------------- JOINS ------------------ #    
    servReq_intern_df = (
        crm_crmd_brelvonae_df.where("B.objectTypeA = 'BUS2000223' and B.objectTypeB = 'BUS2000126'") 
          .join(factservicerequesta_df,expr("SRA.serviceRequestGUID = B.objectKeyA"),"Inner")  
          .join(factinteraction_df,expr("(IR.interactionGUID = B.objectKeyB) and (SRA._recordStart between IR._recordStart and IR._recordEnd)"),"Inner")
          .filter(expr("B.objectKeyB <> B.objectKeyA"))
          .selectExpr("SRA.serviceRequestSK as serviceRequestFK","IR.interactionSK as interactionFK", "IR.interactionID interactionId", "SRA.serviceRequestID serviceRequestId", "'Service Request - Interaction' as relationshipType")
    )  
    
    df = servReq_intern_df   
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"serviceRequestFK||'|'||interactionFK {BK}"
        ,"serviceRequestFK"
        ,"interactionFK"
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
