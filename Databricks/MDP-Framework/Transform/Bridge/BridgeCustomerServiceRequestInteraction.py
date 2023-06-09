# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ---------- 

# MAGIC %run ../../Common/common-helpers 
# COMMAND ---------- 


# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df
  
    # ------------- TABLES ----------------- #

    factservicerequesta_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'factcustomerservicerequest')}").alias('SRA')    
    factinteraction_df =GetTable(f"{get_table_namespace(f'{TARGET}', 'factcustomerinteraction')}").alias('IR') 
    crm_crmd_brelvonae_df = GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_crmd_brelvonae')}").alias('B')  
   
    # ------------- JOINS ------------------ #    
    servReq_intern_df = (
        crm_crmd_brelvonae_df.where("B.objectTypeA = 'BUS2000223' and B.objectTypeB = 'BUS2000126'") 
          .join(factservicerequesta_df,expr("SRA.customerServiceRequestGUID = B.objectKeyA"),"Inner")  
          .join(factinteraction_df,expr("(IR.customerInteractionGUID = B.objectKeyB) and (SRA._recordStart between IR._recordStart and IR._recordEnd)"),"Inner")
          .filter(expr("B.objectKeyB <> B.objectKeyA"))
          .selectExpr("SRA.customerServiceRequestSK as customerServiceRequestFK","IR.customerInteractionSK as customerInteractionFK", "IR.customerInteractionID customerInteractionId", "SRA.customerServiceRequestID customerServiceRequestId", "'Service Request - Interaction' as relationshipType")
    )  
    
    df = servReq_intern_df   
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"customerServiceRequestFK||'|'||customerInteractionFK {BK}"
        ,"customerServiceRequestFK"
        ,"customerInteractionFK"
        ,"customerInteractionId     customerInteractionId"
        ,"customerServiceRequestId  customerServiceRequestId"
        ,"relationshipType  customerServiceRelationshipType"
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
