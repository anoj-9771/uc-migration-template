# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df
    
    # ------------- TABLES ----------------- #    
    factservicerequest_df = GetTable(f"{TARGET}.factservicerequest").alias('SR')
    factservicerequesta_df = GetTable(f"{TARGET}.factservicerequest").alias('SRA')
    crm_crmd_brelvonae_df = GetTable(f"{SOURCE}.crm_crmd_brelvonae").alias('B')
 
    # ------------- JOINS ------------------ #
    servReq_servReq_df = (
        crm_crmd_brelvonae_df.where("B.objectTypeA = 'BUS2000223' and B.objectTypeB = 'BUS2000223'") 
          .join(factservicerequesta_df,expr("SRA.serviceRequestGUID = B.objectKeyA"),"Inner")  
          .join(factservicerequest_df,expr("(SR.serviceRequestGUID = B.objectKeyB) and (SRA._recordStart between SR._recordStart and SR._recordEnd)"),"Inner") 
          .filter(expr("B.objectKeyB <> B.objectKeyA"))  
          .selectExpr("SRA.serviceRequestSK as primaryServiceRequestFK","SR.serviceRequestSK as secondaryServiceRequestFK", "SRA.serviceRequestID as primaryServiceRequestId", 
                      "SR.serviceRequestID as secondaryServiceRequestId", "'Service Request - Service Request' as relationshipType")
    )
    
    df = servReq_servReq_df
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"primaryserviceRequestFK||'|'||secondaryserviceRequestFK {BK}"
        ,"primaryFK"
        ,"secondaryFK"
        ,"primaryServiceRequestId"
        ,"secondaryServiceRequestId"
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
