# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df
    
    # ------------- TABLES ----------------- #    
    factservicerequest_df = GetTable(f"{TARGET}.factservicerequest").alias('SR')
    crm_crmd_link_df = GetTable(f"{SOURCE}.crm_crmd_link").alias('L')
    crm_scapptseg_df = GetTable(f"{SOURCE}.crm_scapptseg").alias('S')
    dimtransactiondate_df = GetTable(f"{TARGET}.dimtransactiondate").alias('T')    

    # ------------- JOINS ------------------ #
    servReq_date_df = (
        factservicerequest_df
          .join(crm_crmd_link_df,expr("SR.serviceRequestGUID = L.hiGUID"),"Inner")  
          .join(crm_scapptseg_df,expr("S.ApplicationGUID = L.setGUID"),"Inner")
          .join(dimtransactiondate_df,expr("T._businessKey = concat(S.applicationGUID, '|', S.apptType)"),"Inner")
          .filter(expr("L.setObjectType = '30'"))
          .filter(expr("SR.createdDateTime >= T.createdDateTime"))
          .selectExpr("SR.serviceRequestSK as serviceRequestFK","T.transactionDateSK as transactionDateFK","SR.serviceRequestId as serviceRequestId", "", "'Service Request - Date' as relationshipType")
    )    

    df = servReq_date_df        
        
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"serviceRequestFK||'|'||transactionDateFK {BK}"
        ,"serviceRequestFK"
        ,"transactionDateFK"
        ,"serviceRequestId"
        ,"NULL tobedetermined"
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
