# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

# MAGIC %run ../../Common/common-helpers 

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df
    
    # ------------- TABLES ----------------- #    
    factservicerequest_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'factcustomerservicerequest')}").alias('SR')
    factservicerequesta_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'factcustomerservicerequest')}").alias('SRA')
    crm_crmd_brelvonae_df = GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_crmd_brelvonae')}").alias('B')
 
    # ------------- JOINS ------------------ #
    servReq_servReq_df = (
        crm_crmd_brelvonae_df.where("B.objectTypeA = 'BUS2000223' and B.objectTypeB = 'BUS2000223'") 
          .join(factservicerequesta_df,expr("SRA.customerServiceRequestGUID = B.objectKeyA"),"Inner")  
          .join(factservicerequest_df,expr("(SR.customerServiceRequestGUID = B.objectKeyB) and (SRA._recordStart between SR._recordStart and SR._recordEnd)"),"Inner") 
          .filter(expr("B.objectKeyB <> B.objectKeyA"))  
          .selectExpr("SRA.customerServiceRequestSK as customerServiceRequestPrimaryFK","SR.customerServiceRequestSK as customerServiceRequestSecondaryFK", "SRA.customerServiceRequestID as customerServiceRequestPrimaryId", 
                      "SR.customerServiceRequestID as customerServiceRequestSecondaryId", "'Service Request - Service Request' as relationshipType")
    )
    
    df = servReq_servReq_df.distinct()
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"customerServiceRequestPrimaryFK||'|'||customerServiceRequestSecondaryFK {BK}"
        ,"customerServiceRequestPrimaryFK"
        ,"customerServiceRequestSecondaryFK"
        ,"customerServiceRequestPrimaryId customerServiceRequestPrimaryId"
        ,"customerServiceRequestSecondaryId customerServiceRequestSecondaryId"
        ,"relationshipType customerServiceRequestRelationshipTypeName"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    #CleanSelf()
    Save(df)
#     DisplaySelf()
pass
Transform()
