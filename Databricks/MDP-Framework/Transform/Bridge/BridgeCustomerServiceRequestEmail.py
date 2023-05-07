# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df    
    global factservicerequest_df
    global crm_crmd_brelvonae_df
    global dimemailheader_df
        
    # ------------- TABLES ----------------- #
   
    factservicerequest_df = GetTable(f"{TARGET}.factcustomerservicerequest").alias('SR')    
    crm_crmd_brelvonae_df = GetTable(f"{SOURCE}.crm_crmd_brelvonae").alias('B')
    dimemailheader_df = GetTable(f"{TARGET}.dimemailheader").alias('H')   
 
    # ------------- JOINS ------------------ #     
    email_df = (
        crm_crmd_brelvonae_df.where("B.objectTypeA = 'BUS2000223' and B.objectTypeB = 'SOFM'")
        .join(dimemailheader_df,expr("trim(B.objectKeyB) = H.customerServiceEmailID and H._recordCurrent = 1"))
        .join(factservicerequest_df,expr("SR.customerServiceRequestGUID = B.objectKeyA and SR._recordCurrent = 1"))          
        .selectExpr("SR.customerServiceRequestSK as customerServiceRequestFK","H.emailHeaderSK as emailHeaderFK", "SR.customerServiceRequestId as customerServiceRequestId", "H.customerServiceEmailID customerServiceEmailID", "'Service Request - Email' as relationshipType")    
    )   
   
 
    
    df = email_df
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"customerServiceRequestFK||'|'||emailHeaderFK {BK}"
        ,"customerServiceRequestFK"
        ,"emailHeaderFK"
        ,"customerServiceRequestId customerServiceRequestId"
        ,"customerServiceEmailID customerServiceEmailId"
        ,"relationshipType customerServiceRequestRelationshipTypeName"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
#   CleanSelf()
    Save(df)
#     DisplaySelf()
pass
Transform()
