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
   
    factservicerequest_df = GetTable(f"{TARGET}.factservicerequest").alias('SR')    
    crm_crmd_brelvonae_df = GetTable(f"{SOURCE}.crm_crmd_brelvonae").alias('B')
    dimemailheader_df = GetTable(f"{TARGET}.dimemailheader").alias('H')   
 
    # ------------- JOINS ------------------ #     
    email_df = (
        crm_crmd_brelvonae_df.where("B.objectTypeA = 'BUS2000223' and B.objectTypeB = 'SOFM'")
          .join(dimemailheader_df,expr("trim(B.objectKeyB) = H.emailID and H._recordCurrent = 1"))
          .join(factservicerequest_df,expr("SR.serviceRequestGUID = B.objectKeyA and SR._recordCurrent = 1"))          
          .selectExpr("SR.serviceRequestSK as serviceRequestFK","H.emailHeaderSK as emailHeaderFK", "SR.serviceRequestId as serviceRequestId", "H.emailID emailId", "'Service Request - Email' as relationshipType")    
    )   
   interactionId
emailId 
    
    df = email_df
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"serviceRequestFK||'|'||emailHeaderFK {BK}"
        ,"serviceRequestFK"
        ,"emailHeaderFK"
        ,"serviceRequestId"
        ,"emailId"
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
