# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

def Transform():
    global df   
    global crm_crmd_brelvonae_df
    global dimemailheader_df   
    global factinteraction_df

    # ------------- TABLES ----------------- #
    crm_crmd_brelvonae_df = GetTable(f"{SOURCE}.crm_crmd_brelvonae").alias('B')
    dimemailheader_df = GetTable(f"{TARGET}.dimemailheader").alias('H')    
    factinteraction_df = GetTable(f"{TARGET}.factinteraction").alias('IR')    

    # ------------- JOINS ------------------ #
    interaction_email_df = (
        crm_crmd_brelvonae_df.where("B.objectTypeA = 'BUS2000126' and B.objectTypeB = 'SOFM'")
          #From Spec - no filter on recordStart nor recordEnd
#           .join(dimemailheader_df,expr("trim(B.objectKeyB) = H.emailID"))
#           .join(factservicerequest_df,expr("SR.serviceRequestGUID = B.objectKeyA"))
          #Will propose - since there doesn't seem to be any date field in B table, use _recordCurrent instead
          .join(dimemailheader_df,expr("trim(B.objectKeyB) = H.emailID and H._recordCurrent = 1"))
          .join(factinteraction_df,expr("IR.interactionGUID = B.objectKeyA and IR._recordCurrent = 1"))
          .selectExpr("IR.interactionSK as interactionFK","H.emailHeaderSK as emailHeaderFK", "IR.interactionId as interactionId", "H.emailID emailId", "'Interaction - Email' as relationshipType")    
    )    
    
    df = interaction_email_df
           
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"interactionFK||'|'||emailHeaderFK {BK}"
        ,"interactionFK"
        ,"emailHeaderFK"
        ,"interactionId"
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
