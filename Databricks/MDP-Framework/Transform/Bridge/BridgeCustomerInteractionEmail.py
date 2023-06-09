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
    global crm_crmd_brelvonae_df
    global dimemailheader_df   
    global factinteraction_df

    # ------------- TABLES ----------------- #
    crm_crmd_brelvonae_df = GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_crmd_brelvonae')}").alias('B')
    dimemailheader_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimcustomerserviceemailheader')}").alias('H')    
    factinteraction_df =GetTable(f"{get_table_namespace(f'{TARGET}', 'factcustomerinteraction')}").alias('IR')    

    # ------------- JOINS ------------------ #
    interaction_email_df = (
        crm_crmd_brelvonae_df.where("B.objectTypeA = 'BUS2000126' and B.objectTypeB = 'SOFM'")
          #From Spec - no filter on recordStart nor recordEnd
#           .join(dimemailheader_df,expr("trim(B.objectKeyB) = H.customerServiceEmailID"))
#           .join(factservicerequest_df,expr("SR.serviceRequestGUID = B.objectKeyA"))
          #Will propose - since there doesn't seem to be any date field in B table, use _recordCurrent instead
          .join(dimemailheader_df,expr("trim(B.objectKeyB) = H.customerServiceEmailID and H._recordCurrent = 1"))
          .join(factinteraction_df,expr("IR.customerInteractionGUID = B.objectKeyA and IR._recordCurrent = 1"))
          .selectExpr("IR.customerInteractionSK as customerInteractionFK","H.customerServiceEmailHeaderSK as customerServiceEmailHeaderFK", "IR.customerInteractionId as customerInteractionId", "H.customerServiceEmailID customerServiceEmailID", "'Interaction - Email' as relationshipType")    
    )    
    
    df = interaction_email_df
           
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"customerInteractionFK||'|'||customerServiceEmailHeaderFK {BK}"
        ,"customerInteractionFK"
        ,"customerServiceEmailHeaderFK"
        ,"customerInteractionId customerInteractionId"
        ,"customerServiceEmailId customerServiceEmailId"
        ,"relationshipType customerInteractionRelationshipTypeName"
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
