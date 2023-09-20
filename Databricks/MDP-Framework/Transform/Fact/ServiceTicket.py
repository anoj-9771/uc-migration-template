# Databricks notebook source
# MAGIC %md 
# MAGIC Vno| Date      | Who         |Purpose
# MAGIC ---|:---------:|:-----------:|:--------:
# MAGIC 1  |28/02/2023 |Mag          |Transform {get_table_namespace('cleansed', 'maximo_relatedrecord')} to Curated_v2.bridgeServiceTicketWorkOrder    

# COMMAND ----------

# MAGIC %run ../../Common/common-transform 

# COMMAND ---------- 

# MAGIC %run ../../Common/common-helpers 
# COMMAND ---------- 


# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{get_table_namespace(f'{SOURCE}', 'maximo_sr')}") 
    
    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"serviceRequest {BK}"
        ,"propertyNumber propertyFK"
        ,"asset assetFK"
        ,"location facilitylocationFK"
        ,"class ticketClass"
        ,"serviceRequest serviceRequest"
        ,"affectedDate affectedDate"
        ,"eMailAffected eMailAffected"
        ,"changedBy changedBy"
        ,"changedDate changedDate"
        ,"summary summary"
        ,"failureCode failureCode"
        ,"cause cause"
        ,"resolution resolution"
        ,"reportedDate reportedDate"
        ,"reportedBy reportedBy"
        ,"eMailReported eMailReported"
        ,"phoneReported phoneReported"
        ,"reportedPriority reportedPriority"
        ,"status status"
        ,"source source"
        ,"statusDate statusDate"
        ,"callerType callerType"
        ,"callType callType"
        ,"createDate createDate"
        ,"facilityNumber facilityNumber"
        ,"internalFacilityId internalFacilityId"
        ,"pointName pointName"
        ,"siteCode siteCode"
        ,"internalSiteId internalSiteId"
        ,"problemType problemType"
        ,"serviceType serviceType"
        ,"taskCode taskCode"
        ,"ticketuid ticketuid"
        
    ]
    
    df = df.selectExpr(
        _.Transforms
    ).dropDuplicates()
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
    #display(df)
    #df.display()
    # CleanSelf()
    Save(df)
    #DisplaySelf()

Transform()
