# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{SOURCE}.crm_crmd_erms_header").withColumn("email_id",expr("right(emailID, 17)"))
    windowSpec1  = Window.partitionBy("email_id") 
    df = df.withColumn("row_number",row_number().over(windowSpec1.orderBy(col("changedDate").desc()))).filter("row_number == 1").drop("row_number","email_id")
    
    crmd_erms_contnt_df = GetTable(f"{SOURCE}.crm_crmd_erms_contnt").select(col("EmailID").alias('emailID'), "subject")
   
    businessPartner_agent_df = GetTable(f"{SOURCE}.crm_0bpartner_attr").select("businessPartnerNumber","firstName", "lastName")
    businessPartner_orgunit_df = GetTable(f"{SOURCE}.crm_0bpartner_attr").select("businessPartnerNumber","organizationName")
    
    
    # ------------- JOINS ------------------ #
    df = df.join(crmd_erms_contnt_df, "emailID", "left") \
    .join(businessPartner_agent_df, df.responsibleAgent == businessPartner_agent_df.businessPartnerNumber,"left").drop("businessPartnerNumber") \
    .join(businessPartner_orgunit_df, concat(lit("OU"), col("organisationUnit").substr(-8,8)) == businessPartner_orgunit_df.businessPartnerNumber,"left") \

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"right(emailId, 17) {BK}"
        ,"right(emailId, 17) emailId"
        ,"case when connectionDirection = 1 then 'externalEmailAddress' when connectionDirection = 2 then 'internalEmailAddress' end emailSender"
        ,"case when connectionDirection = 1 then 'internalEmailAddress' when connectionDirection = 2 then 'externalEmailAddress' end emailRecepient"
        ,"emailStatusCode emailStatusCode"
        ,"emailStatus emailStatusDescription"
        ,"case when connectionDirection = 1 then 'Inbound' when connectionDirection = 2 then 'Outbound' end emailDirection"
        ,"organizationName serviceTeam"
        ,"handlingTime handlingTime"
        ,"responseTime emailResponseTime"
        ,"workItemID workItemId"
        ,"concat(firstName, ' ', lastName) responsibleAgent"
        ,"escalationType escalationType"
        ,"escalationDateTime escalationDateTime"
        ,"escalationTimeInHours escalationTimeInHours"
        ,"subject emailSubject"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
    # display(df)
    # CleanSelf()
    Save(df)
    #DisplaySelf()
pass
Transform()
