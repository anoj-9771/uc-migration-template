# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ---------- 

# MAGIC %run ../../Common/common-helpers 
# COMMAND ---------- 


# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_crmd_erms_header')}").withColumn("email_id",expr("right(emailID, 17)"))
    windowSpec1  = Window.partitionBy("email_id") 
    df = df.withColumn("row_number",row_number().over(windowSpec1.orderBy(col("changedDate").desc()))).filter("row_number == 1").drop("row_number","email_id")
    
    crmd_erms_contnt_df = GetTable(f"{SOURCE}.crm_crmd_erms_contnt").select(col("EmailID").alias('emailID'), "subject")
   
    businessPartner_agent_df = GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_0bpartner_attr')}").select("businessPartnerNumber","firstName", "lastName")
    businessPartner_orgunit_df = GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_0bpartner_attr')}").select("businessPartnerNumber","organizationName")
    
    
    # ------------- JOINS ------------------ #
    df = df.join(crmd_erms_contnt_df, "emailID", "left") \
    .join(businessPartner_agent_df, df.responsibleAgent == businessPartner_agent_df.businessPartnerNumber,"left").drop("businessPartnerNumber") \
    .join(businessPartner_orgunit_df, concat(lit("OU"), col("organisationUnit").substr(-8,8)) == businessPartner_orgunit_df.businessPartnerNumber,"left") \

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"right(emailId, 17) {BK}"
        ,"right(emailId, 17)                        customerServiceEmailId"
        ,"case when connectionDirection = 1 then 'externalEmailAddress' when connectionDirection = 2 then 'internalEmailAddress' end customerServiceEmailSenderAddress"
        ,"case when connectionDirection = 1 then 'internalEmailAddress' when connectionDirection = 2 then 'externalEmailAddress' end customerServiceEmailRecipientAddress"
        ,"emailStatusCode                           customerServiceEmailStatusCode"
        ,"emailStatus                               customerServiceEmailStatusDescription"
        ,"case when connectionDirection = 1 then 'Inbound' when connectionDirection = 2 then 'Outbound' end customerServiceEmailConnectionDirection"
        ,"organizationName                          customerServiceServiceTeamName"
        ,"handlingTime                              customerServiceEmailHandlingTimeQuantity"
        ,"responseTime                              customerServiceEmailResponseTimeQuantity"
        ,"workItemID                                customerServiceWorkItemId"
        ,"concat(firstName, ' ', lastName)          customerServiceResponsibleAgentName"
        ,"escalationType                            customerServiceEscalationTypeCode"
        ,"escalationDateTime                        customerServiceEscalationTimestamp"
        ,"escalationTimeInHours                     customerServiceEscalationTimeInHoursQuantity"
        ,"subject                                   customerServiceEmailSubjectDescription"
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
