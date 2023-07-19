# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

###cleansed layer table cleansed.crm.crmd_erms_header has delta load -hence handling CDF###
#####Determine Load #################
##Move this to controlDB config if its not complex to derive the change columns needed####

changeColumns = []

###############################
driverTable1 = 'cleansed.crm.crmd_erms_header'   

if not(TableExists(_.Destination)):
    isDeltaLoad = False
    #####Table Full Load #####################
    derivedDF1 = GetTable(f"{getEnv()}{driverTable1}").withColumn("_change_type", lit(None))
else:
    #####CDF for eligible tables#####################
    isDeltaLoad = True
    derivedDF1 = getSourceCDF(driverTable1, changeColumns, False)
    if derivedDF1 is None:
        print("No derivedDF1 Returned")
        dbutils.notebook.exit(f"no CDF to process for table for source {driverTable1} -- Destination {_.Destination}")
    elif derivedDF1.count() == 0:
        print("no CDF to process")
        dbutils.notebook.exit(f"no CDF to process for table for source {driverTable1} -- Destination {_.Destination}")

# COMMAND ----------

df = (derivedDF1.withColumn("email_id",expr("right(emailID, 17)")))
windowSpec1  = Window.partitionBy("email_id") 
df = df.withColumn("row_number",row_number().over(windowSpec1.orderBy(col("changedDate").desc()))).filter("row_number == 1").drop("row_number","email_id")
    
crmd_erms_contnt_df = GetTable(f"{getEnv()}cleansed.crm.crmd_erms_contnt").select(col("EmailID").alias('emailID'), "subject")
   
businessPartner_agent_df = GetTable(f"{getEnv()}cleansed.crm.0bpartner_attr").select("businessPartnerNumber","firstName", "lastName")
businessPartner_orgunit_df = GetTable(f"{getEnv()}cleansed.crm.0bpartner_attr").select("businessPartnerNumber","organizationName")
    
df = (df.join(crmd_erms_contnt_df, "emailID", "left") 
    .join(businessPartner_agent_df, df.responsibleAgent == businessPartner_agent_df.businessPartnerNumber,"left").drop("businessPartnerNumber") 
    .join(businessPartner_orgunit_df, concat(lit("OU"), col("organisationUnit").substr(-8,8)) == businessPartner_orgunit_df.businessPartnerNumber,"left"))

# COMMAND ----------

def Transform():
    global finaldf
    finaldf = df
    
    _.Transforms = [
        f"right(emailId, 17) {BK}"
        ,"right(emailId, 17)                        customerServiceEmailId"
        ,"case when connectionDirection = 1 then externalEmailAddress when connectionDirection = 2 then  internalEmailAddress end customerServiceEmailSenderAddress"
        ,"case when connectionDirection = 1 then internalEmailAddress when connectionDirection = 2 then externalEmailAddress end customerServiceEmailRecipientAddress"
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
    finaldf = finaldf.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
    # display(df)
    #CleanSelf()
    SaveWithCDF(finaldf, 'APPEND')
    #DisplaySelf()
pass
Transform()
