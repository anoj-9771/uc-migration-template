# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

# %sql
# select eh.* except(eh.workItemId), sht.workItemId from ppd_cleansed.crm.crmd_erms_header eh
# left join ppd_cleansed.crm.swwwihead sh on eh.workItemID = sh.workItemId and sh.workItemTaskId = 'TS00207915'
# left join ppd_cleansed.crm.swwwihead sht on sh.topLevelInstanceUniqueId = sht.topLevelInstanceUniqueId and sht.workItemTaskId = 'TS00207914' where sht.workItemId like '%4240595%'

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
    derivedDF1 = GetTable(f"{getEnv()}{driverTable1}")
else:
    #####CDF for eligible tables#####################
    isDeltaLoad = True  
    derivedDF1 = getSourceCDF(driverTable1, None, False).filter(col("_change_type").rlike('update_postimage|insert')).drop(col("_change_type"))    
    if derivedDF1.count() == 0:
        print("No delta to be  processed")

# COMMAND ----------

df = (derivedDF1.withColumn("email_id",expr("right(emailID, 17)"))) #GetTable(f"{getEnv()}cleansed.crm.crmd_erms_header")
windowSpec1  = Window.partitionBy("email_id") 
df = df.withColumn("row_number",row_number().over(windowSpec1.orderBy(col("changedDate").desc()))).filter("row_number == 1").drop("row_number","email_id")
    
crmd_erms_contnt_df = GetTable(f"{getEnv()}cleansed.crm.crmd_erms_contnt").select(col("EmailID").alias('emailID'), "subject")
   
businessPartner_agent_df = GetTable(f"{getEnv()}cleansed.crm.0bpartner_attr").select("businessPartnerNumber","firstName", "lastName")
businessPartner_orgunit_df = GetTable(f"{getEnv()}cleansed.crm.0bpartner_attr").select("businessPartnerNumber","organizationName")

swwwihead_df = (spark.sql("""select sh.workItemId as workItemId_915, sht.workItemId as workItemId_914 from ppd_cleansed.crm.swwwihead sh 
left join ppd_cleansed.crm.swwwihead sht on sh.topLevelInstanceUniqueId = sht.topLevelInstanceUniqueId and sht.workItemTaskId = 'TS00207914' where sh.workItemTaskId = 'TS00207915'"""))
    
df = (df.join(crmd_erms_contnt_df, "emailID", "left") 
    .join(businessPartner_agent_df, df.responsibleAgent == businessPartner_agent_df.businessPartnerNumber,"left").drop("businessPartnerNumber") 
    .join(businessPartner_orgunit_df, concat(lit("OU"), col("organisationUnit").substr(-8,8)) == businessPartner_orgunit_df.businessPartnerNumber,"left")
    .join(swwwihead_df,df.workItemID == swwwihead_df.workItemId_915, "left")
    .drop("workItemId","workItemId_915")
    .withColumnRenamed("workItemId_914","workItemID"))

if not(TableExists(_.Destination)):
    df = df.unionByName(spark.createDataFrame([dummyRecord(df.schema)], df.schema))
    enableCDF(f"{getEnv()}cleansed.crm.crmd_erms_header")

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
    Save(finaldf)
    #SaveWithCDF(finaldf, 'APPEND')
    #DisplaySelf()
pass
Transform()

# COMMAND ----------

# MAGIC %sql
# MAGIC select customerserviceemailheadersk,count(1) from ppd_curated.dim.customerserviceemailheader group by all having count(1) > 1--where customerServiceWorkItemId like '%4240595%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from  ppd_curated.dim.customerserviceemailheader

# COMMAND ----------


