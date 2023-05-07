# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    # for dimproperty and dimbuisnesspartner table the default "-1" value is resided in _businessKey as part of UC1 implementation #
    # for all UC3 dimensions the unknown record is implemented as where SK = "-1" , hence the default NULL FK lookup handling can be vary between dimensions #

    df = GetTable(f"{SOURCE}.crm_0crm_sales_act_1") \
    .withColumn("processType_BK",expr("concat(ProcessTypeCode,'|','CRM')")) \
    .withColumn("status_BK",expr("concat(statusProfile, '|', statusCode)")) \
    .withColumn("channel_BK",expr("concat(communicationChannelCode, '|', 'CRM')")) \
    .withColumn("employeeResponsibleNumber_BK",expr("CASE WHEN employeeResponsibleNumber IS NULL THEN '-1' ELSE employeeResponsibleNumber END")) \
    .withColumn("contactPersonNumber_BK",expr("CASE WHEN contactPersonNumber IS NULL THEN '-1' ELSE contactPersonNumber END")) \
    .withColumn("propertyNumber_BK",expr("CASE WHEN propertyNumber IS NULL THEN '-1' ELSE propertyNumber END")) \
    .withColumn("category", expr("'to be provide'")) \
    .withColumn("subCategory" , expr("'to be provide'"))   


    process_type_df = GetTable(f"{DEFAULT_TARGET}.dimCustomerServiceProcessType") \
    .select("customerServiceProcessTypeSK","_BusinessKey","_recordStart","_recordEnd","_recordCurrent")
        
    status_df = GetTable(f"{DEFAULT_TARGET}.dimCustomerInteractionStatus") \
    .select("customerInteractionStatusSK","_BusinessKey","_recordStart","_recordEnd","_recordCurrent")
       
    responsible_employee_df = GetTable(f"{DEFAULT_TARGET}.dimBusinessPartner") \
    .select("businessPartnerSK","businessPartnerNumber","_recordStart","_recordEnd","_businessKey") \
    .withColumnRenamed("businessPartnerSK","responsibleEmployeeFK")
    
    contact_person_df = GetTable(f"{DEFAULT_TARGET}.dimBusinessPartner") \
    .select("businessPartnerSK","businessPartnerNumber","_recordStart","_recordEnd","_businessKey") \
    .withColumnRenamed("businessPartnerSK","contactPersonFK")
   
    property_df = GetTable(f"{DEFAULT_TARGET}.dimProperty") \
    .select("propertySK","_BusinessKey","_recordStart","_recordEnd") 

    channel_df = GetTable(f"{DEFAULT_TARGET}.dimcommunicationchannel") \
    .select("communicationChannelSK","_businessKey","customerServiceChannelCode", "sourceSystemCode", "_recordStart","_recordEnd","_recordCurrent")
    
    date_df = GetTable(f"{DEFAULT_TARGET}.dimdate").select("dateSK","calendarDate")
    
    
    # ------------- JOINS ------------------ #
    df = df.join(process_type_df,(df.processType_BK == process_type_df._BusinessKey) & (process_type_df._recordCurrent == 1),"left") \
    .drop("_BusinessKey","_recordStart","_recordEnd","processType_BK") \
    .join(status_df,(df.status_BK == status_df._BusinessKey) & (status_df._recordCurrent == 1),"left") \
    .drop("_BusinessKey","_recordStart","_recordEnd","status_BK") \
    .join(responsible_employee_df,(df.employeeResponsibleNumber_BK == responsible_employee_df._businessKey) & (df.createdDate.between (responsible_employee_df._recordStart,responsible_employee_df._recordEnd)),"left") \
    .drop("businessPartnerNumber","_recordStart","_recordEnd") \
    .join(contact_person_df,(df.contactPersonNumber_BK == contact_person_df._businessKey) & (df.createdDate.between (contact_person_df._recordStart,contact_person_df._recordEnd)),"left") \
    .drop("businessPartnerNumber","_recordStart","_recordEnd") \
    .join(property_df,(df.propertyNumber_BK == property_df._BusinessKey) & (df.createdDate.between (property_df._recordStart,property_df._recordEnd)),"left") \
    .drop("_BusinessKey","_recordStart","_recordEnd") \
    .join(date_df,to_date(df.createdDate) == date_df.calendarDate,"inner") \
    .join(channel_df,(df.channel_BK == channel_df._businessKey) & (channel_df._recordCurrent == 1 ),"left") \
    .drop("customerServiceChannelCode","_recordStart","_recordEnd","_recordCurrent")     

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"interactionId {BK}"
        ,"interactionId customerInteractionId"
        ,"CASE WHEN customerServiceProcessTypeSK IS NULL THEN '-1' ELSE customerServiceProcessTypeSK END customerInteractionProcessTypeFK"
        ,"CASE WHEN communicationChannelSK IS NULL THEN '-1' ELSE communicationChannelSK END customerInteractionCommunicationChannelFK"
        ,"CASE WHEN customerInteractionStatusSK IS NULL THEN '-1' ELSE customerInteractionStatusSK END customerInteractionStatusFK"
        ,"responsibleEmployeeFK responsibleEmployeeFK"
        ,"contactPersonFK contactPersonFK"
        ,"propertySK propertyFK"
        ,"dateSK createdDateFK"
        ,"interactionGUID customerInteractionGUID"
        ,"category customerInteractionCategory"
        ,"subCategory interationSubCategory"
        ,"externalNumber customerIneteractionExternalNumber"
        ,"description customerInteractionDescription"
        ,"createdDate customerInteractionCreatedTimestamp"
        ,"priority customerInteractionPriorityIndicator"
        ,"direction customerInteractionDirectionIndicator"
        ,"directionCode customerInteractionDirectionCode"        
        
        
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

# COMMAND ----------


