# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{SOURCE}.crm_0crm_sales_act_1") \
    .withColumn("processType_BK",expr("concat(ProcessTypeCode,'|','CRM')")) \
    .withColumn("status_BK",expr("concat(interactionCategory, '|', statusProfile, '|', statusCode)"))
   
    process_type_df = GetTable("curated_v2.dimProcessType") \
    .select("processTypeSK","_BusinessKey","_recordStart","_recordEnd")
        
    status_df = GetTable("curated_v2.dimStatus") \
    .select("StatusSK","_BusinessKey","_recordStart","_recordEnd")
       
    responsible_employee_df = GetTable("curated_v2.dimBusinessPartner") \
    .select("businessPartnerSK","businessPartnerNumber","_recordStart","_recordEnd") \
    .withColumnRenamed("businessPartnerSK","responsibleEmployeeFK")
    
    contact_person_df = GetTable("curated_v2.dimBusinessPartner") \
    .select("businessPartnerSK","businessPartnerNumber","_recordStart","_recordEnd") \
    .withColumnRenamed("businessPartnerSK","contactPersonFK")
   
    property_df = GetTable("curated_v2.dimProperty") \
    .select("propertySK","_BusinessKey","_recordStart","_recordEnd") 
    
    date_df = GetTable("curated.dimdate").select("dateSK","calendarDate")
    
    
    # ------------- JOINS ------------------ #
    df = df.join(process_type_df,(df.processType_BK == process_type_df._BusinessKey) & (df.createdDate.between (process_type_df._recordStart,process_type_df._recordEnd)),"left") \
    .drop("_BusinessKey","_recordStart","_recordEnd","processType_BK") \
    .join(status_df,(df.status_BK == status_df._BusinessKey) & (df.createdDate.between (status_df._recordStart,status_df._recordEnd)),"left") \
    .drop("_BusinessKey","_recordStart","_recordEnd","status_BK") \
    .join(responsible_employee_df,(df.employeeResponsibleNumber == responsible_employee_df.businessPartnerNumber) & (df.createdDate.between (responsible_employee_df._recordStart,responsible_employee_df._recordEnd)),"left") \
    .drop("businessPartnerNumber","_recordStart","_recordEnd") \
    .join(contact_person_df,(df.contactPersonNumber == contact_person_df.businessPartnerNumber) & (df.createdDate.between (contact_person_df._recordStart,contact_person_df._recordEnd)),"left") \
    .drop("businessPartnerNumber","_recordStart","_recordEnd") \
    .join(property_df,(df.propertyNumber == property_df._BusinessKey) & (df.createdDate.between (property_df._recordStart,property_df._recordEnd)),"left") \
    .drop("_BusinessKey","_recordStart","_recordEnd") \
    .join(date_df,to_date(df.createdDate) == date_df.calendarDate,"inner")

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"interactionID {BK}"
        ,"interactionID interactionID"
        ,"processTypeSK processTypeFK"
        ,"communicationChannelCode communicationChannelFK"
        ,"statusSK statusFK"
        ,"responsibleEmployeeFK responsibleEmployeeFK"
        ,"contactPersonFK contactPersonFK"
        ,"propertySK propertyFK"
        ,"dateSK createdDateFK"
        ,"interactionCategory interactionCategory"
        ,"interactionGUID interactionGUID"
        ,"externalNumber externalNumber"
        ,"description description"
        ,"createdDate createdDate"
        ,"priority priority"
        ,"direction direction"
        ,"directionCode directionCode"
        
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    CleanSelf()
    Save(df)
    #DisplaySelf()
pass
Transform()

# COMMAND ----------


