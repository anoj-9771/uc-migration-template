# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #
    df = GetTable(f"{SOURCE}.crm_0crm_srv_req_inci_h") \
    .withColumn("received_BK",expr("concat(coherentAspectIdD,'|',coherentCategoryIdD)")) \
    .withColumn("resolution_BK",expr("concat(coherentAspectIdC,'|',coherentCategoryIdC)")) \
    .withColumn("processType_BK",expr("concat(ProcessTypeCode,'|','CRM')")) \
    .withColumn("status_BK",expr("concat(statusProfile, '|', statusCode)")) 

    received_category_df = GetTable(f"{DEFAULT_TARGET}.dimcategory") \
    .select("categorySK","_BusinessKey","_recordStart","_recordEnd") \
    .withColumnRenamed("categorySK","receivedCategoryFK")

    resolution_category_df = GetTable(f"{DEFAULT_TARGET}.dimcategory") \
    .select("categorySK","_BusinessKey","_recordStart","_recordEnd") \
    .withColumnRenamed("categorySK","resolutionCategoryFK")

    contact_person_df = GetTable(f"{DEFAULT_TARGET}.dimBusinessPartner") \
    .select("businessPartnerSK","businessPartnerNumber","_recordStart","_recordEnd") \
    .withColumnRenamed("businessPartnerSK","contactPersonFK")

    report_by_person_df = GetTable(f"{DEFAULT_TARGET}.dimBusinessPartner") \
    .select("businessPartnerSK","businessPartnerNumber","_recordStart","_recordEnd") \
    .withColumnRenamed("businessPartnerSK","reportByPersonFK")

    service_team_df = GetTable(f"{DEFAULT_TARGET}.dimBusinessPartner") \
    .select("businessPartnerSK","businessPartnerNumber","_recordStart","_recordEnd") \
    .withColumnRenamed("businessPartnerSK","serviceTeamFK")

    responsible_employee_team_df = GetTable(f"{DEFAULT_TARGET}.dimBusinessPartner") \
    .select("businessPartnerSK","businessPartnerNumber","_recordStart","_recordEnd") \
    .withColumnRenamed("businessPartnerSK","responsibleEmployeeFK")

    contract_df = GetTable(f"{DEFAULT_TARGET}.dimContract") \
    .select("contractSK","_BusinessKey","_recordStart","_recordEnd") \
    .withColumnRenamed("contractSK","contractFK")

    process_type_df = GetTable(f"{DEFAULT_TARGET}.dimProcessType") \
    .select("processTypeSK","_BusinessKey","_recordStart","_recordEnd") \
    .withColumnRenamed("processTypeSK","processTypeFK")

    property_df = GetTable(f"{DEFAULT_TARGET}.dimProperty") \
    .select("propertySK","_BusinessKey","_recordStart","_recordEnd") \
    .withColumnRenamed("propertySK","propertyFK")

    status_df = GetTable(f"{DEFAULT_TARGET}.dimServiceRequestStatus") \
    .select("serviceRequestStatusSK","_BusinessKey","_recordStart","_recordEnd") \
    .withColumnRenamed("serviceRequestStatusSK","StatusFK")
    
    channel_df = GetTable(f"{DEFAULT_TARGET}.dimCommunicationChannel") \
    .select("sourceChannelCode","communicationChannelSK")

    aurion_df = spark.sql(f"""select concat('HR8', RIGHT(concat('000000',ED.personnumber),7)) as personNumber, ED.dateEffective, ED.dateTo, BPM.businessPartnerSK as reportToManagerFK, BPO.businessPartnerSK as organisationUnitFK from cleansed.vw_aurion_employee_details ED
    LEFT JOIN {DEFAULT_TARGET}.dimbusinesspartner BPM on BPM.businessPartnerNumber = concat('HR8', RIGHT(concat('000000',ED.personnumber),7))
    LEFT JOIN {DEFAULT_TARGET}.dimbusinesspartner BPO on BPO.businessPartnerNumber = concat('OU6', RIGHT(concat('000000',ED.OrganisationUnitNumber ),7))""")
    createdBy_username_df = spark.sql(f"""select userid, givenNames as createdBy_givenName, surname as createdBy_surname from {SOURCE}.vw_aurion_employee_details""").drop_duplicates()
    changedBy_username_df = spark.sql(f"""select userid, givenNames as changedBy_givenName, surname as changedBy_surname from {SOURCE}.vw_aurion_employee_details""").drop_duplicates()

    location_df = GetTable(f"{DEFAULT_TARGET}.dimlocation").select("locationSK","locationID","_RecordStart","_RecordEnd")
    
    date_df = GetTable(f"{DEFAULT_TARGET}.dimdate").select("dateSK","calendarDate")
    
    response_df = spark.sql(f"""select F.serviceRequestGUID,S.apptStartDatetime as respondByDateTime,S2.apptStartDatetime as respondedDateTime,case when S2.apptStartDateTime is NULL then NULL else DateDiff(second,F.requestStartDate,COALESCE(S2.apptStartDatetime, F.requestEndDate))/(3600*24) end as interimResponseDays,case 
when S2.apptStartDateTime is NULL then NULL 
when (DateDiff(second,S.apptStartDatetime,COALESCE(S2.apptStartDatetime, F.requestEndDate))/(3600*24)) <= 0 THEN 'Yes' else 'No' END as metInterimResponseFlag from hive_metastore.cleansed.crm_0crm_srv_req_inci_h F
LEFT JOIN {SOURCE}.crm_crmd_link L on F.serviceRequestGUID = L.hiGUID
LEFT JOIN {SOURCE}.crm_scapptseg S on S.ApplicationGUID = L.setGUID and S.apptTypeDescription = 'First Response By'
LEFT JOIN {SOURCE}.crm_scapptseg S2 on S2.ApplicationGUID = L.setGUID and  S2.apptType =(CASE WHEN F.processTypeCode = 'ZCMP' THEN 'VALIDTO' ELSE 'ZRESPONDED' END)
where L.setObjectType = '30'""")

    # ------------- JOINS ------------------ #
    df = df.join(received_category_df,(df.received_BK == received_category_df._BusinessKey) & (df.lastChangedDateTime.between (received_category_df._recordStart,received_category_df._recordEnd)),"left") \
    .drop("_BusinessKey","_recordStart","_recordEnd","received_BK") \
    .join(resolution_category_df,(df.resolution_BK == resolution_category_df._BusinessKey) & (df.lastChangedDateTime.between (resolution_category_df._recordStart,resolution_category_df._recordEnd)),"left") \
    .drop("_BusinessKey","_recordStart","_recordEnd","resolution_BK") \
    .join(contact_person_df,(df.contactPersonNumber == contact_person_df.businessPartnerNumber) & (df.lastChangedDateTime.between (contact_person_df._recordStart,contact_person_df._recordEnd)),"left") \
    .drop("businessPartnerNumber","_recordStart","_recordEnd") \
    .join(report_by_person_df,(df.reportedByPersonNumber == report_by_person_df.businessPartnerNumber) & (df.lastChangedDateTime.between (report_by_person_df._recordStart,report_by_person_df._recordEnd)),"left") \
    .drop("businessPartnerNumber","_recordStart","_recordEnd") \
    .join(service_team_df,(df.serviceTeamCode == service_team_df.businessPartnerNumber) & (df.lastChangedDateTime.between (service_team_df._recordStart,service_team_df._recordEnd)),"left") \
    .drop("businessPartnerNumber","_recordStart","_recordEnd") \
    .join(responsible_employee_team_df,(df.responsibleEmployeeNumber == responsible_employee_team_df.businessPartnerNumber) & (df.lastChangedDateTime.between (responsible_employee_team_df._recordStart,responsible_employee_team_df._recordEnd)),"left") \
    .drop("businessPartnerNumber","_recordStart","_recordEnd") \
    .join(contract_df,(df.contractId == contract_df._BusinessKey) & (df.lastChangedDateTime.between (contract_df._recordStart,contract_df._recordEnd)),"left") \
    .drop("_BusinessKey","_recordStart","_recordEnd") \
    .join(process_type_df,(df.processType_BK == process_type_df._BusinessKey) & (df.lastChangedDateTime.between (process_type_df._recordStart,process_type_df._recordEnd)),"left") \
    .drop("_BusinessKey","_recordStart","_recordEnd") \
    .join(property_df,(df.propertyNumber == property_df._BusinessKey) & (df.lastChangedDateTime.between (property_df._recordStart,property_df._recordEnd)),"left") \
    .drop("_BusinessKey","_recordStart","_recordEnd") \
    .join(status_df,(df.status_BK == status_df._BusinessKey) & (df.lastChangedDateTime.between (status_df._recordStart,status_df._recordEnd)),"left") \
    .join(location_df,(df.propertyNumber == location_df.locationID) & (df.lastChangedDateTime.between (location_df._RecordStart,location_df._RecordEnd)),"left") \
    .drop("_BusinessKey","_recordStart","_recordEnd") \
    .join(date_df, to_date(df.requestStartDate) == date_df.calendarDate,"left").withColumnRenamed('dateSK','serviceRequestStartDateFK').drop("calendarDate") \
    .join(date_df, to_date(df.requestEndDate) == date_df.calendarDate,"left").withColumnRenamed('dateSK','serviceRequestEndDateFK').drop("calendarDate") \
    .join(response_df,"serviceRequestGUID","inner") \
    .join(aurion_df, (df.responsibleEmployeeNumber == aurion_df.personNumber) & (df.lastChangedDateTime.between (aurion_df.dateEffective,aurion_df.dateTo)),"left") \
    .join(createdBy_username_df,df.createdBy == createdBy_username_df.userid, "left").drop("userid") \
    .join(changedBy_username_df,df.changedBy == changedBy_username_df.userid, "left").drop("userid") \
    .join(channel_df,df.communicationChannelCode == channel_df.sourceChannelCode,"left") \
    .withColumn("CreatedByName",concat_ws(" ","createdBy_givenName","createdBy_surname")) \
    .withColumn("changedByName",concat_ws(" ","changedBy_givenName","changedBy_surname"))
    
#     Logic to pick only first record for ServiceRequestGUID. Aurion Data in Test env produces duplicates 
#     Aurion attributes are "reportToManagerFK","organisationUnitFK","CreatedByName","changedByName"
    windowSpec1  = Window.partitionBy("serviceRequestGUID") 
    df = df.withColumn("row_number",row_number().over(windowSpec1.orderBy(lit(1)))).filter("row_number == 1").drop("row_number")


    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"serviceRequestGUID {BK}"
        ,"serviceRequestID serviceRequestId"
        ,"serviceRequestGUID serviceRequestGUID"
        ,"receivedCategoryFK receivedCategoryFK"
        ,"resolutionCategoryFK resolutionCategoryFK"
        ,"communicationChannelSK channelFK"
        ,"contactPersonFK contactPersonFK"
        ,"reportByPersonFK reportByPersonFK"
        ,"serviceTeamFK serviceTeamFK"
        ,"contractFK contractFK"
        ,"responsibleEmployeeFK responsibleEmployeeFK" 
        ,"reportToManagerFK reportToManagerFK"  #aurion
        ,"organisationUnitFK organisationUnitFK" #aurion
        ,"processTypeFK processTypeFK"
        ,"propertyFK propertyFK"
        ,"locationSK locationFK"
        ,"statusFK statusFK"
        ,"salesEmployeeNumber salesEmployeeFK"
        ,"serviceRequestStartDateFK serviceRequestStartDateFK"
        ,"serviceRequestEndDateFK serviceRequestEndDateFK"
        ,"totalDuration totalDuration"
        ,"workDuration workDuration"
        ,"source sourceName"
        ,"sourceCode sourceCode"
        ,"issueResponsibility issueResponsibility"
        ,"issueResponsibilityCode issueResponsibilityCode"
        ,"postingDate postingDate"
        ,"requestStartDate serviceRequestStartDate"
        ,"requestEndDate serviceRequestEndDate"
        ,"numberOfInteractionRecords numberOfInteractions"
        ,"notificationNumber notificationNumber"
        ,"transactionDescription serviceRequestDescription"
        ,"direction direction"
        ,"directionCode directionCode"
        ,"maximoWorkOrderNumber maximoWorkOrderNumber"
        ,"projectId projectId"
        ,"agreementNumber agreementNumber"
        ,"recommendedPriority recommendedPriority"
        ,"impact impactScore"
        ,"urgency urgencyScore"
        ,"serviceLifeCycleUnit serviceLifeCycle"
        ,"serviceLifeCycleUnit serviceLifeCycleUnit"
        ,"activityPriorityCode activityPriorityCode"
        ,"respondByDateTime respondByDateTime"
        ,"respondedDateTime respondedDateTime"
        ,"interimResponseDays interimResponseDays"
        ,"metInterimResponseFlag metInterimResponseFlag"
        ,"CreatedDateTime CreatedDateTime"
        ,"CreatedBy CreatedBy"
        ,"coalesce(CreatedByName, CreatedBy) CreatedByName" #aurion
        ,"lastChangedDate changeDate"
        ,"lastChangedDateTime changeDateTime"
        ,"changedBy changedBy"
        ,"coalesce(changedByName, changedBy) changedByName" #aurion

    ]
    df = df.selectExpr(
        _.Transforms
    ).dropDuplicates()
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    # CleanSelf()
    Save(df)
    #DisplaySelf()
pass
Transform()
