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
    .withColumn("status_BK",expr("concat(serviceRequestCategory, '|', statusProfile, '|', statusCode)")) 

    received_category_df = GetTable("curated_v2. dimcategory") \
    .select("categorySK","_BusinessKey","_recordStart","_recordEnd") \
    .withColumnRenamed("categorySK","receivedCategoryFK")

    resolution_category_df = GetTable("curated_v2. dimcategory") \
    .select("categorySK","_BusinessKey","_recordStart","_recordEnd") \
    .withColumnRenamed("categorySK","resolutionCategoryFK")

    contact_person_df = GetTable("curated_v2.dimBusinessPartner") \
    .select("businessPartnerSK","businessPartnerNumber","_recordStart","_recordEnd") \
    .withColumnRenamed("businessPartnerSK","contactPersonFK")

    report_by_person_df = GetTable("curated_v2.dimBusinessPartner") \
    .select("businessPartnerSK","businessPartnerNumber","_recordStart","_recordEnd") \
    .withColumnRenamed("businessPartnerSK","reportByPersonFK")

    service_team_df = GetTable("curated_v2.dimBusinessPartner") \
    .select("businessPartnerSK","businessPartnerNumber","_recordStart","_recordEnd") \
    .withColumnRenamed("businessPartnerSK","serviceTeamSK")

    responsible_employee_team_df = GetTable("curated_v2.dimBusinessPartner") \
    .select("businessPartnerSK","businessPartnerNumber","_recordStart","_recordEnd") \
    .withColumnRenamed("businessPartnerSK","responsibleEmployeeFK")

    contract_df = GetTable("curated_v2.dimContract") \
    .select("contractSK","_BusinessKey","_recordStart","_recordEnd") \
    .withColumnRenamed("contractSK","contractFK")

    process_type_df = GetTable("curated_v2.dimProcessType") \
    .select("processTypeSK","_BusinessKey","_recordStart","_recordEnd") \
    .withColumnRenamed("processTypeSK","processTypeFK")

    property_df = GetTable("curated_v2.dimProperty") \
    .select("propertySK","_BusinessKey","_recordStart","_recordEnd") \
    .withColumnRenamed("propertySK","propertyFK")

    status_df = GetTable("curated_v2.dimStatus") \
    .select("StatusSK","_BusinessKey","_recordStart","_recordEnd") \
    .withColumnRenamed("StatusSK","StatusFK")

    aurion_df = spark.sql("""select concat('HR8', RIGHT(concat('000000',EM.personnumber),7)) as personNumber, E.dateCommenced, E.dateTo, BPM.businessPartnerSK as reportToManagerFK, BPO.businessPartnerSK as organisationUnitFK from cleansed.aurion_active_employees E 
    INNER JOIN cleansed.aurion_position P on E.positionNumber = P.PositionNumber
    LEFT JOIN  cleansed.aurion_active_employees EM on P.ReportstoPosition = EM.positionNumber
    LEFT JOIN curated_v2.dimbusinesspartner BPM on BPM.businessPartnerNumber = concat('HR8', RIGHT(concat('000000',EM.personnumber),7))
    LEFT JOIN curated_v2.dimbusinesspartner BPO on BPO.businessPartnerNumber = concat('OU6', RIGHT(concat('000000',P.OrganisationUnitNumber ),7))""")

    location_df = GetTable("curated.dimlocation").select("locationSK","locationID","_RecordStart","_RecordEnd")

    date_df = GetTable("curated.dimdate").select("dateSK","calendarDate")

    # ------------- JOINS ------------------ #
    df = df.join(received_category_df,(df.received_BK == received_category_df._BusinessKey) & (df.lastChangedDateTime.between (received_category_df._recordStart,received_category_df._recordEnd)),"left") \
    .drop("_BusinessKey","_recordStart","_recordEnd","received_BK") \
    .join(resolution_category_df,(df.resolution_BK == resolution_category_df._BusinessKey) & (df.lastChangedDateTime.between (resolution_category_df._recordStart,resolution_category_df._recordEnd)),"left") \
    .drop("_BusinessKey","_recordStart","_recordEnd","resolution_BK") \
    .join(contact_person_df,(df.contactPersonNumber == contact_person_df.businessPartnerNumber) & (df.lastChangedDateTime.between (contact_person_df._recordStart,contact_person_df._recordEnd)),"left") \
    .drop("businessPartnerNumber","_recordStart","_recordEnd") \
    .join(report_by_person_df,(df.contactPersonNumber == report_by_person_df.businessPartnerNumber) & (df.lastChangedDateTime.between (report_by_person_df._recordStart,report_by_person_df._recordEnd)),"left") \
    .drop("businessPartnerNumber","_recordStart","_recordEnd") \
    .join(service_team_df,(df.contactPersonNumber == service_team_df.businessPartnerNumber) & (df.lastChangedDateTime.between (service_team_df._recordStart,service_team_df._recordEnd)),"left") \
    .drop("businessPartnerNumber","_recordStart","_recordEnd") \
    .join(responsible_employee_team_df,(df.contactPersonNumber == responsible_employee_team_df.businessPartnerNumber) & (df.lastChangedDateTime.between (responsible_employee_team_df._recordStart,responsible_employee_team_df._recordEnd)),"left") \
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
    .join(aurion_df, (df.responsibleEmployeeNumber == aurion_df.personNumber) & (df.lastChangedDateTime.between (aurion_df.dateCommenced,aurion_df.dateTo)),"left")
   


    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"serviceRequestGUID {BK}"
        ,"serviceRequestID serviceRequestId"
        ,"serviceRequestGUID serviceRequestGUID"
        ,"receivedCategoryFK receivedCategoryFK"
        ,"resolutionCategoryFK resolutionCategoryFK"
        ,"communicationChannelCode channelFK"
        ,"contactPersonFK contactPersonFK"
        ,"reportByPersonFK reportByPersonFK"
        ,"serviceTeamSK serviceTeamSK"
        ,"contractFK contractFK"
        ,"responsibleEmployeeFK responsibleEmployeeFK"
        ,"reportToManagerFK reportToManagerFK"
        ,"organisationUnitFK organisationUnitFK"
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
        ,"CreatedDateTime CreatedDateTime"
        ,"CreatedBy CreatedBy"
        ,"CreatedBy CreatedByName"
        ,"lastChangedDate changeDate"
        ,"lastChangedDateTime changeDateTime"
        ,"changedBy changedBy"
        ,"changedBy changedByName"

    ]
    df = df.selectExpr(
        _.Transforms
    ).dropDuplicates()
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    CleanSelf()
    Save(df)
    #DisplaySelf()
pass
Transform()
