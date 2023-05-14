# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

from pyspark.sql.types import FloatType
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd
import numpy as np
import datetime
from dateutil import tz

def NSWWorkingDaysWithinRange(fromDates: pd.Series, toDates: pd.Series) -> pd.Series:
    fromDates = pd.to_datetime(fromDates, format='%Y-%m-%dT%H:%M:%S.%f%z')
    toDates = pd.to_datetime(toDates, format='%Y-%m-%dT%H:%M:%S.%f%z') 
    publicHolidays = pd.to_datetime(publicHolidaysPD['holidayDate'])

    workingSeconds = []
    for f, t in zip(fromDates, toDates):
        totalSeconds = 0
        while f <= t:
            is_public_holiday = f.date() in publicHolidaysPD.values

            if f.weekday() < 5 and not is_public_holiday:
                endofDay = f.replace(hour=23, minute=59, second=59, microsecond=999999)
                minTime = endofDay if endofDay < t else t
                timeDiff = (minTime - f).total_seconds()
                totalSeconds += timeDiff
                f = f.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=1)
            else:
                f += datetime.timedelta(days=1)
            f = f.replace(tzinfo=None) + (t - t.to_pydatetime().replace(tzinfo=None))
        workingSeconds.append(totalSeconds)

    
    workingDays = np.array(workingSeconds) / (3600 * 24)
    return pd.Series(workingDays)

@pandas_udf(returnType=FloatType())
def workingDaysNSWVectorizedUDF(fromDates: pd.Series, toDates: pd.Series) -> pd.Series:
    return NSWWorkingDaysWithinRange(fromDates, toDates)


# COMMAND ----------

from pyspark.sql.functions import col

def Transform():
    global df
    global publicHolidaysPD
    # ------------- TABLES ----------------- #
    df = GetTable(f"{SOURCE}.crm_0crm_srv_req_inci_h") \
    .withColumn("received_BK",expr("concat(coherentAspectIdD,'|',coherentCategoryIdD)")) \
    .withColumn("resolution_BK",expr("concat(coherentAspectIdC,'|',coherentCategoryIdC)")) \
    .withColumn("processType_BK",expr("concat(trim(processTypeCode),'|','CRM')")) \
    .withColumn("status_BK",expr("concat(statusProfile, '|', statusCode)")) \
    .withColumn("reportedByPersonNumber_BK", expr("CASE WHEN reportedByPersonNumber IS NULL THEN '-1' ELSE ltrim('0',reportedByPersonNumber) END" )) \
    .withColumn("contactPersonNumber_BK", expr("CASE WHEN contactPersonNumber IS NULL THEN '-1' ELSE ltrim('0',contactPersonNumber) END")) \
    .withColumn("salesEmployeeNumber_BK", expr("CASE WHEN salesEmployeeNumber IS NULL THEN '-1' ELSE ltrim('0',salesEmployeeNumber) END")) \
    .withColumn("responsibleEmployeeNumber_BK", expr("CASE WHEN responsibleEmployeeNumber IS NULL THEN '-1' ELSE ltrim('0',responsibleEmployeeNumber) END")) \
    .withColumn("salesEmployeeNumber_BK", expr("CASE WHEN salesEmployeeNumber IS NULL THEN '-1' ELSE ltrim('0',salesEmployeeNumber) END")) \
    .withColumn("propertyNumber_BK", expr("CASE WHEN propertyNumber IS NULL THEN '-1' ELSE propertyNumber END" )) \
    .withColumn("contract_BK", expr("CASE WHEN contractID IS NULL THEN '-1' ELSE contractID END" )) \
    .withColumn("ChannelCode_BK", expr("concat(trim(communicationChannelCode),'|','CRM')"))

    received_category_df = GetTable(f"{DEFAULT_TARGET}.dimcustomerservicecategory") \
    .select("customerServiceCategorySK","_BusinessKey","_recordStart","_recordEnd","sourceBusinessKey","sourceRecordCurrent","sourceValidFromDatetime","sourceValidToDatetime") \
    .withColumnRenamed("customerServiceCategorySK","receivedCategoryFK")

    resolution_category_df = GetTable(f"{DEFAULT_TARGET}.dimcustomerservicecategory") \
    .select("customerServiceCategorySK","_BusinessKey","_recordStart","_recordEnd","sourceBusinessKey","sourceRecordCurrent","sourceValidFromDatetime","sourceValidToDatetime") \
    .withColumnRenamed("customerServiceCategorySK","resolutionCategoryFK")

    contact_person_df = GetTable(f"{DEFAULT_TARGET}.dimBusinessPartner") \
    .select("businessPartnerSK","businessPartnerNumber","_BusinessKey","_recordStart","_recordEnd","_recordCurrent") \
    .withColumnRenamed("businessPartnerSK","contactPersonFK")

    report_by_person_df = GetTable(f"{DEFAULT_TARGET}.dimBusinessPartner") \
    .select("businessPartnerSK","businessPartnerNumber","_BusinessKey","_recordStart","_recordEnd","_recordCurrent") \
    .withColumnRenamed("businessPartnerSK","reportByPersonFK")

    service_team_df = GetTable(f"{DEFAULT_TARGET}.dimBusinessPartner") \
    .select("businessPartnerSK","businessPartnerNumber","_BusinessKey","_recordStart","_recordEnd","_recordCurrent") \
    .withColumnRenamed("businessPartnerSK","serviceTeamFK")

    responsible_employee_team_df = GetTable(f"{DEFAULT_TARGET}.dimBusinessPartner") \
    .select("businessPartnerSK","businessPartnerNumber","_BusinessKey","_recordStart","_recordEnd","_recordCurrent") \
    .withColumnRenamed("businessPartnerSK","responsibleEmployeeFK")

    sales_employee_df = GetTable(f"{DEFAULT_TARGET}.dimBusinessPartner") \
    .select("businessPartnerSK","businessPartnerNumber","_BusinessKey","_recordStart","_recordEnd","_recordCurrent") \
    .withColumnRenamed("businessPartnerSK","salesEmployeeFK")

    contract_df = GetTable(f"{DEFAULT_TARGET}.dimContract") \
    .select("contractSK","_BusinessKey","_recordStart","_recordEnd","_recordCurrent") \
    .withColumnRenamed("contractSK","contractFK")

    process_type_df = GetTable(f"{DEFAULT_TARGET}.dimcustomerserviceprocesstype") \
    .select("customerServiceProcessTypeSK","_BusinessKey","_recordStart","_recordEnd","_recordCurrent") \
    .withColumnRenamed("customerServiceProcessTypeSK","processTypeFK")

    property_df = GetTable(f"{DEFAULT_TARGET}.dimProperty") \
    .select("propertySK","_BusinessKey","_recordStart","_recordEnd","_recordCurrent") \
    .withColumnRenamed("propertySK","propertyFK")

    status_df = GetTable(f"{DEFAULT_TARGET}.dimcustomerservicerequestStatus") \
    .select("customerServiceRequestStatusSK","_BusinessKey","_recordStart","_recordEnd","_recordCurrent") \
    .withColumnRenamed("customerServiceRequestStatusSK","StatusFK")
    
    channel_df = GetTable(f"{DEFAULT_TARGET}.dimCommunicationChannel") \
    .select("customerServiceChannelCode","communicationChannelSK","_recordCurrent","_BusinessKey")

    ####FetchDummyBusinessPartner

    dummyDimPartnerSKD = spark.sql(f""" Select businessPartnerSK from {DEFAULT_TARGET}.dimbusinesspartner where _businessKey = '-1' """)

    first_row = dummyDimPartnerSKD.first()    
    if first_row:
        dummyDimPartnerSK   = first_row["businessPartnerSK"]    


################################ 

    aurion_df = spark.sql(f"""select concat('HR8', RIGHT(concat('000000',ED.personnumber),7)) as personNumber, ED.dateEffective, ED.dateTo, 
                                CASE WHEN BPM.businessPartnerSK IS NULL THEN (Select businessPartnerSK from {DEFAULT_TARGET}.dimbusinesspartner where _businessKey = '-1') ELSE BPM.businessPartnerSK END as reportToManagerFK, 
                                CASE WHEN BPO.businessPartnerSK IS NULL THEN (Select businessPartnerSK from {DEFAULT_TARGET}.dimbusinesspartner where _businessKey = '-1') ELSE BPO.businessPartnerSK END as organisationUnitFK 
                                FROM {SOURCE}.vw_aurion_employee_details ED
                                LEFT JOIN {SOURCE}.vw_aurion_employee_details MA on ED.ReportstoPosition = MA.PositionNumber
                                LEFT JOIN {DEFAULT_TARGET}.dimbusinesspartner BPM on BPM.businessPartnerNumber = CASE WHEN MA.businessPartnerNumber IS NULL THEN '-1' ELSE MA.businessPartnerNumber  END
                                                                                                               and ED.dateEffective between BPM._RecordStart and BPM._RecordEnd                               
                                LEFT JOIN {DEFAULT_TARGET}.dimbusinesspartner BPO on BPO.businessPartnerNumber = CASE WHEN ED.OrganisationUnitNumber IS NULL THEN '-1' ELSE concat('OU6', RIGHT(concat('000000',ED.OrganisationUnitNumber ),7)) END
                                                                                                                and ED.dateEffective between BPO._RecordStart and BPO._RecordEnd""")

    createdBy_username_df = spark.sql(f"""select userid, givenNames as createdBy_givenName, surname as createdBy_surname from {SOURCE}.vw_aurion_employee_details""").drop_duplicates()
    changedBy_username_df = spark.sql(f"""select userid, givenNames as changedBy_givenName, surname as changedBy_surname from {SOURCE}.vw_aurion_employee_details""").drop_duplicates()

    location_df = GetTable(f"{DEFAULT_TARGET}.dimlocation").select("locationSK","locationID","_BusinessKey","_RecordStart","_RecordEnd","_recordCurrent")
    
    date_df = GetTable(f"{DEFAULT_TARGET}.dimdate").select("dateSK","calendarDate")
    
    response_df = spark.sql(f"""select F.serviceRequestGUID,
                            S.apptStartDatetime respondByDateTime,
                            F.requestStartDate startDate, 
                            S2.apptStartDatetime respondedDateTime,                           
                            COALESCE(S2.apptStartDatetime, F.requestEndDate) endDate,
                            CASE WHEN S3.apptType = 'ZCLOSEDATE' THEN S3.apptStartDatetime ELSE NULL END as serviceRequestClosedDateTime,
                            CASE WHEN S3.apptType = 'SRV_RREADY' THEN S3.apptStartDatetime ELSE NULL END as toDoByDateTime,                            
                            DateDiff(second,F.requestStartDate,COALESCE(S2.apptStartDatetime, F.requestEndDate))/(3600*24) as interimResponseDays,
                            case when (DateDiff(second,S.apptStartDatetime,COALESCE(S2.apptStartDatetime, F.requestEndDate))/(3600*24)) <= 0 THEN 'Yes' else 'No' END as metInterimResponseFlag
                            from {SOURCE}.crm_0crm_srv_req_inci_h F
                            LEFT JOIN {SOURCE}.crm_crmd_link L on F.serviceRequestGUID = L.hiGUID
                            LEFT JOIN {SOURCE}.crm_scapptseg S on S.ApplicationGUID = L.setGUID and S.apptTypeDescription = 'First Response By'
                            LEFT JOIN {SOURCE}.crm_scapptseg S2 on S2.ApplicationGUID = L.setGUID and S2.apptType = (CASE WHEN F.processTypeCode = 'ZCMP' THEN 'VALIDTO' ELSE 'ZRESPONDED' END)
                            LEFT JOIN {SOURCE}.crm_scapptseg S3 on S3.ApplicationGUID = L.setGUID and S3.apptType in ('ZCLOSEDATE', 'SRV_RREADY')
                            where L.setObjectType = '30'""") 

    workingcalc_df = response_df.select("serviceRequestGUID", "startDate", "endDate")
    publicHolidaysPD = GetTable(f"{SOURCE}.datagov_australiapublicholidays").filter(col('jurisdiction').rlike("NSW|NAT")) \
                                                                            .filter(upper(col('holidayName')) != "BANK HOLIDAY") \
                                                                        .select('date').withColumnRenamed("date","holidayDate").toPandas()
                                                                        
    workingcalc_df = workingcalc_df.withColumn('interimResponseWorkingDays', workingDaysNSWVectorizedUDF(workingcalc_df['startDate'], workingcalc_df['endDate'])) \
                                     .select("serviceRequestGUID", "interimResponseWorkingDays")

    # ------------- JOINS ------------------ #
    # ------------- JOINS ------------------ #
    df = df.join(received_category_df,(df.received_BK == received_category_df.sourceBusinessKey) & (df.lastChangedDateTime.between (received_category_df.sourceValidFromDatetime,received_category_df.sourceValidToDatetime)),"left") \
    .drop("received_category_df._BusinessKey", "df._recordStart","received_category_df._recordStart", "df._recordEnd", "received_category_df._recordEnd", "df.received_BK") \
    .join(resolution_category_df,(df.resolution_BK == resolution_category_df.sourceBusinessKey) & (df.lastChangedDateTime.between (resolution_category_df.sourceValidFromDatetime,resolution_category_df.sourceValidToDatetime)),"left") \
    .drop("resolution_category_df._BusinessKey","resolution_category_df._recordStart","resolution_category_df._recordEnd","df.resolution_BK", "df._recordStart", "df._recordEnd") \
    .join(contact_person_df,(df.contactPersonNumber_BK == contact_person_df.businessPartnerNumber) & (df.lastChangedDateTime.between (contact_person_df._recordStart,contact_person_df._recordEnd)),"left") \
    .drop("contact_person_df.businessPartnerNumber","contact_person_df._recordStart","contact_person_df._recordEnd", "df._recordStart", "df._recordEnd") \
    .join(report_by_person_df,(df.reportedByPersonNumber_BK == report_by_person_df.businessPartnerNumber) & (df.lastChangedDateTime.between (report_by_person_df._recordStart,report_by_person_df._recordEnd)),"left") \
    .drop("contact_person_df.businessPartnerNumber","contact_person_df._recordStart","contact_person_df._recordEnd", "df._recordStart", "df._recordEnd") \
    .join(service_team_df,(df.serviceTeamCode == service_team_df.businessPartnerNumber) & (df.lastChangedDateTime.between (service_team_df._recordStart,service_team_df._recordEnd)),"left") \
    .drop("service_team_df.businessPartnerNumber","service_team_df._recordStart","service_team_df._recordEnd", "df._recordStart", "df._recordEnd") \
    .join(responsible_employee_team_df,(df.responsibleEmployeeNumber_BK == responsible_employee_team_df.businessPartnerNumber) & (df.lastChangedDateTime.between (responsible_employee_team_df._recordStart,responsible_employee_team_df._recordEnd)),"left") \
    .join(sales_employee_df,(df.salesEmployeeNumber_BK == sales_employee_df.businessPartnerNumber) & (df.lastChangedDateTime.between (sales_employee_df._recordStart,sales_employee_df._recordEnd)),"left") \
    .drop("sales_employee_df.businessPartnerNumber",  "sales_employee_df._recordStart","sales_employee_df._recordEnd", "df._recordStart", "df._recordEnd") \
    .join(contract_df,(df.contract_BK == contract_df._BusinessKey) & (contract_df._recordCurrent == 1),"left") \
    .drop("contract_df._BusinessKey","contract_df._recordStart","contract_df._recordEnd", "df._recordStart", "df._recordEnd") \
    .join(process_type_df,(df.processType_BK == process_type_df._BusinessKey) & (process_type_df._recordCurrent == 1),"left") \
    .drop("process_type_df._BusinessKey","process_type_df._recordStart","process_type_df._recordEnd", "df._recordStart", "df._recordEnd") \
    .join(property_df,(df.propertyNumber_BK == property_df._BusinessKey) & (df.lastChangedDateTime.between (property_df._recordStart,property_df._recordEnd)),"left") \
    .drop("property_df._BusinessKey","property_df._recordStart","property_df._recordEnd", "df._recordStart", "df._recordEnd") \
    .join(status_df,(df.status_BK == status_df._BusinessKey) & (status_df._recordCurrent == 1),"left") \
    .join(location_df,(df.propertyNumber_BK == location_df.locationID) & (df.lastChangedDateTime.between (location_df._RecordStart,location_df._RecordEnd)),"left") \
    .drop("location_df._BusinessKey","location_df._recordStart","location_df._recordEnd", "df._recordStart", "df._recordEnd") \
    .join(date_df, to_date(df.requestStartDate) == date_df.calendarDate,"left").withColumnRenamed('dateSK','serviceRequestStartDateFK').drop("calendarDate") \
    .join(date_df, to_date(df.requestEndDate) == date_df.calendarDate,"left").withColumnRenamed('dateSK','serviceRequestEndDateFK').drop("calendarDate") \
    .join(date_df, to_date(df.lastChangedDate) == date_df.calendarDate,"left").withColumnRenamed('dateSK','snapshotDateFK').drop("calendarDate") \
    .join(response_df,"serviceRequestGUID","inner") \
    .join(workingcalc_df, "serviceRequestGUID","inner") \
    .join(aurion_df, (df.responsibleEmployeeNumber == aurion_df.personNumber) & (df.lastChangedDateTime.between (aurion_df.dateEffective,aurion_df.dateTo)),"left") \
    .join(createdBy_username_df,df.createdBy == createdBy_username_df.userid, "left").drop("userid") \
    .join(changedBy_username_df,df.changedBy == changedBy_username_df.userid, "left").drop("userid") \
    .join(channel_df,(df.ChannelCode_BK == channel_df._BusinessKey) & (channel_df._recordCurrent == 1),"left") \    
    .withColumn("CreatedByName",concat_ws(" ","createdBy_givenName","createdBy_surname")) \
    .withColumn("changedByName",concat_ws(" ","changedBy_givenName","changedBy_surname"))

    
#     Logic to pick only first record for ServiceRequestGUID. Aurion Data in Test env produces duplicates 
#     Aurion attributes are "reportToManagerFK","organisationUnitFK","CreatedByName","changedByName"
    windowSpec1  = Window.partitionBy("serviceRequestGUID") 
    df = df.withColumn("row_number",row_number().over(windowSpec1.orderBy(lit(1)))).filter("row_number == 1").drop("row_number")
    

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"serviceRequestGUID||'|'||lastChangedDateTime {BK}"
        ,"serviceRequestID customerServiceRequestId"
        ,"serviceRequestGUID customerServiceRequestGUID"
        ,"CASE WHEN receivedCategoryFK IS NULL THEN '-1' ELSE receivedCategoryFK END  customerServiceRequestReceivedCategoryFK"      
        ,"CASE WHEN resolutionCategoryFK IS NULL THEN '-1' ELSE resolutionCategoryFK END customerServiceRequestResolutionCategoryFK" 
        ,"CASE WHEN communicationChannelSK IS NULL THEN '-1' ELSE communicationChannelSK END communicationChannelFK"        
        ,f"CASE WHEN contactPersonFK IS NULL THEN '{dummyDimPartnerSK}' ELSE contactPersonFK END contactPersonFK"
        ,"reportByPersonFK reportByPersonFK"
        ,"serviceTeamFK serviceTeamFK"
        ,"contractFK contractFK"
        ,"responsibleEmployeeFK responsibleEmployeeFK" 
        ,"reportToManagerFK  reportToManagerFK"  
        ,"organisationUnitFK organisationUnitFK" 
        ,"CASE WHEN processTypeFK IS NULL THEN '-1' ELSE processTypeFK END customerServiceProcessTypeFK" 
        ,"propertyFK propertyFK"
        ,"locationSK locationFK"
        ,"CASE WHEN statusFK IS NULL THEN '-1' ELSE statusFK END customerServiceRequestStatusFK"  
        ,"salesEmployeeFK salesEmployeeFK"
        ,"serviceRequestStartDateFK customerServiceRequestStartDateFK"
        ,"serviceRequestEndDateFK customerServiceRequestEndDateFK"
        ,"snapshotDateFK customerServiceRequestSnapshotDateFK"        
        ,"totalDuration customerServiceRequestTotalDurationHourQuantity"
        ,"workDuration customerServiceRequestWorkDurationHourQuantity" 
        ,"source customerServiceRequestSourceName"
        ,"sourceCode customerServiceRequestSourceCode"
        ,"issueResponsibility customerServiceRequestIssueResponsibilityName"
        ,"issueResponsibilityCode customerServiceRequestIssueResponsibilityCode"
        ,"postingDate customerServiceRequestPostingDate"
        ,"requestStartDate customerServiceRequestStartTimestamp"
        ,"requestEndDate customerServiceRequestEndTimestamp"
        ,"numberOfInteractionRecords customerServiceRequestInteractionsCount"
        ,"notificationNumber  customerServiceRequestNotificationNumber"
        ,"transactionDescription customerServiceRequestDescription"
        ,"direction customerServiceRequestDirectionIdentifier"
        ,"directionCode customerServiceRequestDirectionCode"
        ,"maximoWorkOrderNumber customerServiceRequestMaximoWorkOrderNumber"
        ,"projectId customerServiceRequestProjectId"
        ,"agreementNumber customerServiceRequestAgreementNumber"
        ,"CAST( recommendedPriority AS INTEGER) customerServiceRequestRecommendedPriorityNumber"
        ,"CAST(impact AS INTEGER) customerServiceRequestImpactScoreNumber"
        ,"CAST(urgency AS INTEGER) customerServiceRequestUrgencyNumber"
        ,"serviceLifeCycle customerServiceRequestServiceLifeCycleUnitHourQuantity"
        ,"serviceLifeCycleUnit customerServiceRequestServiceLifeCycleUnitName"
        ,"activityPriorityCode customerServiceRequestActivityPriorityCode"
        ,"respondByDateTime customerServiceRequestRespondByTimestamp"
        ,"respondedDateTime customerServiceRequestRespondedTimestamp"
        ,"serviceRequestClosedDateTime customerServiceRequestClosedTimestamp"
        ,"toDoByDateTime customerServiceRequestToDoByTimestamp"
        ,"CAST(interimResponseDays        as DECIMAL(15,2)) customerServiceRequestInterimResponseDaysQuantity"
        ,"CAST(interimResponseWorkingDays as DECIMAL(15,2)) customerServiceRequestInterimResponseWorkingDaysQuantity"
        ,"metInterimResponseFlag customerServiceRequestMetInterimResponseIndicator"
        ,"CreatedDateTime customerServiceRequestCreatedTimestamp"
        ,"CreatedBy customerServiceRequestCreatedByUserId" 
        ,"coalesce(CreatedByName, CreatedBy) customerServiceRequestCreatedByUserName" 
        ,"lastChangedDateTime customerServiceRequestSnapshotTimestamp"
        ,"lastChangedDateTime customerServiceRequestLastChangeTimestamp"
        ,"changedBy customerServiceRequestChangedByUserId"
        ,"coalesce(changedByName, changedBy) customerServiceRequestChangedByUserName" 

    ]
    df = df.selectExpr(
        _.Transforms
    ).dropDuplicates()
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
    # display(df)
    #CleanSelf()
    Save(df)
    #DisplaySelf()
pass
Transform()
