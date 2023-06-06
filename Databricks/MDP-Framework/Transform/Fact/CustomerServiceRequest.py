# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ---------- 

# MAGIC %run ../../Common/common-helpers 
# COMMAND ---------- 


# COMMAND ----------

from pyspark.sql.types import FloatType, DecimalType
from pyspark.sql.functions import pandas_udf, PandasUDFType, unix_timestamp, concat, lit, col, when, regexp_replace, to_timestamp, row_number
from pyspark.sql.window import Window 
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

    workingDays = np.array(workingSeconds) 
    return pd.Series(workingDays)

@pandas_udf(returnType=FloatType())
def workingDaysNSWVectorizedUDF(fromDates: pd.Series, toDates: pd.Series) -> pd.Series:
    return NSWWorkingDaysWithinRange(fromDates, toDates)


def dataDiffTimeStamp(start, end, dtFormat):
     fromDates = to_timestamp(start)
     toDates = to_timestamp(end) 
     return (unix_timestamp(toDates) - unix_timestamp(fromDates)) / dtFormat

# COMMAND ----------

###Variables ############################
dummyDimPartnerSK = '60e35f602481e8c37d48f6a3e3d7c30d' ##Derived by hashing -1 and recordStart
global publicHolidaysPD
###################CONFIG / REFERENCE DF#################################

publicHolidaysPD = (GetTable(f"{get_table_namespace(f'{SOURCE}', 'datagov_australiapublicholidays')}")
                             .filter(col('jurisdiction').rlike("NSW|NAT")) 
                             .filter(upper(col('holidayName')) != "BANK HOLIDAY") 
                            .select('date').withColumnRenamed("date","holidayDate")
                   ).toPandas() 

# COMMAND ----------

#####-----------DIRECT DATAFRAMES CRM--------------------------###############
coreDF =(( GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_0crm_srv_req_inci_h')}")
           .withColumn("sourceSystemCode",lit("CRM"))
           .withColumn("receivedBK", concat(col("coherentAspectIdD"),lit("|"),col("coherentCategoryIdD")))
           .withColumn("resolutionBK", concat(col("coherentAspectIdC"),lit("|"),col("coherentCategoryIdC")))
           .withColumn("processTypeBK", concat(trim(col("processTypeCode")),lit("|"),lit("CRM")))
           .withColumn("statusBK", concat(col("statusProfile"),lit("|"),col("statusCode")))
           .withColumn("reportedByPersonNoBK", when(col("reportedByPersonNumber").isNull(), lit('-1')).otherwise(regexp_replace(col("reportedByPersonNumber"), "^0*", "")))
           .withColumn("contactPersonNoBK", when(col("contactPersonNumber").isNull(), lit('-1')).otherwise(regexp_replace(col("contactPersonNumber"), "^0*", "")))
           .withColumn("salesEmployeeNoBK", when(col("salesEmployeeNumber").isNull(), lit('-1')).otherwise(regexp_replace(col("salesEmployeeNumber"), "^0*", "")))
           .withColumn("responsibleEmployeeNoBK", when(col("responsibleEmployeeNumber").isNull(), lit('-1')).otherwise(regexp_replace(col("responsibleEmployeeNumber"), "^0*", "")))
           .withColumn("propertyNoBK", when(col("propertyNumber").isNull(), lit('-1')).otherwise(col("propertyNumber")))
           .withColumn("contractBK", when(col("contractID").isNull(), lit('-1')).otherwise(col("contractID")))
           .withColumn("channelCodeBK", concat(trim(col("communicationChannelCode")),lit("|"),lit("CRM")))
           .withColumn("ID", monotonically_increasing_id())
           .withColumn("customerServiceRequestTotalDurationSecondQuantity", (dataDiffTimeStamp(col("requestStartDate"), col("requestEndDate"), lit("1").cast("int")))) 
           .withColumn('customerServiceRequestWorkDurationSecondQuantity', (workingDaysNSWVectorizedUDF(col("requestStartDate"), col("requestEndDate")))))
           .select(col("sourceSystemCode")
                  ,col("serviceRequestID")
                  ,col("serviceRequestGUID")
                  ,col("lastChangedDateTime")
                  ,col("customerServiceRequestTotalDurationSecondQuantity")
                  ,col("customerServiceRequestWorkDurationSecondQuantity")  
                  ,col("source")
                  ,col("sourceCode")
                  ,col("serviceTeamCode").alias("serviceTeamCodeBK")
                  ,col("issueResponsibility")
                  ,col("issueResponsibilityCode")
                  ,col("postingDate")
                  ,col("requestStartDate")
                  ,col("requestEndDate")
                  ,col("numberOfInteractionRecords")
                  ,col("notificationNumber")
                  ,col("transactionDescription")
                  ,col("direction")
                  ,col("directionCode")
                  ,col("maximoWorkOrderNumber")
                  ,col("projectID")
                  ,col("processTypeCode")
                  ,col("agreementNumber")
                  ,col("responsibleEmployeeNumber")                  
                  ,col("recommendedPriority").cast("int").alias("recommendedPriority")
                  ,col("impact").cast("int").alias("impact")
                  ,col("urgency").cast("int").alias("urgency")
                  ,col("serviceLifeCycle")
                  ,col("serviceLifeCycleUnit")
                  ,col("activityPriorityCode")
                  ,col("createdDateTime")
                  ,col("createdBy")
                  ,col("changedBy")        
                  ,col("receivedBK")
                  ,col("resolutionBK")
                  ,col("processTypeBK")
                  ,col("statusBK")
                  ,col("reportedByPersonNoBK")
                  ,col("contactPersonNoBK")
                  ,col("salesEmployeeNoBK")
                  ,col("responsibleEmployeeNoBK")
                  ,col("propertyNoBK")
                  ,col("contractBK")
                  ,col("channelCodeBK")
                  ,col("di_Sequence_Number").alias("seqNo")
                  ,col("ID")
                  )                 
        )
    
servCatDF =  ( GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'dimcustomerservicecategory')}")
                             .select( col("customerServiceCategorySK").alias("resolutionCategoryFK")
                                     ,col("customerServiceCategorySK").alias("receivedCategoryFK")
                                     ,col("sourceBusinessKey")                                  
                                     ,col("sourceValidFromDatetime")
                                     ,col("sourceValidToDatetime")
                             )
            )
    
busPartDF = ( GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'dimBusinessPartner')}")
                             .select( col("businessPartnerSK")
                                     ,col("businessPartnerNumber")
                                     ,col("_recordStart")
                                     ,col("_recordEnd")
                      ) 
                 
            )

busPartGrpDF = ( GetTable(f"{DEFAULT_TARGET}.dimBusinessPartnerGroup")
                             .select( col("businessPartnerGroupSK").alias("businessPartnerGroupFK")
                                     ,col("businessPartnerGroupNumber")
                                     ,col("_recordStart")
                                     ,col("_recordEnd")
                      ) 
                 
            )
    

contractDF = ( GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'dimContract')}").filter(col("_recordCurrent") == lit("1"))
                           .select( col("contractSK").alias("contractFK")
                                   ,col("_BusinessKey")
                                  ) 
             )
    
procTypeDF = ( GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'dimcustomerserviceprocesstype')}")
                                          .filter(col("_recordCurrent") == lit("1"))
                           .select( col("customerServiceProcessTypeSK").alias("processTypeFK")
                                     ,col("_BusinessKey")
                                  ) 
             )
    
propertyDF = ( GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'dimProperty')}")
                           .select( col("propertySK").alias("propertyFK")
                                     ,col("_BusinessKey")
                                     ,col("_recordStart")
                                     ,col("_recordEnd")                                  
                              ) 
                 
            )
    
statusDF = ( GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'dimcustomerservicerequestStatus')}")
                                        .filter(col("_recordCurrent") == lit("1"))
                           .select( col("customerServiceRequestStatusSK").alias("StatusFK")
                                     ,col("_BusinessKey")
                                   ) 
            )
    
    
channelDF = ( GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'dimCommunicationChannel')}")
                     .filter(col("_recordCurrent") == lit("1"))
                     .select( col("communicationChannelSK").alias("communicationChannelFK")
                              ,col("_BusinessKey")
                      ) 
                 
           )

crmLinkDF = ( GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_crmd_link')}")
                           .select( col("hiGUID")
                                   ,col("setGUID")).filter(col("setObjectType") == lit("30"))
             
            )

crmSappSegDF = (GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_scapptseg')}").filter(col("apptType").isin(['ZCLOSEDATE', 'SRV_RREADY', 'SRV_RFIRST', 'VALIDTO','ZRESPONDED']))                                                   
                                                   .select(col("applicationGUID"), col("apptType"), col("apptStartDatetime"))
                                                   .groupBy("applicationGUID")
                                                   .pivot("apptType", ['ZCLOSEDATE', 'SRV_RREADY', 'SRV_RFIRST', 'VALIDTO','ZRESPONDED'])
                                                   .agg(max("apptStartDatetime"))
               ) 

aurion_df =  (GetTable(f"{get_table_namespace(f'{SOURCE}', 'vw_aurion_employee_details')}")
                  .withColumn("OrganisationUnitNumberF", when(col("OrganisationUnitNumber").isNull(), lit("-1"))
                                                         .otherwise(concat(lit("OU6"),lpad(col("OrganisationUnitNumber"), 7, "0"))))
            )

auDistDF  = (GetTable(f"{get_table_namespace(f'{SOURCE}', 'vw_aurion_employee_details')}")
                  .select(col("businessPartnerNumber").alias("businessPartnerNumberM")
                         ,col("positionNumber").alias("positionNumberM")
                         ,col("dateEffective").alias("dateEffectiveM")
                         ,col("dateTo").alias("dateToM")).distinct()
            )

ebpDF =    (  GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'dimBusinessPartner')}")
                   .filter(col("_recordCurrent") == 1)
                   .select(col("businessPartnerSK"), col("businessPartnerNumber"))
            )
                   

aurUserDF = (GetTable(f"{get_table_namespace(f'{SOURCE}', 'vw_aurion_employee_details')}")
               .select(col("userid")
                      ,concat_ws(" ",col("givenNames"), col("surname")).alias("createdByName")
                      ,concat_ws(" ",col("givenNames"), col("surname")).alias("ChangedByName")
                      ).drop_duplicates()
            )


locationDF = (GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'dimlocation')}")
                   .select(col("locationSK").alias("locationFK"),
                           col("locationID"),                           
                           col("_RecordStart"),
                           col("_RecordEnd"))
            )

dateDF = (GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'dimDate')}")
                   .select(col("dateSK").alias("serviceRequestStartDateFK"),
                           col("dateSK").alias("serviceRequestEndDateFK"), 
                           col("dateSK").alias("snapshotDateFK"),
                           col("calendarDate"))
            )

# COMMAND ----------

#derived Dataframes ##############
#### (1)     Transpose BusinessPartner to join with coreDF  ################################
busPartDict = {  "contactPersonNoBK": "contactPersonFK", 
                 "reportedByPersonNoBK": "reportByPersonFK", 
                 "serviceTeamCodeBK": "serviceTeamFK",
                 "responsibleEmployeeNoBK": "responsibleEmployeeFK", 
                 "salesEmployeeNoBK": "salesEmployeeFK" 
              }

unionBusPartDF = None
for colName, role in busPartDict.items():
    tempDF =  ( coreDF.select("ID", col(colName).alias("businessPartnerNo"), lit(role).alias("role"), col("lastChangedDateTime")).alias("tc") 
                      .join(busPartDF.alias("sbp") , on = (col("tc.businessPartnerNo") == col("sbp.businessPartnerNumber")) &  
                                                          (col("tc.lastChangedDateTime").between(col("sbp._recordStart"), col("sbp._recordEnd")))
                                                   , how = 'inner')
                      .select( col("tc.ID").alias("ID")
                              ,col("tc.role").alias("role")
                              ,col("sbp.businessPartnerSK").alias("businessPartnerSK")
                             )
               
              )
    if unionBusPartDF is None:
        unionBusPartDF =  tempDF
    else:
        unionBusPartDF = unionBusPartDF.union(tempDF)

pivotBusPartDF = unionBusPartDF.groupBy("ID").pivot("role").agg(first("businessPartnerSK")).alias("pbp")
busDF = coreDF.alias("mn").join(pivotBusPartDF, (col("mn.ID") == col("pbp.ID")), how="inner").drop("mn.ID", 
                                                                                                       "pbp.ID", 
                                                                                                       "mn.contactPersonNoBK", 
                                                                                                       "mn.reportedByPersonNoBK", 
                                                                                                       "mn.serviceTeamCodeBK", 
                                                                                                       "mn.responsibleEmployeeNoBK", 
                                                                                                       "mn.salesEmployeeNoBK").cache()

# COMMAND ----------

###############derived Dataframes ##### (2) Aurion Logic ###################
window_spec = ( Window.partitionBy(col("auE.businessPartnerNumber")).orderBy( col("auE.dateEffective"),
        when(col("auE.EmployeeStatus") == "ACTIVE", 1) 
        .when(col("auE.EmployeeStatus") == "TERMINATED", 2) 
        .when(col("auE.EmployeeStatus") == "HISTORY", 3) 
        .otherwise(4)  # Handle other cases 
    ) )

aurionDF = ( (((
  aurion_df.alias("auE").join(auDistDF.alias("auM"), 
    ((col("auE.ReportstoPosition") == col("auM.PositionNumberM")) &
    (col("auE.dateEffective").between(col("auM.dateEffectiveM"), col("auM.dateToM")))),"left") 
  .withColumn("rownumber", row_number().over(window_spec)).filter(col("rownumber") == 1))
  .select(col("*")).filter(col("auM.dateEffectiveM").isNotNull()))  
  .join(ebpDF.alias("ebp"), 
        col("auM.businessPartnerNumberM") == col("ebp.businessPartnerNumber"), "left")
  .join(ebpDF.alias("obp"), 
         col("auE.OrganisationUnitNumberF") == col("obp.businessPartnerNumber"), "left"))
  .withColumn("reportToManagerFK", col("ebp.businessPartnerSK"))
  .withColumn("organisationUnitFK", col("obp.businessPartnerSK"))
  .drop(col("ebp.businessPartnerNumber"), 
        col("ebp.businessPartnerSK"),
        col("obp.businessPartnerNumber"),
        col("obp.businessPartnerSK"))
   .select(col("businessPartnerNumber"), col("reportToManagerFK"), col("organisationUnitFK"), col("DateEffective"), col("DateTo"))
)  

# COMMAND ----------

##############################Main DATAFRAME / Join FOR CRM ###############################
finalCRMDF = (((busDF.alias("core") 
  .join(crmLinkDF.alias("cl"), (col("core.serviceRequestGUID") == col("cl.hiGUID")), how= "left")  
 .join(crmSappSegDF.alias("css"), (col("cl.setGUID") == col("css.applicationGUID")), how= "left").drop("cl.setGUID", "cl.hiGUID", "css.applicationGUID")
 .withColumn("endDate", coalesce(when(col("core.processTypeCode") == lit("ZCMP"), 
                         col("css.VALIDTO")).otherwise(col("css.ZRESPONDED")), 
                                                     col("core.requestEndDate"))))  
 .join(contractDF.alias("sv"), (col("core.contractBK") == col("sv._BusinessKey")), "left").drop("core.contractBK", "sv._BusinessKey")
 .join(procTypeDF.alias("pc"), (col("core.processTypeBK") == col("pc._BusinessKey")), "left").drop("core.processTypeBK","pc._BusinessKey") 
 .join(channelDF.alias("ch"), (col("core.channelCodeBK") == col("ch._BusinessKey")), "left").drop("core.channelCodeBK", "ch._BusinessKey")
 .join(dateDF.alias("dt1"), (to_date(col("core.requestStartDate")) == col("dt1.calendarDate")), "left").drop("dt1.calendarDate", "dt1.serviceRequestEndDateFK", "dt1.snapshotDateFK")
 .join(dateDF.alias("dt2"), (to_date(col("core.requestEndDate")) == col("dt2.calendarDate")), "left").drop("dt2.calendarDate", "dt2.serviceRequestStartDateFK", "dt2.snapshotDateFK") 
 .join(dateDF.alias("dt3"), (to_date(col("core.lastChangedDateTime")) == col("dt3.calendarDate")), "left").drop("dt3.calendarDate", "dt3.serviceRequestStartDateFK","dt3.serviceRequestEndDateFK") 
 .join(aurUserDF.alias("au1"), (col("core.createdBy") == col("au1.userid")), "left").drop("au1.userid", "au1.changedByName")
 .join(aurUserDF.alias("au2"), (col("core.changedBy") == col("au2.userid")), "left").drop("au2.userid", "au2.createdByName")
 .join(aurionDF.alias("au"), 
       ((col("core.responsibleEmployeeNumber") == col("au.businessPartnerNumber")) &
           (col("core.lastChangedDateTime").between(col("au.DateEffective"), 
                                                          col("au.DateTo")))), "left").drop("au.businessPartnerNumber", "au.DateEffective", "au.DateTo")  
 .join(propertyDF.alias("pr"), ((col("core.propertyNoBK") == col("pr._BusinessKey")) &  
                 (col("core.lastChangedDateTime").between(col("pr._recordStart"), 
                                                          col("pr._recordEnd")))), "left").drop("pr._BusinessKey", "pr._recordStart", "pr._recordEnd")  
 .join(locationDF.alias("lo"), ((col("core.propertyNoBK") == col("lo.locationID")) &  
                 (col("core.lastChangedDateTime").between(col("lo._recordStart"), 
                                                          col("lo._recordEnd")))),"left").drop("lo.locationID", "lo._recordStart", "lo._recordEnd") 
 .join(busPartGrpDF.alias("bgp"), ((col("core.propertyNoBK") == col("bgp.businessPartnerGroupNumber")) &  
                 (col("core.lastChangedDateTime").between(col("bgp._recordStart"), 
                                                col("bgp._recordEnd")))), "left").drop("bgp.businessPartnerGroupNumber", "bgp._recordStart", "bgp._recordEnd")
 .join(servCatDF.alias("sc1"), ((col("core.receivedBK") == col("sc1.sourceBusinessKey")) &
           (col("core.lastChangedDateTime").between(col("sc1.sourceValidFromDatetime"), 
                                                          col("sc1.sourceValidToDatetime")))), "left").drop("sc1.sourceBusinessKey", "sc1.sourceValidFromDatetime", "sc1.sourceValidToDatetime", "sc1.resolutionCategoryFK") 
 .join(servCatDF.alias("sc2"), ((col("core.resolutionBK") == col("sc2.sourceBusinessKey")) &
           (col("core.lastChangedDateTime").between(col("sc2.sourceValidFromDatetime"), 
                                                          col("sc2.sourceValidToDatetime")))), "left").drop("sc2.sourceBusinessKey", "sc2.sourceValidFromDatetime", "sc2.sourceValidToDatetime","sc2.receivedCategoryFK")  
 .join(statusDF.alias("st"), (col("core.statusBK") == col("st._BusinessKey")), "left").drop("core.statusBK", "st._BusinessKey")
 .withColumn("BusinessKey", concat_ws("|", col("serviceRequestGUID").cast("string"), col("lastChangedDateTime").cast("string")
                                          ,when(col("seqNo").isNull(), lit('')).otherwise(col("seqNo"))))
 .withColumn("respondByDateTime", col("css.SRV_RFIRST")) 
 .withColumn("respondedDateTime", when(col("core.processTypeCode") == lit("ZCMP"), 
                                  col("css.VALIDTO")).otherwise(col("css.ZRESPONDED"))) 
 .withColumn("serviceRequestClosedDateTime", col("css.ZCLOSEDATE")) 
 .withColumn("toDoByDateTime", col("css.SRV_RREADY")) 
 .withColumn("interimResponseDays", 
                                 dataDiffTimeStamp( col("core.requestStartDate")
                                 ,col("endDate")
                                 ,lit("86400").cast("int"))
                            )
 .withColumn("metInterimResponseFlag", 
                         when(dataDiffTimeStamp(col("css.SRV_RFIRST")
                                            ,col("endDate")
                                            ,lit("86400").cast("int")) <= 0, "Yes").otherwise("No")
                            )
 .withColumn('interimResponseWorkingDays', 
            (workingDaysNSWVectorizedUDF( col("core.requestStartDate")
                                         ,col("endDate")) / lit("86400").cast("int"))
                            )
         ).select(col("BusinessKey").alias(f"{BK}")
                 ,col("sourceSystemCode")
                 ,col("serviceRequestID").alias("customerServiceRequestId")
                 ,col("serviceRequestGUID").alias("customerServiceRequestGUID")
                 ,when(col("receivedCategoryFK").isNull(), lit('-1')).otherwise(col("receivedCategoryFK")).alias("customerServiceRequestReceivedCategoryFK")
                 ,when(col("resolutionCategoryFK").isNull(), lit('-1')).otherwise(col("resolutionCategoryFK")).alias("customerServiceRequestResolutionCategoryFK")
                 ,when(col("communicationChannelFK").isNull(), lit('-1')).otherwise(col("communicationChannelFK")).alias("communicationChannelFK") 
                 ,when(col("contactPersonFK").isNull(), lit(f"{dummyDimPartnerSK}")).otherwise(col("contactPersonFK")).alias("contactPersonFK")
                 ,col("reportByPersonFK")
                 ,col("serviceTeamFK")
                 ,col("contractFK")
                 ,col("responsibleEmployeeFK")
                 ,col("reportToManagerFK")
                 ,col("organisationUnitFK")
                 ,when(col("processTypeFK").isNull(), lit('-1')).otherwise(col("processTypeFK")).alias("customerServiceProcessTypeFK")
                 ,col("propertyFK")
                 ,col("locationFK")
                 ,col("businessPartnerGroupFK")
                 ,when(col("statusFK").isNull(), lit('-1')).otherwise(col("statusFK")).alias("customerServiceRequestStatusFK")
                 ,col("salesEmployeeFK")
                 ,col("serviceRequestStartDateFK").alias("customerServiceRequestStartDateFK")
                 ,col("serviceRequestEndDateFK").alias("customerServiceRequestEndDateFK")
                 ,col("snapshotDateFK").alias("customerServiceRequestSnapshotDateFK")
                 ,col("customerServiceRequestTotalDurationSecondQuantity")
                 ,col("customerServiceRequestWorkDurationSecondQuantity")
                 ,col("source").alias("customerServiceRequestSourceName")
                 ,col("sourceCode").alias("customerServiceRequestSourceCode")
                 ,col("issueResponsibility").alias("customerServiceRequestIssueResponsibilityName")
                 ,col("issueResponsibilityCode").alias("customerServiceRequestIssueResponsibilityCode")
                 ,col("postingDate").alias("customerServiceRequestPostingDate")
                 ,col("requestStartDate").alias("customerServiceRequestStartTimestamp")
                 ,col("requestEndDate").alias("customerServiceRequestEndTimestamp")
                 ,col("numberOfInteractionRecords").alias("customerServiceRequestInteractionsCount")
                 ,col("notificationNumber").alias("customerServiceRequestNotificationNumber")
                 ,col("transactionDescription").alias("customerServiceRequestDescription")
                 ,col("direction").alias("customerServiceRequestDirectionIdentifier")
                 ,col("directionCode").alias("customerServiceRequestDirectionCode")
                 ,col("maximoWorkOrderNumber").alias("customerServiceRequestMaximoWorkOrderNumber")
                 ,col("projectId").alias("customerServiceRequestProjectId")
                 ,col("agreementNumber").alias("customerServiceRequestAgreementNumber")
                 ,col("recommendedPriority").alias("customerServiceRequestRecommendedPriorityNumber")
                 ,col("impact").alias("customerServiceRequestImpactScoreNumber")
                 ,col("urgency").alias("customerServiceRequestUrgencyNumber")
                 ,col("serviceLifeCycle").alias("customerServiceRequestServiceLifeCycleUnitHourQuantity")
                 ,col("serviceLifeCycleUnit").alias("customerServiceRequestServiceLifeCycleUnitName")
                 ,col("activityPriorityCode").alias("customerServiceRequestActivityPriorityCode")
                 ,col("respondByDateTime").alias("customerServiceRequestRespondByTimestamp")
                 ,col("respondedDateTime").alias("customerServiceRequestRespondedTimestamp")
                 ,col("serviceRequestClosedDateTime").alias("customerServiceRequestClosedTimestamp")
                 ,col("toDoByDateTime").alias("customerServiceRequestToDoByTimestamp")
                 ,col("interimResponseDays").cast(DecimalType(15,2)).alias("customerServiceRequestInterimResponseDaysQuantity")
                 ,col("interimResponseWorkingDays").cast(DecimalType(15,2)).alias("customerServiceRequestInterimResponseWorkingDaysQuantity")
                 ,col("metInterimResponseFlag").alias("customerServiceRequestMetInterimResponseIndicator")
                 ,col("CreatedDateTime").alias("customerServiceRequestCreatedTimestamp")
                 ,col("CreatedBy").alias("customerServiceRequestCreatedByUserId")
                 ,coalesce(col("createdByName"), col("CreatedBy")).alias("customerServiceRequestCreatedByUserName")
                 ,col("lastChangedDateTime").alias("customerServiceRequestSnapshotTimestamp")
                 ,col("lastChangedDateTime").alias("customerServiceRequestLastChangeTimestamp")
                 ,col("changedBy").alias("customerServiceRequestChangedByUserId")
                 ,coalesce(col("changedByName"), col("changedBy")).alias("customerServiceRequestChangedByUserName")
                )
)

# COMMAND ----------

##############################Main DATAFRAME / Join FOR MAXIMO ###############################
df2 = spark.sql(f""" WITH MAXIMO AS (SELECT 
                        'MAXIMO' as sourceSystemCode,
                        'ZCMP' as processTypeCode,
                        'Complaint' as processType,
                         CAST(WO.reportedDateTime as DATE ) as calendarDate,
                         WO.reportedDateTime as calendarDatetime,
                         WO.statusDate as lastChangedDateTime,
                         SR.serviceRequest as serviceRequestGUID,
                         WO.workOrder as maximoWorkOrderNumber,
                         SR.serviceRequest as serviceRequestId,
                         SR.propertyNumber,
                         WO.status workOrderStatus,
                         Concat('Product : ' , CASE WHEN LOC.product IS NULL THEN '' ELSE LOC.product END , CHAR(10),CHAR(13),
                                'Problem Type : ' , CASE WHEN PT.description IS NULL THEN '' ELSE PT.description END , CHAR(10),CHAR(13),
                                'Service Type : ' , CASE WHEN WO.serviceType IS NULL THEN '' ELSE WO.serviceType END , CHAR(10),CHAR(13),
                                'Job Plan : ' , CASE WHEN WO.jobPlan IS NULL THEN '' ELSE CAST(WO.jobPlan as string) END, CHAR(10),CHAR(13),
                                'Task Code : ' , CASE WHEN WO.taskCode IS NULL THEN '' ELSE CAST(WO.taskCode as string) END, CHAR(10),CHAR(13),
                                'Job Plan ID : ' , CASE WHEN JP.jobPlanId IS NULL THEN '' ELSE CAST(JP.jobPlanId as string) END, CHAR(10),CHAR(13),
                                'Call Type : ' , CASE WHEN SR.callType IS NULL THEN '' ELSE CAST(SR.callType as string) END, CHAR(10),CHAR(13),
                                'Word Order Description : ', CASE WHEN WO.description IS NULL THEN '' ELSE WO.description END 
                               ) as transactionDescription,
                        WO.reportedDateTime as createdDatetime,
                        WO.actualStart      as requestStartDate,
                        CASE WHEN ((WO.actualFinish IS NULL OR YEAR(WO.actualFinish) = 9999) AND WO.status = 'FINISHED') THEN WO.statusDate ELSE WO.actualFinish END as requestEndDate,
                        'ZSERVREQ' as statusProfile,
                        CASE WO.status WHEN 'FINISHED' THEN 'E0012' WHEN 'CAN' THEN 'E0003'END as statusCode,
                        'ZSW_SERV_REQ' as coherentAspectIDD,  
                        'ZSW_SERV_REQ_RES_CAT' as  coherentAspectIDC,
                        CASE WHEN LOC.product = 'Water' AND PT.description = 'Discoloured Supply' THEN 'WK_12'
                             WHEN LOC.product = 'Water' AND PT.description = 'Bad Smell' THEN 'WK_15'
                             WHEN LOC.product = 'Water' AND PT.description = 'Bad Taste' THEN 'WK_16'
                             WHEN LOC.product = 'Water' AND PT.description = 'Discoloured Supply - Brown' THEN 'WK_12'
                             WHEN LOC.product = 'Water' AND PT.description = 'Discoloured Supply - Particles' THEN 'WK_12'
                             WHEN LOC.product = 'Water' AND PT.description = 'Discoloured Supply - Yellow' THEN 'WK_12'
                             WHEN LOC.product = 'Water' AND PT.description = 'Discoloured Supply - Red' THEN 'WK_12'
                             WHEN LOC.product = 'Water' AND PT.description = 'Discoloured Supply - Orange' THEN 'WK_12'
                             WHEN LOC.product = 'Water' AND PT.description = 'Discoloured Supply - Green' THEN 'WK_12'
                             WHEN LOC.product = 'Water' AND PT.description = 'Discoloured Supply - Black' THEN 'WK_12'
                             WHEN LOC.product = 'Water' AND PT.description = 'Discoloured Supply - White' THEN 'WK_12'
                             WHEN LOC.product = 'Water' AND PT.description = 'Discoloured Supply - Blue' THEN 'WK_12'
                             WHEN LOC.product = 'WasteWater' AND    PT.description = 'Odour Enquiry' THEN 'OD_15'    
                             WHEN LOC.product = 'StormWater' AND    PT.description = 'Flooding' THEN 'SD_12'    
                             WHEN LOC.product = 'RecycledWater' AND PT.description = 'Discoloured Supply' THEN 'WZ_11'
                             WHEN LOC.product = 'RecycledWater' AND PT.description = 'Bad Smell' THEN 'WZ_14'
                             WHEN LOC.product = 'RecycledWater' AND PT.description = 'Bad Taste' THEN 'WZ_13' -- other
                             WHEN LOC.product = 'RecycledWater' AND PT.description = 'Discoloured Supply - Brown' THEN 'WZ_11'
                             WHEN LOC.product = 'RecycledWater' AND PT.description = 'Discoloured Supply - Particles' THEN 'WZ_11'
                             WHEN LOC.product = 'RecycledWater' AND PT.description = 'Discoloured Supply - Yellow' THEN 'WZ_11'
                             WHEN LOC.product = 'RecycledWater' AND PT.description = 'Discoloured Supply - Red' THEN 'WZ_11'
                             WHEN LOC.product = 'RecycledWater' AND PT.description = 'Discoloured Supply - Orange' THEN 'WZ_11'
                             WHEN LOC.product = 'RecycledWater' AND PT.description = 'Discoloured Supply - Green' THEN 'WZ_11'
                             WHEN LOC.product = 'RecycledWater' AND PT.description = 'Discoloured Supply - Black' THEN 'WZ_11'
                             WHEN LOC.product = 'RecycledWater' AND PT.description = 'Discoloured Supply - White' THEN 'WZ_11'
                             WHEN LOC.product = 'RecycledWater' AND PT.description = 'Discoloured Supply - Blue' THEN 'WZ_11'  
                             ELSE '' END as coherentCategoryIDD,
   
                        CASE WHEN LOC.product = 'Water' AND PT.description = 'Discoloured Supply' THEN 'WK_12'
                             WHEN LOC.product = 'Water' AND PT.description = 'Bad Smell' THEN 'WK_143'
                             WHEN LOC.product = 'Water' AND PT.description = 'Bad Taste' THEN 'WK_150'
                             WHEN LOC.product = 'Water' AND PT.description = 'Discoloured Supply - Brown' THEN 'WK_115'
                             WHEN LOC.product = 'Water' AND PT.description = 'Discoloured Supply - Particles' THEN 'WK_137'
                             WHEN LOC.product = 'Water' AND PT.description = 'Discoloured Supply - Yellow' THEN 'WK_117'
                             WHEN LOC.product = 'Water' AND PT.description = 'Discoloured Supply - Red' THEN 'WK_115'
                             WHEN LOC.product = 'Water' AND PT.description = 'Discoloured Supply - Orange' THEN 'WK_117'
                             WHEN LOC.product = 'Water' AND PT.description = 'Discoloured Supply - Green' THEN 'WK_114'
                             WHEN LOC.product = 'Water' AND PT.description = 'Discoloured Supply - Black' THEN 'WK_113'
                             WHEN LOC.product = 'Water' AND PT.description = 'Discoloured Supply - White' THEN 'WK_116'
                             WHEN LOC.product = 'Water' AND PT.description = 'Discoloured Supply - Blue' THEN 'WK_114'
                             WHEN LOC.product = 'WasteWater' AND    PT.description = 'Odour Enquiry' THEN 'OD_16'    
                             WHEN LOC.product = 'StormWater' AND    PT.description = 'Flooding'                      THEN 'SD_12'    
                             WHEN LOC.product = 'RecycledWater' AND PT.description = 'Discoloured Supply'             THEN 'WZ_11'
                             WHEN LOC.product = 'RecycledWater' AND PT.description = 'Bad Smell'                      THEN 'WZ_126'
                             WHEN LOC.product = 'RecycledWater' AND PT.description = 'Bad Taste'                      THEN 'WZ_13' -- other
                             WHEN LOC.product = 'RecycledWater' AND PT.description = 'Discoloured Supply - Brown'     THEN 'WZ_111'
                             WHEN LOC.product = 'RecycledWater' AND PT.description = 'Discoloured Supply - Particles' THEN 'WZ_11'
                             WHEN LOC.product = 'RecycledWater' AND PT.description = 'Discoloured Supply - Yellow' THEN 'WZ_116'
                             WHEN LOC.product = 'RecycledWater' AND PT.description = 'Discoloured Supply - Red'    THEN 'WZ_116'
                             WHEN LOC.product = 'RecycledWater' AND PT.description = 'Discoloured Supply - Orange' THEN 'WZ_116'
                             WHEN LOC.product = 'RecycledWater' AND PT.description = 'Discoloured Supply - Green'  THEN 'WZ_112'
                             WHEN LOC.product = 'RecycledWater' AND PT.description = 'Discoloured Supply - Black'  THEN 'WZ_111'
                             WHEN LOC.product = 'RecycledWater' AND PT.description = 'Discoloured Supply - White'  THEN 'WZ_113'
                             WHEN LOC.product = 'RecycledWater' AND PT.description = 'Discoloured Supply - Blue'   THEN 'WZ_112'  
                             ELSE '' END as coherentCategoryIDC ,
                        CASE WHEN WO.taskCode = 'IVA1'   THEN 'Sydney Water'
                             WHEN WO.taskCode like 'WR%' THEN 'Sydney Water Contractor' ELSE '' END as issueResponsibility,
                        CASE WHEN WO.taskCode = 'IVA1'   THEN '10'
                             WHEN WO.taskCode like 'WR%' THEN '11' ELSE '' END as issueResponsibilityCode,
                        row_number() Over(partition by WO.workOrder order by CAST(WO.rowStamp as BIGINT) desc)  as rkn          
                    FROM {get_table_namespace('cleansed', 'maximo_workorder')} WO
                    LEFT JOIN {get_table_namespace('cleansed', 'maximo_swcproblemtype')} PT on WO.problemType = PT.problemType
                    LEFT JOIN {get_table_namespace('cleansed', 'maximo_jobplan')} JP on WO.jobTaskId = jp.jobPlanId
                    LEFT JOIN {get_table_namespace('cleansed', 'maximo_locoper')} LOC on WO.location =  LOC.location  
                    LEFT JOIN {get_table_namespace('cleansed', 'maximo_relatedrecord')} REL on WO.workOrder = REL.recordKey
                    LEFT JOIN {get_table_namespace('cleansed', 'maximo_ticket')} SR on   SR.serviceRequest = REL.relatedRecordKey
                    LEFT JOIN {get_table_namespace('cleansed', 'maximo_failurereport')} FR on WO.workOrder = FR.workOrder AND FR.type = 'REMEDY'
                    WHERE WO.status in ('FINISHED','CAN') 
                      AND WO.serviceType != 'R' 
                      AND SR.callType = 'J'
                      AND ( (     LOC.product IN('Water','RecycledWater') and PT.description in ('Discoloured Supply',
                                              'Bad Smell',
                                              'Bad Taste',
                                              'Discoloured Supply - Brown',
                                              'Discoloured Supply - Particles',
                                              'Discoloured Supply - Yellow',
                                              'Discoloured Supply - Red',
                                              'Discoloured Supply - Orange',
                                              'Discoloured Supply - Green',
                                              'Discoloured Supply - Black',
                                              'Discoloured Supply - White',
                                              'Discoloured Supply - Blue' ))
                              OR (LOC.product = 'StormWater' and PT.description in ('Flooding')) 
                              OR (LOC.product = 'WasteWater' and WO.parentWo IS NULL AND FR.failureCode = 'RWW-SR2H') 
                          )
                      AND WO.reportedDateTime>='2019-06-01' ) SELECT * from MAXIMO where rkn = 1 """) 


# ------------- JOINS ------------------ #

df2 = ( df2.withColumn("receivedBK",expr("concat(coherentAspectIdD,'|',coherentCategoryIdD)")) 
             .withColumn("resolutionBK",expr("concat(coherentAspectIdC,'|',coherentCategoryIdC)")) 
             .withColumn("processTypeBK",expr("concat(trim(processTypeCode),'|','CRM')")) 
             .withColumn("statusBK",expr("concat(statusProfile, '|', statusCode)")) 
             .withColumn("propertyNoBK", expr("CASE WHEN propertyNumber IS NULL THEN '-1' ELSE propertyNumber END" ))                      
             .withColumn("customerServiceRequestTotalDurationSecondQuantity", (dataDiffTimeStamp(col("requestStartDate"), col("requestEndDate"), lit("1").cast("int")))) #, lit("3600").cast("int")
             .withColumn("customerServiceRequestWorkDurationSecondQuantity", when(col("workOrderStatus") == lit("FINISHED"), 
                                            (workingDaysNSWVectorizedUDF(col("requestStartDate"), col("requestEndDate")) )).otherwise(lit("0")) #/ 3600
                        )
        )

finalMAXDF = ((df2.alias("core")           
      .join(procTypeDF.alias("pc"), (col("core.processTypeBK") == col("pc._BusinessKey")), "left").drop("core.processTypeBK","pc._BusinessKey") 
      .join(statusDF.alias("st"), (col("core.statusBK") == col("st._BusinessKey")), "left").drop("core.statusBK", "st._BusinessKey")
      .join(dateDF.alias("dt1"), (to_date(col("core.requestStartDate")) == col("dt1.calendarDate")), "left").drop("dt1.calendarDate", "dt1.serviceRequestEndDateFK", "dt1.snapshotDateFK")
      .join(dateDF.alias("dt2"), (to_date(col("core.requestEndDate")) == col("dt2.calendarDate")), "left").drop("dt2.calendarDate", "dt2.serviceRequestStartDateFK", "dt2.snapshotDateFK") 
      .join(dateDF.alias("dt3"), (to_date(col("core.lastChangedDateTime")) == col("dt3.calendarDate")), "left").drop("dt3.calendarDate", "dt3.serviceRequestStartDateFK","dt3.serviceRequestEndDateFK") 
      .join(propertyDF.alias("pr"), ((col("core.propertyNoBK") == col("pr._BusinessKey")) &  
                 (col("core.lastChangedDateTime").between(col("pr._recordStart"), 
                                                          col("pr._recordEnd")))), "left").drop("pr._BusinessKey", "pr._recordStart", "pr._recordEnd") 
      .join(locationDF.alias("lo"), ((col("core.propertyNoBK") == col("lo.locationID")) &  
                 (col("core.lastChangedDateTime").between(col("lo._recordStart"), 
                                                          col("lo._recordEnd")))),"left").drop("lo.locationID", "lo._recordStart", "lo._recordEnd") 
      .join(busPartGrpDF.alias("bgp"), ((col("core.propertyNoBK") == col("bgp.businessPartnerGroupNumber")) &  
                 (col("core.lastChangedDateTime").between(col("bgp._recordStart"), 
                                                col("bgp._recordEnd")))), "left").drop("bgp.businessPartnerGroupNumber", "bgp._recordStart", "bgp._recordEnd")
      .join(servCatDF.alias("sc1"), ((col("core.receivedBK") == col("sc1.sourceBusinessKey")) &
           (col("core.lastChangedDateTime").between(col("sc1.sourceValidFromDatetime"), 
                                                          col("sc1.sourceValidToDatetime")))), "left").drop("sc1.sourceBusinessKey", "sc1.sourceValidFromDatetime", "sc1.sourceValidToDatetime", "sc1.resolutionCategoryFK") 
      .join(servCatDF.alias("sc2"), ((col("core.resolutionBK") == col("sc2.sourceBusinessKey")) &
           (col("core.lastChangedDateTime").between(col("sc2.sourceValidFromDatetime"), 
                                                          col("sc2.sourceValidToDatetime")))), "left").drop("sc2.sourceBusinessKey", "sc2.sourceValidFromDatetime", "sc2.sourceValidToDatetime","sc2.receivedCategoryFK") 
       .withColumn("BusinessKey", concat_ws("|", col("core.serviceRequestGUID").cast("string"), col("core.lastChangedDateTime").cast("string"),lit('')))
      ).select(col("BusinessKey").alias(f"{BK}")
                 ,col("sourceSystemCode")
                 ,col("serviceRequestId").alias("customerServiceRequestId")
                 ,col("serviceRequestGUID").alias("customerServiceRequestGUID")
                 ,when(col("receivedCategoryFK").isNull(), lit('-1')).otherwise(col("receivedCategoryFK")).alias("customerServiceRequestReceivedCategoryFK")
                 ,when(col("resolutionCategoryFK").isNull(), lit('-1')).otherwise(col("resolutionCategoryFK")).alias("customerServiceRequestResolutionCategoryFK")
                 ,lit("-1").alias("communicationChannelFK")  
                 ,lit(f"{dummyDimPartnerSK}").alias("contactPersonFK")
                 ,lit(f"{dummyDimPartnerSK}").alias("reportByPersonFK")
                 ,lit(f"{dummyDimPartnerSK}").alias("serviceTeamFK")
                 ,lit(f"{dummyDimPartnerSK}").alias("contractFK")
                 ,lit(f"{dummyDimPartnerSK}").alias("responsibleEmployeeFK")
                 ,lit(f"{dummyDimPartnerSK}").alias("reportToManagerFK")
                 ,lit(f"{dummyDimPartnerSK}").alias("organisationUnitFK")
                 ,when(col("processTypeFK").isNull(), lit('-1')).otherwise(col("processTypeFK")).alias("customerServiceProcessTypeFK")
                 ,col("propertyFK")
                 ,col("locationFK")
                 ,col("businessPartnerGroupFK")
                 ,when(col("statusFK").isNull(), lit('-1')).otherwise(col("statusFK")).alias("customerServiceRequestStatusFK")
                 ,lit(f"{dummyDimPartnerSK}").alias("salesEmployeeFK")
                 ,col("serviceRequestStartDateFK").alias("customerServiceRequestStartDateFK")
                 ,col("serviceRequestEndDateFK").alias("customerServiceRequestEndDateFK")
                 ,col("snapshotDateFK").alias("customerServiceRequestSnapshotDateFK")
                 ,col("customerServiceRequestTotalDurationSecondQuantity")
                 ,col("customerServiceRequestWorkDurationSecondQuantity")
                 ,lit("MAXIMO").alias("customerServiceRequestSourceName")
                 ,lit("MAXIMO").alias("customerServiceRequestSourceCode")
                 ,col("issueResponsibility").alias("customerServiceRequestIssueResponsibilityName")
                 ,col("issueResponsibilityCode").alias("customerServiceRequestIssueResponsibilityCode")
                 ,lit(None).alias("customerServiceRequestPostingDate")
                 ,col("requestStartDate").alias("customerServiceRequestStartTimestamp")
                 ,col("requestEndDate").alias("customerServiceRequestEndTimestamp")
                 ,lit(None).alias("customerServiceRequestInteractionsCount")
                 ,lit(None).alias("customerServiceRequestNotificationNumber")
                 ,col("transactionDescription").alias("customerServiceRequestDescription")
                 ,lit(None).alias("customerServiceRequestDirectionIdentifier")
                 ,lit(None).alias("customerServiceRequestDirectionCode")
                 ,col("maximoWorkOrderNumber").alias("customerServiceRequestMaximoWorkOrderNumber")
                 ,lit(None).alias("customerServiceRequestProjectId")
                 ,lit(None).alias("customerServiceRequestAgreementNumber")
                 ,lit(None).alias("customerServiceRequestRecommendedPriorityNumber")
                 ,lit(None).alias("customerServiceRequestImpactScoreNumber")
                 ,lit(None).alias("customerServiceRequestUrgencyNumber")
                 ,lit(None).alias("customerServiceRequestServiceLifeCycleUnitHourQuantity")
                 ,lit(None).alias("customerServiceRequestServiceLifeCycleUnitName")
                 ,lit(None).alias("customerServiceRequestActivityPriorityCode")
                 ,lit(None).alias("customerServiceRequestRespondByTimestamp")
                 ,lit(None).alias("customerServiceRequestRespondedTimestamp")
                 ,lit(None).alias("customerServiceRequestClosedTimestamp")
                 ,lit(None).alias("customerServiceRequestToDoByTimestamp")
                 ,lit('0').cast(DecimalType(15,2)).alias("customerServiceRequestInterimResponseDaysQuantity")
                 ,lit('0').cast(DecimalType(15,2)).alias("customerServiceRequestInterimResponseWorkingDaysQuantity")
                 ,lit("Yes").alias("customerServiceRequestMetInterimResponseIndicator")
                 ,col("requestStartDate").alias("customerServiceRequestCreatedTimestamp")
                 ,lit(None).alias("customerServiceRequestCreatedByUserId")
                 ,lit(None).alias("customerServiceRequestCreatedByUserName")
                 ,lit(None).alias("customerServiceRequestSnapshotTimestamp")
                 ,lit(None).alias("customerServiceRequestLastChangeTimestamp")
                 ,lit(None).alias("customerServiceRequestChangedByUserId")
                 ,lit(None).alias("customerServiceRequestChangedByUserName")
                )
)

#finalMAXDF.display()    
finaldf = finalCRMDF.unionByName(finalMAXDF) 

# COMMAND ----------

def Transform():
    global df    
    df = finaldf
    # ------------- TRANSFORMS ------------- #
    _.Transforms = ['*']
    df = df.selectExpr(_.Transforms)
    #display(df)
    #CleanSelf()
    Save(df)
    busDF.unpersist()
Transform()
