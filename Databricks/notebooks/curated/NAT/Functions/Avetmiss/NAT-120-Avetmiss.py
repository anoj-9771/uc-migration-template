# Databricks notebook source
def GetNAT120Avetmiss(dfAvetmissCourseEnrolment, dfAvetmissLocation, dfAvetmissStudent, dfAvetmissUnit, dfAvetmissUnitEnrolment, dfFundingSource, params):
  ###########################################################################################################################
  # Function: GetNAT120Avetmiss
  # Parameters: 
  # dfAvetmissCoursEnrolment = Avetmiss Course Enrolment
  # dfAvetmissLocation = Avetmiss Location
  # dfAvetmissStudent = Avetmiss Student
  # dfAvetmissUnit = Avetmiss Unit
  # dfAvetmissUnitEnrolmentByPeriod = Avetmiss Unit Enrolment By Period
  # params = Master Notebook Parameters
  # Returns:
  #  Dataframe for all NAT 120 Avetmiss
  #############################################################################################################################
  NAT_Dates = spark.read.json(sc.parallelize([params]))
  CollectionStart = params["CollectionStartAvt"]
  CollectionEnd = params["CollectionEndAvt"]
  CutOffDate = params["CutOffDateAvt"]
  ReportingYear = params["ReportingYearAvt"]
  #PRADA-1610
  dfAvetmissUnitEnrolment = dfAvetmissUnitEnrolment.where(expr("UnitLevelStatusActive = 'Y' AND UnitLevelStatusType = 'OUTCOME' and not (RecordSource='Academic Record' and UnitAwardDate is null)"))
  #Alias for UnitEnrolmentByPeriod
  dfUnitEnrolmentV=dfAvetmissUnitEnrolment.select(*(col(x).alias("UEP_"+ x) for x in dfAvetmissUnitEnrolment.columns))
  
  #Alias for CourseEnrolment
  dfCourseEnrolmentV=dfAvetmissCourseEnrolment.select(*(col(x).alias("CE_"+ x) for x in dfAvetmissCourseEnrolment.columns)).filter(col("ReportingYear")==lit(ReportingYear))
  
  #Alias for FundingSource reference
  dfVPFundingSource=dfFundingSource.select(*(col(x).alias("FS_"+ x) for x in dfFundingSource.columns))
  
  #############################################################
  #######Start Main Table Join - AvetmissUnitEnrolment#########
  #############################################################
  df = dfUnitEnrolmentV.filter(col("ReportingYear")==lit(ReportingYear))
  df = df.filter(~col("UEP_AvetmissClientID").like('165%'))
  df = df.filter(col("UEP_UnitEnrolmentEndDate")>=lit(CollectionStart))
  df = df.filter(col("UEP_UnitEnrolmentStartDate")<=lit(CollectionEnd))
  df = df.filter(col("UEP_CourseEnrolmentStartDate")<=lit(CollectionEnd))
  df = df.filter(col("UEP_CourseEnrolmentEndDate")>=lit(CollectionStart))
  df = df.where( (~col("UEP_CourseProgressStatus").isin(['L','N'])) | (col("UEP_CourseProgressStatus").isNull()) )
  df = df.filter( (~col("UEP_CourseProgressCode").isin(['3.3WN', '3.4NS'])) | (col("UEP_CourseProgressCode").isNull()) )
  df = df.filter( (~col("UEP_CourseProgressReason").isin(['CNPORT','WDCC','WDEC','WDSBC'])) | (col("UEP_CourseProgressReason").isNull()) )
  df = df.where(expr(f"NOT( UEP_CourseProgressCode IN ('3.1 WD', '4.2 CLOSE', '2.1 WDTFR') AND to_date(UEP_CourseProgressDate, 'yyyyMMdd') < '{CollectionStart}' and to_date(UEP_UnitAwardDate, 'yyyyMMdd') < '{CollectionStart}'  ) "))
  
  #CourseEnrolment
  df=df.join(dfCourseEnrolmentV, (df.UEP_CourseOfferingEnrolmentId==dfCourseEnrolmentV.CE_CourseOfferingEnrolmentId) & (df.UEP_ReportingYear==dfCourseEnrolmentV.CE_ReportingYear) ,how="left")
  df=df.join(dfVPFundingSource, df.UEP_UnitFundingSourceCode==dfVPFundingSource.FS_FundingSourceCode ,how="left")

  
  #Outcome ID for Quarterly Avetmiss
  df=df.withColumn("OutcomeID", expr("CASE \
                                     WHEN UEP_Grade IS NOT NULL THEN UEP_OutcomeNational \
                                     WHEN UEP_Grade IS NULL AND UEP_UnitEnrolmentStartDate < CAST(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'Australia/Sydney') AS DATE) THEN '70' \
                                     WHEN UEP_Grade IS NULL AND UEP_UnitEnrolmentStartDate >= CAST(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'Australia/Sydney') AS DATE) THEN '85' \
                                     WHEN UEP_Grade IS NOT NULL THEN UEP_OutcomeNational \
                                     ELSE '' END"))

  df=df.withColumn("ACTIVITY_START_DT",\
                   when(col("UEP_RecordSource")=="Unit Offering Enrolment",to_date(col("UEP_UnitEnrolmentStartDate"),"yyyyMMdd"))
                   .when(col("UEP_RecordSource")=="Academic Record",expr(f"case \
                                                                           when UEP_RecordSource='Academic Record' and UEP_UnitAwardDate<'{CollectionStart}' then UEP_AttainmentCreatedDate \
                                                                           else UEP_UnitAwardDate end"))
                  )
  df=df.withColumn("ACTIVITY_END_DT",\
                   when(col("UEP_RecordSource")=="Unit Offering Enrolment",to_date(col("UEP_UnitEnrolmentEndDate"),"yyyyMMdd"))
                   .when(col("UEP_RecordSource")=="Academic Record",expr(f"case \
                                                                           when UEP_RecordSource='Academic Record' and UEP_UnitAwardDate<'{CollectionStart}' then UEP_AttainmentCreatedDate \
                                                                           else UEP_UnitAwardDate end"))
                  )
  df=df.withColumn("DELIVERY_MODE_ID",\
                  when(col("OutcomeID").isin(['51', '52', '60']),"NNN")\
                  .when(col("UEP_OriginalDeliveryMode")== "CLAS" , 'YNN')\
                  .when(col("UEP_OriginalDeliveryMode")== 'SELF' , 'YYN')\
                  .when(col("UEP_OriginalDeliveryMode")== 'ELEC' , 'NYN')\
                  .when(col("UEP_OriginalDeliveryMode")== 'BLND' , 'YYN')\
                  .when(col("UEP_OriginalDeliveryMode")== 'ONJB' , 'NNY')\
                  .when(col("UEP_OriginalDeliveryMode")== 'ONJD' , 'NYY')\
                  .when(col("UEP_OriginalDeliveryMode")== 'SIMU' , 'NYY')\
                  .when(col("UEP_OriginalDeliveryMode")== 'DIST' , 'NYN')\
                  .when(col("UEP_OriginalDeliveryMode")== 'CLON' , 'YYN')\
                  .when(col("UEP_OriginalDeliveryMode")== 'COMB' , 'YYY')\
                  .when(col("UEP_OriginalDeliveryMode")== 'ONJO' , 'NYY')\
                  .when(col("UEP_OriginalDeliveryMode")== 'ONLN' , 'NYN')\
                  .otherwise('NNN')
                  )
  df = df.withColumn("SCHEDULED_HOURS",col("UEP_UnitScheduledHours").cast(DecimalType(4, 0)))
        
  df = df.withColumn("COMMENCING_PROG_ID",\
                     when(col("UEP_CourseEnrolmentStartDate")<lit(CollectionStart),'4')\
                     .when(col("UEP_CourseEnrolmentStartDate")>=lit(CollectionStart),'3')\
                     .otherwise('8')
                     )
  df = df.withColumn("CLIENT_ID_APPRENTICESHIP", \
                     when(instr(col("CE_TrainingContractID"),"/")<1,col("CE_TrainingContractID"))\
                     .otherwise(col("CE_TrainingContractID").substr(lit(1),instr(col("CE_TrainingContractID"),"/")-1)))
  df = df.withColumn("STUDY_REASON",coalesce(col("CE_StudyReason"),lit('@@')))
  df = df.withColumn("OUTCOME_ID_TRAINING_ORG",\
                     when(col("UEP_CourseProgressReason").isin(['SSDISC', 'WDAC', 'SSDiscCOV']),'TNC')\
                     .when(col("UEP_CourseProgressReason").isin(['SSDEF', 'SSDefCOV']),'D')\
                     .otherwise(' ')
                    )
  df = df.withColumn("PURCHASING_CONTRACT_ID",rpad(lit(' '),12,' ')) #Purchasing_contract_id missing
  df = df.withColumn("PURCHASING_CONTRACT_SCHED",rpad(lit(' '),3,' ')) #hardcode ' '
  df = df.withColumn("HOURS_ATTENDED",rpad(lit(' '),4,' ')) #hardcode ' '
  df = df.withColumn("ATT_UPDATED_DATE",to_date(col("UEP_AttainmentUpdatedDate"), "yyyyMMdd")) #UEP_UpdatedDate
  df = df.withColumn("ATT_CREATED_DATE",to_date(col("UEP_AttainmentCreatedDate"), "yyyyMMdd")) #UEP_CreatedDate
  df = df.withColumn("AttainmentCode",col("UEP_AttainmentCode").cast(DecimalType(15, 0)))
  #PRADA-1733

  df=df.withColumn("Output", \
                  concat( \
                          rpad(coalesce(col("UEP_UnitDeliveryLocationCode"),lit(' ')),10," ") ,\
                          rpad(col("UEP_AvetmissClientID"),10," "),
                          rpad(col("UEP_AvetmissUnitCode"),12," "),\
                          rpad(col("UEP_AvetmissCourseCode"),15," "),\
                          lpad(date_format(col("ACTIVITY_START_DT"), "ddMMyyyy"),8,'0'), \
                          lpad(date_format(col("ACTIVITY_END_DT"), "ddMMyyyy"),8,'0'), \
                          rpad(coalesce(col("DELIVERY_MODE_ID"),lit(' ')),3," "), \
                          rpad(coalesce(col("OutcomeID"),lit(' ')),2," "), \
                          lpad(coalesce(col("SCHEDULED_HOURS"),lit(' ')),4,'0'), \
                          rpad(coalesce(col("FS_FundingSourceNationalID"),lit(' ')),2," "), \
                          rpad(coalesce(col("COMMENCING_PROG_ID"),lit(' ')),1," "), \
                          rpad(coalesce(col("CE_TrainingContractID"),lit(' ')),10," "), \
                          rpad(coalesce(col("CLIENT_ID_APPRENTICESHIP"),lit(' ')),10," "), \
                          lpad(col("STUDY_REASON"),2,"0"), \
                          rpad(coalesce(col("UEP_VetInSchoolFlag"),lit(' ')),1," "), \
                          rpad(coalesce(col("FS_SpecificFundingCodeID"),lit(' ')),10," "), \
                          rpad(coalesce(col("UEP_UnitOutcome"),lit(' ')),3," "), \
                          rpad(coalesce(col("UEP_UnitOfferingEnrolmentId"),lit(' ')),8," "), \
                          rpad(coalesce(col("UEP_UnitFundingSourceCode"),lit(' ')),12," "), \
                          rpad(coalesce(col("UEP_UnitRecognitionType"),lit(' ')),3," "), \
                          rpad(coalesce(col("UEP_CourseOfferingEnrolmentId"),lit(' ')),14," "), \
                          rpad(coalesce(col("UEP_StudentId").cast(DecimalType(10,0)),lit(' ')),10," "), \
                          lpad(coalesce(date_format(col("UEP_UnitAwardDate"), "ddMMyyyy"),lit(' ')),8," "), \
                          rpad(coalesce(col("CE_CommitmentIdentifier"),lit(' ')),12," "), \
                          rpad(coalesce(col("PURCHASING_CONTRACT_ID"),lit(' ')),12," "), \
                          rpad(coalesce(col("PURCHASING_CONTRACT_SCHED"),lit(' ')),3," "), \
                          rpad(coalesce(col("HOURS_ATTENDED"),lit(' ')),4," "), \
                          rpad(coalesce(col("CE_CourseProgressStatus"),lit(' ')), 2," "), \
                          rpad(coalesce(col("CE_CourseProgressCode"),lit(' ')),12," "), \
                          rpad(coalesce(col("CE_CourseFundingSourceCode"),lit(' ')),12," "), \
                          rpad(coalesce(col("CE_CourseProgressReason"),lit(' ')),12," "), \
                          rpad(coalesce(col("UEP_UnitLevelStatus"),lit(' ')),12," "), \
                          rpad(coalesce(date_format(col("CE_CourseAwardDate"), "ddMMyyyy"),lit(' ')),10," "), \
                          rpad(coalesce(col("UEP_OriginalDeliveryMode"),lit(' ')),15," "), \
                          rpad(coalesce(col("UEP_CourseOfferingEnrolmentId"),lit(' ')),12," "), \
                          rpad(coalesce(date_format(col("ATT_UPDATED_DATE"), "ddMMyyyy"),lit(' ')),10," "), \
                          rpad(coalesce(col("UEP_UnitProgressCode"),lit(' ')),12," "), \
                          rpad(coalesce(col("UEP_UnitProgressStatus"),lit(' ')),2," "), \
                          rpad(coalesce(col("UEP_UnitProgressReason"),lit(' ')),12," "), \
                          rpad(coalesce(date_format(col("UEP_UnitProgressDate"), "ddMMyyyy"),lit(' ')),10," "), \
                          rpad(coalesce(date_format(col("ATT_CREATED_DATE"), "ddMMyyyy"),lit(' ')),10," "), \
                          rpad(coalesce(col("AttainmentCode"),lit(' ')),15," "), \
                          rpad(coalesce(date_format(col("UEP_CourseEnrolmentStartDate"), "ddMMyyyy"),lit(' ')),10," "), \
                          rpad(coalesce(date_format(col("UEP_CourseEnrolmentEndDate"), "ddMMyyyy"),lit(' ')),10," "), \
                          rpad(coalesce(col("UEP_UnitOfferingId").cast(DecimalType(15, 0)),lit(' ')),15," "), \
                          rpad(coalesce(col("CE_PredominantDeliveryMode"),lit(' ')),1," ")
                        ))
  #PRADA-1724
  df = df.where("COALESCE(UEP_CourseProgressCode, '') != ''") 
  #PRADA-1717, PRADA-1738
  df = df.withColumn("CY_Rank", expr("ROW_NUMBER() OVER (PARTITION BY UEP_UnitEnrolmentId, UEP_ReportingYear ORDER BY UEP_ReportingDate DESC, CE_ReportingDate DESC)")).where("CY_Rank=1")
  #PRADA-1691, PRADA-1728
  df = df.where("COALESCE(UEP_UnitOutcome, '') NOT IN ('NS')") 
  df = df.selectExpr(
    "UEP_UnitDeliveryLocationCode AS TRAINING_ORG_DEL_LOC_ID"
    ,"UEP_AvetmissClientID AS CLIENT_ID"
    ,"UEP_AvetmissUnitCode AS SUBJECT_ID"
    ,"UEP_AvetmissCourseCode AS PROGRAM_ID"
    ,"ACTIVITY_START_DT"
    ,"ACTIVITY_END_DT"
    ,"DELIVERY_MODE_ID"
    ,"OutcomeID AS OUTCOME_ID_NATIONAL"
    ,"SCHEDULED_HOURS"
    ,"FS_FundingSourceNationalID as FUNDING_SOURCE_NATION"
    ,"COMMENCING_PROG_ID"
    ,"CE_TrainingContractID AS TRAINING_CONT_ID"
    ,"CLIENT_ID_APPRENTICESHIP"
    ,"STUDY_REASON"
    ,"UEP_VetInSchoolFlag AS VET_IN_SCHOOL_FLAG"
    ,"FS_SpecificFundingCodeID AS SPEC_FUND_ID"
    ,"UEP_UnitOutcome OUTCOME_ID_TRAINING_ORG"
    ,"UEP_UnitOfferingEnrolmentId AS UNIT_PU_ID"
    ,"UEP_UnitFundingSourceCode AS FUNDING_SOURCE"
    ,"UEP_UnitRecognitionType AS RECOGNITION"
    ,"UEP_CourseOfferingEnrolmentId AS COURSE_PU_ID"
    ,"UEP_StudentId AS PERSON_CODE"
    ,"UEP_UnitAwardDate AS DATE_AWARDED"
    ,"CE_CommitmentIdentifier AS COMMITMENT_ID"
    ,"PURCHASING_CONTRACT_ID"
    ,"PURCHASING_CONTRACT_SCHED"
    ,"HOURS_ATTENDED"
    ,"UEP_CourseProgressStatus AS COURSE_PROGRESS_STATUS"
    ,"UEP_CourseProgressCode AS COURSE_PROGRESS_CODE"
    ,"CE_CourseFundingSourceCode AS COURSE_FUNDING_SOURCE"
    ,"CE_CourseProgressReason AS PROGRESS_REASON"
    ,"UEP_UnitLevelStatus AS AWARD_STATUS"
    ,"CE_CourseAwardDate AS COURSE_DATE_AWARDED"
    ,"UEP_OriginalDeliveryMode AS TAFE_DELIVERY_MODE /*PRADA-1609*/"
    ,"UEP_CourseOfferingId AS COURSE_UIO_ID"
    ,"ATT_UPDATED_DATE"
    ,"UEP_UnitProgressCode AS UNIT_PROGRESS_CODE"
    ,"UEP_UnitProgressStatus AS UNIT_PROGRESS_STATUS"
    ,"UEP_UnitProgressReason AS UNIT_PROGRESS_REASON"
    ,"UEP_UnitProgressDate AS UNIT_PROGRESS_DATE"
    ,"ATT_CREATED_DATE"
    ,"AttainmentCode as ATTAINMENT_CODE"
    ,"UEP_CourseEnrolmentStartDate AS COURSE_START_DATE"
    ,"UEP_CourseEnrolmentEndDate AS COURSE_END_DATE"
    ,"UEP_UnitOfferingId AS UNIT_UIO_ID"
    ,"CE_PredominantDeliveryMode AS PREDOMINANT_DEL_MODE"
    ,"UEP_AdjustedGrade AS ADJUSTED_GRADE"
    ,"CE_DateConferred AS PROCESSED_DATE"
    ,"Output"
  ).dropDuplicates()

  
  return df

# COMMAND ----------


