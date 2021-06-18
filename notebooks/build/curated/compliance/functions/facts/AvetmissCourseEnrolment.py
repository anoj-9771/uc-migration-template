# Databricks notebook source
import pyspark.sql.functions as func

# COMMAND ----------

def _CourseEnrolmentRollup(unitEnrolmentDf):
  ######################################################################################
  #accepts unit enrolments data sets
  #dedup unit enrolments by ReportingYear, AvetmissClientId, AvetmissCourseCode, DeliveryLocationCode, ValidFlag
  #calculate the units aggregations
  #roll up to course enrolments with unique course enrolments by ReportingYear, AvetmissClientId, AvetmissCourseCode, DeliveryLocationCode, ValidFlag
  #returns the dataframe with units enrolments rollup to course enrolment with the units aggregations
  ######################################################################################
  
  dfUnitEnrolment = unitEnrolmentDf.filter(col("IncludeFlag")==1).withColumnRenamed("UnitDeliveryLocationCode", "DeliveryLocationCode")
  
  dfCourseEnrolmentRollupValid = dfUnitEnrolment.filter(col("ValidFlag") == 1).select('ReportingYear', 'AvetmissClientId', 'AvetmissCourseCode', 'DeliveryLocationCode', 'ValidFlag', 'CourseOfferingEnrolmentId').distinct()
  dfCourseEnrolmentRollupInvalid = dfUnitEnrolment.filter(col("ValidFlag") == 0).select('ReportingYear', 'AvetmissClientId', 'AvetmissCourseCode', 'DeliveryLocationCode', 'ValidFlag', 'CourseOfferingEnrolmentId').distinct()

  dfCourseEnrolmentRollupInvalidNotValid = dfCourseEnrolmentRollupInvalid.join(dfCourseEnrolmentRollupValid, on=['ReportingYear', 'AvetmissClientId', 'AvetmissCourseCode', 'DeliveryLocationCode'], how='left_anti')

  dfCourseEnrolmentRollupCombined = dfCourseEnrolmentRollupValid.union(dfCourseEnrolmentRollupInvalidNotValid)

  dfUnitEnrolmentValidOverInvalid = dfUnitEnrolment.join(dfCourseEnrolmentRollupCombined, on=['ReportingYear', 'AvetmissClientId', 'AvetmissCourseCode', 'DeliveryLocationCode', 'ValidFlag', 'CourseOfferingEnrolmentId'])
  
  dfUnitEnrolmentRollupToCourseEnrolment = dfUnitEnrolmentValidOverInvalid.withColumn("rn", expr("row_number() over(partition by ReportingYear, AvetmissClientId, AvetmissCourseCode, DeliveryLocationCode, ValidFlag, CourseOfferingEnrolmentId order by CourseFundingSourceCode desc)")).where(col("rn") == 1).drop("rn")

  dfUnitEnrolmentAgg = dfUnitEnrolmentValidOverInvalid.groupBy("ReportingYear", "AvetmissClientId",  "AvetmissCourseCode", "DeliveryLocationCode", "ValidFlag", "CourseOfferingEnrolmentId") \
    .agg(sum(expr("CASE WHEN OutcomeNational IN ('60','61') THEN 0 ELSE UnitScheduledHours END /*EDWDM-269*/")).alias("TotalHours") \
        ,sum(expr("CASE WHEN OutcomeNational IN ('51','52') THEN UnitScheduledHours ELSE 0 END /*EDWDM-270*/")).alias("RPLHours") \
        ,sum(expr("CASE WHEN OutcomeNational IN ('60','61','51','52') THEN 0 ELSE UnitScheduledHours END /*EDWDM-271*/")).alias("TrainingHours") \
        ,sum(expr("CASE WHEN OutcomeNational IN ('70','71') THEN UnitScheduledHours ELSE 0 END /*EDWDM-272*/")).alias("ContinuingHours") \
        ,sum(expr("CASE WHEN OutcomeNational IN ('20','30','40','51','52','81','82') THEN 1 ELSE 0 END /*EDWDM-273*/")).alias("ValidUnitCount") \
        ,sum(expr("1 /*EDWDM-274*/")).alias("UnitCount") \
        ,sum(expr("CASE WHEN OutcomeNational IN ('20','30','40','51','52','81','82','70') THEN 1 ELSE 0 END /*EDWDM-275*/")).alias("FinalGradeUnitCount") \
        ,sum(expr("CASE WHEN OutcomeNational IN ('20') THEN 1 ELSE 0 END /*EDWDM-276*/")).alias("PassGradeUnitCount") \
        ,sum(expr("CASE WHEN OutcomeNational IN ('40') THEN 1 ELSE 0 END /*EDWDM-277*/")).alias("WithdrawnUnitCount") \
        ,sum(expr("CASE WHEN OutcomeNational IN ('30') THEN 1 ELSE 0 END /*EDWDM-278*/")).alias("FailGradeUnitCount") \
        ,sum(expr("CASE WHEN OutcomeNational IN ('51','52') THEN 1 ELSE 0 END /*EDWDM-279*/")).alias("RPLUnitCount") \
        ,sum(expr("CASE WHEN OutcomeNational IN ('60','61') THEN 1 ELSE 0 END /*EDWDM-280*/")).alias("CreditTransferUnitCount") \
        ,sum(expr("CASE WHEN OutcomeNational IN ('81','82') THEN 1 ELSE 0 END /*EDWDM-281*/")).alias("NonAssessableUnitCount") \
        ,sum(expr("CASE WHEN OutcomeNational IN ('90','0','') THEN 1 ELSE 0 END /*EDWDM-282*/")).alias("MissingGradeUnitCount") \
        ,sum(expr("CASE WHEN OutcomeNational IN ('70','71') THEN 1 ELSE 0 END /*EDWDM-283*/")).alias("ContinuingUnitCount") \
        ,sum(expr("CASE WHEN OutcomeNational IN ('51') THEN 1 ELSE 0 END /*EDWDM-284*/")).alias("RPLGrantedUnitCount") \
        ,sum(expr("CASE WHEN OutcomeNational IN ('52') THEN 1 ELSE 0 END /*EDWDM-285*/")).alias("RPLNotGrantedUnitCount") \
        ,sum(expr("CASE WHEN OutcomeNational IN ('60') THEN 1 ELSE 0 END /*EDWDM-286*/")).alias("CreditTransferNationalRecognitionUnitCount") \
        ,sum(expr("CASE WHEN OutcomeNational IN ('61') THEN 1 ELSE 0 END /*EDWDM-287*/")).alias("SupersededUnitCount") \
        ,sum(expr("CASE WHEN OutcomeNational IN ('81') THEN 1 ELSE 0 END /*EDWDM-288*/")).alias("NonAssessableSatisfactorilyCompletedUnitCount") \
        ,sum(expr("CASE WHEN OutcomeNational IN ('82') THEN 1 ELSE 0 END /*EDWDM-289*/")).alias("NonAssessableNotSatisfactorilyCompletedUnitCount") \
        ,sum(expr("CASE WHEN OutcomeNational IN ('20','30','40','51','52','81','82','70') THEN 1 ELSE 0 END /*EDWDM-290*/")).alias("PayableUnitCount") \
        ,sum(expr("CASE WHEN Semester1Hours> 0 AND Semester2Hours= 0 THEN 1 ELSE 0 END /*EDWDM-291*/")).alias("Semester1UnitCount") \
        ,sum(expr("CASE WHEN Semester1Hours= 0 AND Semester2Hours> 0 THEN 1 ELSE 0 END /*EDWDM-292*/")).alias("Semester2UnitCount") \
        ,sum(expr("CASE WHEN Semester1Hours> 0 AND Semester2Hours> 0 THEN 1 ELSE 0 END /*EDWDM-293*/")).alias("Semester3UnitCount") \
        ,sum(expr("Semester1Hours /*EDWDM-294*/")).alias("TotalSemester1Hours") \
        ,sum(expr("Semester2Hours /*EDWDM-295*/")).alias("TotalSemester2Hours") \
        ,sum(expr("CASE WHEN DeliveryMode = 'ADVD' THEN 1 ELSE 0 END /*EDWDM-296*/")).alias("ExemptedDeliveryUnitCount") \
        ,sum(expr("CASE WHEN DeliveryMode = 'CLAS' THEN 1 ELSE 0 END /*EDWDM-297*/")).alias("InClassDeliveryUnitCount") \
        ,sum(expr("CASE WHEN DeliveryMode = 'DIST' THEN 1 ELSE 0 END /*EDWDM-298*/")).alias("DistanceDeliveryUnitCount") \
        ,sum(expr("CASE WHEN DeliveryMode = 'ELEC' THEN 1 ELSE 0 END /*EDWDM-299*/")).alias("ElectronicDeliveryUnitCount") \
        ,sum(expr("CASE WHEN DeliveryMode = 'BLND' THEN 1 ELSE 0 END /*EDWDM-300*/")).alias("BlendedDeliveryUnitCount") \
        ,sum(expr("CASE WHEN DeliveryMode = 'SELF' THEN 1 ELSE 0 END /*EDWDM-301*/")).alias("SelfDeliveryUnitCount") \
        ,sum(expr("CASE WHEN DeliveryMode = 'ONJB' THEN 1 ELSE 0 END /*EDWDM-302*/")).alias("OnTheJobTrainingDeliveryUnitCount") \
        ,sum(expr("CASE WHEN DeliveryMode = 'ONJD' THEN 1 ELSE 0 END /*EDWDM-303*/")).alias("OnTheJobDistanceDeliveryUnitCount") \
        ,sum(expr("CASE WHEN DeliveryMode = 'SIMU' THEN 1 ELSE 0 END /*EDWDM-304*/")).alias("SimulatedDeliveryUnitCount") \
        ,sum(expr("CASE WHEN DeliveryMode = '' THEN 1 ELSE 0 END /*EDWDM-305*/")).alias("MissingDeliveryModeUnitCount") \
        ,sum(expr("CASE WHEN COALESCE(DeliveryMode, '') NOT IN ('ADVD','CLAS','SELF','ELEC','BLND','DIST','ONJB','ONJD','SIMU','') THEN 1 ELSE 0 END /*EDWDM-306*/")).alias("OtherDeliveryUnitCount") \
        ,sum(expr("CASE WHEN COALESCE(OutcomeNational, '') NOT IN ('51','52','60','61') THEN 1 ELSE 0 END /*EDWDM-307*/")).alias("DeliveredUnitCount") \
      )

  dfCourseEnrolmentRollup = dfUnitEnrolmentRollupToCourseEnrolment.join(dfUnitEnrolmentAgg, ["ReportingYear", "AvetmissClientId", "AvetmissCourseCode", "DeliveryLocationCode", "ValidFlag", "CourseOfferingEnrolmentId"])

  #EDWDM-312
  dfCourseEnrolmentRollup = dfCourseEnrolmentRollup.withColumn("ShellEnrolmentFlag", lit(0))

  return dfCourseEnrolmentRollup

# COMMAND ----------

def _CourseEnrolmentAll(feesListDf, instalmentPlansDf, feesListTempDf, peopleUnitsDf, peopleDf, attainmentsDf, unitInstanceOccurrencesDf, unitInstancesDf, peopleUnitsSpecialDf, configurableStatusesDf, locationDf, avetmissCourseDf, organisationUnitsDf, awardPrintedDetailsDf, webConfigDf, feeTypesDf, whoToPayDf, feeValuesDf, pulDf, RefreshLinkSP):
  ######################################################################################
  #Objective - collect the course enrolments from OneEBS tables
  #accepts oneebs tables data frames used to get the course enrolments
  #calculate the required fees related columns
  #determine the required columns for transitioned from course enrolments
  #join the related dataframes and get the required columns for course enrolments
  #dedup the course enrolments by CourseOfferingEnrolmentId, AvetmissCourseCode
  #returns the dataframe with course enrolments (all)
  ######################################################################################
  
  i=webConfigDf.where(expr("parameter = 'INST_TYPE' and parameter_value like 'A%'")).count()

  #EBS_FEE_DATA
  fl4Df = GeneralAliasDataFrameColumns(feesListDf, "fl4_")
  fl5Df = GeneralAliasDataFrameColumns(feesListDf, "fl5_")

  #firstFeeRecord and finalFeeRecord
  if RefreshLinkSP:
    LogEtl("Collecting linked fee records recursively for course enrolments - starts...")
    AzSqlExecTSQL("exec compliance.PopulateAvetmissFeeRecord")
    LogEtl("Collecting linked fee records recursively for course enrolments - ends...")
  
  feeRecordMappingDf = AzSqlGetData("compliance.AvetmissFeeRecord")
  
  #fl3Df
  fl1Df = feesListDf.where(expr("fes_associated_record IS NOT NULL")) \
    .selectExpr(
      "fee_record_code" \
      ,"CASE \
          WHEN status = 'F' THEN fee_record_code \
          WHEN status = 'R' THEN 2 \
          ELSE 1 \
        END as weight" \
      ,"status" \
      ,"fes_person_code" \
      ,"fes_record_type" \
      ,"fes_associated_record" \
      ,"amount" \
      ,"id") \
    .groupBy("fes_associated_record", "fes_record_type", "fes_person_code") \
    .agg(
      max(col("id")).alias("id")
      ,max(col("fee_record_code")).alias("fee_record_code1")
      ,max(col("weight")).alias("max_weight")
      ,max(col("fee_record_code")).alias("max_fee_record_code")
      ,sum(col("amount")).alias("adjustedAmount")
    ) \
    .selectExpr(
      "id"
      ,"fee_record_code1"
      ,"CASE WHEN max_weight > 2 THEN max_weight ELSE max_fee_record_code END fee_record_code"
      ,"adjustedAmount"
      ,"fes_person_code"
    )

  fl2Df = feesListDf.where(expr("fes_associated_record IS NULL")) \
    .selectExpr("id", "fee_record_code as fee_record_code1", "fee_record_code", "amount", "fes_person_code")

  fl3Df = GeneralAliasDataFrameColumns(fl1Df.union(fl2Df), "fl3_")

  ftDf = GeneralAliasDataFrameColumns(feeTypesDf, "ft_")

  ipDf = instalmentPlansDf.where(expr("matched_fee_Record_code > 0"))
  fltDf = GeneralAliasDataFrameColumns(feesListTempDf.where(expr("epayment_status = 'R' AND status = 'F'")), "flt_")

  fee_ipDf = GeneralAliasDataFrameColumns(ipDf \
    .join(fltDf, (fltDf.flt_INSTALMENT_ID == ipDf.ID) & (fltDf.flt_FES_PERSON_CODE == ipDf.PERSON_CODE), how="left") \
    .groupBy("person_code", "matched_fee_record_code") \
    .agg(sum(col("AMOUNT")).alias("amount")
        ,sum(expr(f"case when {i} = 1 then 0 else COALESCE(flt_amount, 0) end")).alias("pend_amount")) \
    .selectExpr(
      "amount" 
      ,"pend_amount" \
      ,"person_code" \
      ,"matched_fee_record_code"), "fee_ip_"
    )

  rec_pendDf = GeneralAliasDataFrameColumns(feesListTempDf.where(expr("epayment_status = 'R' AND matched_fee_record_code > 0")) \
    .groupBy("fes_person_code", "matched_fee_record_code") \
    .agg(sum(col("amount")).alias("amount")), "rec_pend_")

  #joins
  df_fee_data = fl4Df \
    .join(fl3Df, (fl3Df.fl3_fee_record_code == fl4Df.fl4_FEE_RECORD_CODE) & (fl3Df.fl3_fes_person_code == fl4Df.fl4_FES_PERSON_CODE), how="inner") \
    .join(ftDf, col("fl4_FES_FEE_TYPE") == col("ft_fee_type_code"), how="inner") \
    .join(GeneralAliasDataFrameColumns(whoToPayDf, "wtp_"), col("fl4_fes_who_to_pay") == col("wtp_who_to_pay"), how="inner") \
    .join(GeneralAliasDataFrameColumns(peopleDf, "p_"), col("fl4_fes_person_code") == col("p_person_code"), how="inner") \
    .join(GeneralAliasDataFrameColumns(feeValuesDf, "fv_"), col("fl4_fee_value_number") == col("fv_fee_value_number"), how="inner") \
    .join(fee_ipDf, (fee_ipDf.fee_ip_matched_fee_record_code == fl4Df.fl4_FEE_RECORD_CODE) & (fee_ipDf.fee_ip_person_code == fl4Df.fl4_FES_PERSON_CODE), how="left") \
    .join(rec_pendDf, (rec_pendDf.rec_pend_matched_fee_record_code == fl4Df.fl4_FEE_RECORD_CODE), how="left") \
    .join(GeneralAliasDataFrameColumns(feeRecordMappingDf, "frm_"), col("frm_FeeRecordCode") == col("fl4_FEE_RECORD_CODE"), how="left") \
    .selectExpr(
      "fl4_fes_person_code as fes_person_code"
      ,"fl4_fee_record_code as fee_record_code"
      ,"fl4_matched_fee_record_code as matched_fee_record_code"
      ,"frm_FirstFeeRecord as firstFeeRecord"
      ,"frm_FinalFeeRecord as finalFeeRecord"
      ,"CASE WHEN fl4_FES_ASSOCIATED_RECORD IS NULL THEN 0 ELSE fl3_adjustedAmount END AS adjusted_amount" 
      ,"CASE WHEN fl4_FES_ASSOCIATED_RECORD IS NULL AND fl4_FES_RECORD_TYPE = 'F' THEN fl4_amount ELSE 0 END AS original_amount"
      ,"CASE \
          WHEN fl4_INSTALMENT_ID IS NULL OR (fl4_INSTALMENT_ID > 0 AND fl4_status != 'F') THEN COALESCE(fee_ip_amount - fee_ip_pend_amount, 0) \
          ELSE (-1 * fl4_amount) \
        END AS instalment_amount"
      ,"CASE WHEN fl4_fes_record_type = 'R' AND fl4_matched_fee_record_code > 0 AND fl4_status = 'F' THEN fl4_amount ELSE 0 END receipt_amount" \
      ,"CASE WHEN rec_pend_amount IS NOT NULL THEN rec_pend_amount ELSE 0 END pending_amount" \
    )

  #EBS_FEE_SUMMARY  
  fl2Df = GeneralAliasDataFrameColumns(feesListDf, "fl2_")
  f1Df = df_fee_data.where(expr("COALESCE(matched_fee_record_code, 0) > (CASE WHEN finalFeeRecord IS NOT NULL THEN -1 ELSE 0 END)")) \
    .join(fl2Df.selectExpr("fl2_fee_record_code originalfeerecordcode", "fl2_fes_person_code originalfespersoncode"), (col("firstfeerecord") == col("originalfeerecordcode")) & (col("fes_person_code") == col("originalfespersoncode")), how="left") \
    .withColumn("finalfeerecordgroupby", expr("CASE WHEN finalfeerecord IS NULL THEN fee_record_code ELSE 0 END")) \
    .groupBy("firstfeerecord", "fes_person_code", "finalfeerecordgroupby") \
    .agg(
      max(col("finalFeeRecord")).alias("fee_record_code"),
      sum(col("adjusted_amount")).alias("adjusted_amount"),
      sum(col("original_amount")).alias("original_amount"),  
      sum(col("pending_amount")).alias("pending_amount"),
      sum(col("instalment_amount")).alias("instalment_amount"),
      sum(col("receipt_amount")).alias("receipt_amount")
    ) \
    .selectExpr(
      "fes_person_code",
      "fee_record_code",
      "adjusted_amount as adjustments",
      f"(original_amount + adjusted_amount - instalment_amount - receipt_amount - (case when {i} = 1 then 0 else pending_amount end)) as outstanding",
      "(-1 * instalment_amount) as instalments",
      "original_amount as fee_value",
      "(adjusted_amount + original_amount) as actual_amount",
      "(-1 * receipt_amount) as receipts"
    )
  f1Df = GeneralAliasDataFrameColumns(f1Df, "f1_")  


  df_fee_summary = f1Df \
    .join(fl2Df, (col("f1_fee_record_code") == col("fl2_fee_record_code")) & (col("f1_fes_person_code") == col("fl2_fes_person_code")) & (col("fl2_status") == 'F') & (col("fl2_fes_record_type") == 'F'), how="inner") \
    .join(GeneralAliasDataFrameColumns(feeValuesDf, "fv_"), col("fl2_fee_value_number") == col("fv_fee_value_number"), how="inner") \
    .join(GeneralAliasDataFrameColumns(peopleDf, "p_"), col("fl2_fes_person_code") == col("p_person_code"), how="inner") \
    .selectExpr(
      "fl2_rul_code as RUL_CODE",
      "fl2_fes_fee_type as FES_FEE_TYPE_CODE",
      "f1_ADJUSTMENTS as ADJUSTMENTS",
      "f1_OUTSTANDING as OUTSTANDING",
      "f1_INSTALMENTS as INSTALMENTS",
      "f1_FEE_VALUE as FEE_VALUE",
      "f1_ACTUAL_AMOUNT as ACTUAL_AMOUNT",
      "f1_RECEIPTS as RECEIPTS"
    )
  
  #Get the fees rolled upto course enrolments from unit enrolments
  dfFeeSum2 = df_fee_summary.join(pulDf, col("RUL_CODE") == col("CHILD_ID"), how="inner") \
    .join(ftDf, col("FES_FEE_TYPE_CODE") == col("ft_FEE_TYPE_CODE"), how="inner") \
    .where(expr("ft_FEE_CATEGORY_CODE IN ('TVH','TAFEFEE', 'COMM')")) \
    .groupBy("PARENT_ID") \
    .agg(
            func.round(sum("fee_value"), 2).alias("OriginalFee"),
            func.round(sum("ACTUAL_AMOUNT"), 2).alias("ActualFee"),
            func.round(sum("RECEIPTS"), 2).alias("FeePaid"),
            func.round(sum("INSTALMENTS"), 2).alias("FeeInstalment"),
            func.round(sum("ADJUSTMENTS"), 2).alias("FeeAdjustment"),
            func.round(sum("OUTSTANDING"), 2).alias("FeeOutstanding")
          )
  #Get the fees for course enrolments
  dfFeeSum = df_fee_summary.join(ftDf, col("FES_FEE_TYPE_CODE") == col("ft_FEE_TYPE_CODE"), how="inner") \
    .where(expr("ft_FEE_CATEGORY_CODE IN ('TVH','TAFEFEE', 'COMM')")) \
    .groupBy("RUL_CODE") \
    .agg(
            func.round(sum("fee_value"), 2).alias("OriginalFee"),
            func.round(sum("ACTUAL_AMOUNT"), 2).alias("ActualFee"),
            func.round(sum("RECEIPTS"), 2).alias("FeePaid"),
            func.round(sum("INSTALMENTS"), 2).alias("FeeInstalment"),
            func.round(sum("ADJUSTMENTS"), 2).alias("FeeAdjustment"),
            func.round(sum("OUTSTANDING"), 2).alias("FeeOutstanding")
          )
  
  ######################################################################################

  transitionsDf = GeneralAliasDataFrameColumns(peopleUnitsDf, "pu_").filter(col("pu_transition_from_people_unit_id").isNotNull()) \
    .join(GeneralAliasDataFrameColumns(peopleUnitsDf, "pu_t_"), col("pu_TRANSITION_FROM_PEOPLE_UNIT_ID") == col("pu_t_ID"), how="inner") \
    .join(GeneralAliasDataFrameColumns(unitInstanceOccurrencesDf, "uio_"), col("uio_uio_id") == col("pu_t_uio_id"), how="inner") \
    .join(GeneralAliasDataFrameColumns(unitInstancesDf, "ui_"), col("ui_fes_unit_instance_code") == col("uio_fes_uins_instance_code"), how="inner") \
    .join(GeneralAliasDataFrameColumns(peopleUnitsSpecialDf, "pus_"), col("pus_people_units_id") == col("pu_t_id"), how="left") \
    .filter((col("pu_transition_from_people_unit_id").isNotNull()) & (col("ui_ui_level") == 'COURSE') & (col("ui_unit_category") != 'BOS')) \
    .selectExpr("pu_id as id" \
            ,"pu_transition_from_people_unit_id as transition_from_people_unit_id" \
            ,"pu_t_unit_instance_code as transition_from_course_code" \
            ,"pu_t_calocc_code as transition_from_course_calocc_code" \
            ,"cast(coalesce(pus_start_date, uio_fes_start_date) as date) as transition_from_course_enrolment_start_date")

  ###########################################################################

  attainmentsDf = attainmentsDf.withColumn("rn", expr("row_number()over(partition by people_units_id order by coalesce(updated_date, created_date) desc, attainment_code desc)")).filter(col("rn") == 1).drop("rn")
  
  df_ce_base = GeneralAliasDataFrameColumns(peopleUnitsDf, "pu_") \
    .join(GeneralAliasDataFrameColumns(peopleDf, "p_"), col("p_person_code") == col("pu_person_code"), how="inner") \
    .join(GeneralAliasDataFrameColumns(unitInstanceOccurrencesDf, "uio_"), col("uio_UIO_ID") == col("pu_UIO_ID"), how="inner") \
    .join(GeneralAliasDataFrameColumns(unitInstancesDf, "ui_"), col("ui_fes_unit_instance_code") == col("uio_fes_uins_instance_code"), how="inner") \
    .join(GeneralAliasDataFrameColumns(attainmentsDf, "att_"), col("att_people_units_id") == col("pu_id"), how="left") \
    .join(GeneralAliasDataFrameColumns(peopleUnitsSpecialDf, "pus_"), col("pus_PEOPLE_UNITS_ID") == col("PU_ID"), how="left") \
    .join(GeneralAliasDataFrameColumns(configurableStatusesDf, "cs_"), col("cs_ID") == col("att_configurable_status_id"), how="left") \
    .join(GeneralAliasDataFrameColumns(transitionsDf, "tra_"), col("tra_id") == col("pu_id"), how="left") \
    .join(webConfigDf.where("parameter = 'INST_CODE'").selectExpr("parameter_value as InstituteId")) \
    .join(GeneralAliasDataFrameColumns(avetmissCourseDf, "ac_"), expr("CASE WHEN ui_NATIONAL_COURSE_CODE IS NOT NULL THEN ui_NATIONAL_COURSE_CODE WHEN LEFT(RIGHT(pu_UNIT_INSTANCE_CODE,3),1) = 'V' AND LEFT(RIGHT(pu_UNIT_INSTANCE_CODE,2),1) IN ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9') THEN SUBSTRING(pu_UNIT_INSTANCE_CODE, 1, CHAR_LENGTH(pu_UNIT_INSTANCE_CODE)-3) ELSE pu_UNIT_INSTANCE_CODE END") == col("ac_CourseCodeConverted"), how="left") \
    .join(GeneralAliasDataFrameColumns(organisationUnitsDf, "ou_"), col("ou_organisation_code") == col("uio_owning_organisation"), how="left") \
    .join(GeneralAliasDataFrameColumns(awardPrintedDetailsDf, "apd_"), col("apd_ATTAINMENT_CODE") == col("att_ATTAINMENT_CODE"), how="left") \
    .join(GeneralAliasDataFrameColumns(dfFeeSum2, "FS2_"), (col("pu_id") == col("FS2_PARENT_ID")), how="left") \
    .join(GeneralAliasDataFrameColumns(dfFeeSum, "FS_"), (col("pu_id") == col("FS_RUL_CODE")), how="left") \
    .where(expr("pu_UNIT_TYPE = 'R' AND ui_UI_LEVEL = 'COURSE' AND coalesce(ui_UNIT_CATEGORY, '') <> 'BOS'")) \
    .selectExpr(
      "cast(InstituteId as int) as InstituteId"
      ,"uio_SLOC_LOCATION_CODE as DeliveryLocationCode /*01M-EDWDM-210*/"
      ,"coalesce(cast(p_REGISTRATION_NO as bigint), concat(cast(InstituteId as int), cast(p_PERSON_CODE as int))) as AvetmissClientId/*02T-EDWDM-211*/"
      ,"CASE WHEN p_REGISTRATION_NO IS NULL THEN 'Y' ELSE 'N' END as AvetmissClientIdMissingFlag /*03T-EDWDM-212*/"
      ,"cast(p_PERSON_CODE as int) as StudentId /*04M-EDWDM-213*/"
      ,"COALESCE(cast(pu_ID as bigint), concat(cast(InstituteId as int), cast(p_PERSON_CODE as bigint))) as CourseOfferingEnrolmentId /*05T-EDWDM-214*/"
      ,"pu_UIO_ID as CourseOfferingId /*06M-EDWWM-215*/"
      ,"pu_CALOCC_CODE as CourseCaloccCode /*07M-EDWWM-216*/"
      ,"ui_FES_UNIT_INSTANCE_CODE as CourseCode /*08M-EDWDM-217*/"
      ,"ui_NATIONAL_COURSE_CODE as NationalCourseCode /*09M-EDWDM-218*/"
      ,"CASE \
          WHEN ui_NATIONAL_COURSE_CODE IS NOT NULL THEN ui_NATIONAL_COURSE_CODE \
          WHEN LEFT(RIGHT(pu_UNIT_INSTANCE_CODE,3),1) = 'V' AND LEFT(RIGHT(pu_UNIT_INSTANCE_CODE,2),1) IN ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9') THEN SUBSTRING(pu_UNIT_INSTANCE_CODE, 1, CHAR_LENGTH(pu_UNIT_INSTANCE_CODE)-3) \
          ELSE pu_UNIT_INSTANCE_CODE \
        END as CourseCodeConverted"
      ,"COALESCE(ac_AvetmissCourseCode, \
                  LEFT(CASE \
                         WHEN ui_NATIONAL_COURSE_CODE IS NOT NULL THEN ui_NATIONAL_COURSE_CODE \
                         WHEN LEFT(RIGHT(pu_UNIT_INSTANCE_CODE,3),1) = 'V'   \
                              AND LEFT(RIGHT(pu_UNIT_INSTANCE_CODE,2),1) IN ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')  \
                                THEN SUBSTRING(pu_UNIT_INSTANCE_CODE, 1, CHAR_LENGTH(pu_UNIT_INSTANCE_CODE)-3) \
                         ELSE pu_UNIT_INSTANCE_CODE \
                       END, 10)) as AvetmissCourseCode /*10T-EDWDM-219*/"
      ,"cast(COALESCE(pus_START_DATE, uio_FES_START_DATE) as date) as CourseEnrolmentStartDate/*11T-EDWDM-220*/"
      ,"cast(COALESCE(pus_END_DATE, uio_FES_END_DATE) as date) as CourseEnrolmentEndDate /*12T-DWDM-221*/"
      ,"pu_PROGRESS_CODE as CourseProgressCode/*13M-EDWDM-222*/ "
      ,"pu_PROGRESS_STATUS as CourseProgressStatus/*14M-EDWDM-223*/ "
      ,"pu_DESTINATION as CourseProgressReason/* 15M-EDWDM-224*/ "
      ,"cast(pu_PROGRESS_DATE as date) as CourseProgressDate/* 16M-EDWDM-225 */ "
      ,"cast(att_DATE_AWARDED as date) as CourseAwardDate/* 17M-EDWDM-226 */ "
      ,"SUBSTRING(att_DESCRIPTION, 1, 70) as CourseAwardDescription/* 18T-EDWDM-227 */ "
      ,"cs_STATUS_CODE as CourseAwardStatus/* 19M-EDWDM-228  */ "
      ,"pu_NZ_FUNDING as CourseFundingSourceCode/* 20M-EDWDM-229 */ "
      ,"tra_transition_from_people_unit_id as CourseEnrolmentIdTransitionFrom "
      ,"tra_transition_from_course_code as CourseCodeTransitionFrom "
      ,"tra_transition_from_course_calocc_code as CourseCaloccCodeTransitionFrom "
      ,"tra_transition_from_course_enrolment_start_date as CourseEnrolmentStartDateTransitionFrom/*29T-EDWDM-230  */ "
      ,"SUBSTRING(uio_FES_USER_1, 1, 50) as CourseOfferingRemarks/* 30T-EDWDM-231 */ "
      ,"uio_OWNING_ORGANISATION as CourseOfferingFunctionalUnit/* 31M-EDWDM-232 */ "
      ,"CASE \
         WHEN att_date_awarded is not null AND coalesce(ATT_DESCRIPTION, '') <> 'Withdrawn Award' AND CS_STATUS_CODE  = 'PUBLISHED' THEN 'Y' \
         ELSE 'N' \
        END as CourseAwardFlag /*36T-EDWDM-237  */"
      ,"CASE \
          WHEN pu_progress_code ='1.1 ACTIVE' THEN 1 \
          WHEN pu_progress_code in ('1.6 VFHAPP','1.8FHAPP') then 2 \
          ELSE 3 \
        END as CourseProgressRank /*38T-EDWDM-239*/"
      ,"pu_SUBSIDISED_AMOUNT as SubsidisedAmount/* 45M-EDWDM-422 */ "
      ,"pu_STANDARD_SUBSIDY_AMOUNT as StandardSubsidyAmount/*46M-EDWDM-423  */ "
      ,"pu_SUBSIDISED_NEEDS_LOADING as NeedsLoadingAmount/*47M-EDWDM-424  */ "
      ,"pu_SUBSIDISED_LOCATION_LOADING as LocationLoadingAmount/*48M-EDWDM-425  */ "
      ,"ui_AWARD_CATEGORY_CODE as AWARD /*EDWDM-893*/ "
      ,"SUBSTRING(pu_USER_1,1,1) AS APPRTRAINEE /*EDWDM-895*/ "
      ,"pu_COMMITMENT_IDENTIFIER as CommitmentIdentifier "
      ,"ou_FES_FULL_NAME AS EnrolmentTeachingSectionName"
      ,"pu_HAS_LTU_EVIDENCE HasLongTermUnemploymentEvidence "
      ,"uio_OFFERING_TYPE CourseEnrolmentOfferingType "
      ,"pu_SPONSOR_ORG_CODE as SponsorOrginationCode "
      ,"pu_STUDY_REASON as StudyReason " 
      ,"SUBSTRING(uio_FES_USER_1,1,20) AS OFFERING_CODE "
      ,"pu_TRAINING_CONTRACT_IDENTIFIER AS TrainingContractID "
      ,"pu_WELFARE_STATUS as WelfareStatus "
      ,"pu_WHO_TO_PAY as WhoToPay "
      ,"pu_ORGANISATION_CODE "
      ,"pu_ALTERNATE_EMPLOYER_POSTCODE as EmployerPostcode "
      ,"pu_ALTERNATE_EMPLOYER_SUBURB as EmployerSuburb " 
      ,"pu_USER_5 as WorksInNSW " 
      ,"ui_UNIT_CATEGORY " 
      ,"ui_IS_VET_FEE_HELP VFHCourse " 
      ,"pu_CREATED_DATE CourseEnrolmentCreatedDate " 
      ,"ui_COURSE_OCCUPATION ANZSCO " 
      ,"pu_DATA_RETURN AS AvetmissFlag " 
      ,"uio_PREDOMINANT_DELIVERY_MODE AS PredominantDeliveryMode" 
      ,"pu_SUBSIDY_STATUS AS SubsidyStatus" 
      ,"att_AWARD_ID AS AwardID"
      ,"CASE WHEN apd_TESTAMUR_DATE_LAST_PRINTED IS NOT NULL THEN 'Y' ELSE 'N' END AS IssuedFlag" 
      ,"apd_TESTAMUR_DATE_LAST_PRINTED AS ParchmentIssueDate /*PRADA-1624*/" 
      ,"att_DATE_CONFERRED AS DateConferred" 
      ,"att_Configurable_status_id AS ConfigurableStatusID" 
      ,"att_CREATED_DATE AS AttainmentCreatedDate"
      ,"att_UPDATED_DATE AS AttainmentUpdatedDate"
      ,"pu_PROGRAM_STREAM ProgramStream"
      ,"CASE  \
          /*WHEN att_date_awarded is not null AND att_date_awarded < d_ReportingYearStartDate THEN 0 */ \
          WHEN pu_PROGRESS_CODE IN ('0.0 CANCRG','0.1 STBY','0.2 ROI','0.3 TVHPEN','0.4 AS','0.5 AO','0.5 INC','0.6 AD','0.7PAIDNS','0.8 A/T UN','0.9 RENR','1.0 UNPAID','3.3WN','3.4NS',' ','0.71TFCMAN','0.72TFCERR','0.25 VSLAE', '0.23 TESTE','0.24 EVIE', '0.25VSLAEE','0.12 AW','0.21 EP','0.23 TESTE','0.24 EVIE','0.7 AA') THEN 0 \
          WHEN pu_DESTINATION IN ('CNPORT','WDCC','WDEC','WDSBC') /*('Student Withdraws Before Class','Cancelled via Portal','Withdrawal Course Cancelled','Withdrawal Error Correct') */ THEN 0 \
          ELSE 1 \
        END EnrolCountPU /*35T-EDWDM-236 */" 
      ,"coalesce(FS2_OriginalFee, FS_OriginalFee) as OriginalFee"
      ,"coalesce(FS2_ActualFee, FS_ActualFee) as ActualFee"
      ,"coalesce(FS2_FeePaid, FS_FeePaid) as FeePaid"
      ,"coalesce(FS2_FeeInstalment, FS_FeeInstalment) as FeeInstalment"
      ,"coalesce(FS2_FeeAdjustment, FS_FeeAdjustment) as FeeAdjustment"
      ,"coalesce(FS2_FeeOutstanding, FS_FeeOutstanding) as FeeOutstanding"
     ).dropDuplicates()

  #dedup
  dfCourseEnrolmentAll = df_ce_base.withColumn("rownum", 
                            expr("row_number() over (partition by CourseOfferingEnrolmentId \
                                                          order by EnrolCountPU desc, \
                                                                  coalesce(AttainmentUpdatedDate, AttainmentCreatedDate) desc, \
                                                                  coalesce(CourseAwardDate, to_date('9999-12-31')) desc, \
                                                                  CourseProgressDate desc, \
                                                                  CourseCaloccCode desc)"))\
                            .filter(col("rownum") == 1)\
                            .drop("rownum")
  
  return dfCourseEnrolmentAll

# COMMAND ----------

def _CourseEnrolmentShell(dfCourseEnrolmentAll, dfCourseEnrolmentRollup, dfCurrentReportingPeriod):
  ######################################################################################
  #Objective - determine the course shell enrolments
  #accepts the dataframes for course enrolments - all, course enrolment - rollup from unit enrolments and filtered date to get the reporting year for the course enrolments 
  #fetch the course enrolments from course enrolments - all which do not exist in course enrolment - rollup; meaning these course enrolments do not have any unit enrolments attached, hence shell enrolments
  #returns the dataframe with course enrolments (shell)
  ######################################################################################

  dfCourseEnrolmentAllCurrent = dfCourseEnrolmentAll.join(dfCurrentReportingPeriod, (dfCourseEnrolmentAll.CourseEnrolmentStartDate >= dfCurrentReportingPeriod.ReportingYearStartDate) & (dfCourseEnrolmentAll.CourseEnrolmentStartDate <= dfCurrentReportingPeriod.ReportingYearEndDate) & (dfCourseEnrolmentAll.CourseEnrolmentEndDate >= dfCurrentReportingPeriod.ReportingYearStartDate), how="inner")
  
  dfCourseEnrolmentAllCurrent = dfCourseEnrolmentAllCurrent.where(expr("coalesce(CourseProgressCode, '') not in ('0.0 CANCRG','0.1 STBY','0.2 ROI','0.3 TVHPEN','0.4 AS','0.5 INC','0.6 AD','0.7PAIDNS','0.8 A/T UN','0.9 RENR','1.0 UNPAID','3.3WN','3.4NS',' ','0.71TFCMAN','0.72TFCERR','0.25 VSLAE', '0.23 TESTE','0.24 EVIE', '0.25VSLAEE','0.12 AW','0.21 EP','0.23 TESTE','0.24 EVIE','0.7 AA')"))
  dfCourseEnrolmentAllCurrent = dfCourseEnrolmentAllCurrent.where(expr("coalesce(CourseProgressReason, '') not in ('CNPORT','WDCC','WDEC','WDSBC')"))
  
  dfCourseEnrolmentAllCurrent = dfCourseEnrolmentAllCurrent.withColumn("GraduateFlag", 
                                     expr("CASE \
                                             WHEN CourseAwardFlag = 'Y' AND CourseAwardDate >= ReportingYearStartDate THEN 1 \
                                             ELSE 0 \
                                           END")) 

  dfShellEnrolment = dfCourseEnrolmentAllCurrent.join (dfCourseEnrolmentRollup, 
                    (dfCourseEnrolmentAllCurrent.ReportingYear == dfCourseEnrolmentRollup.ReportingYear) &
                    (dfCourseEnrolmentAllCurrent.AvetmissClientId == dfCourseEnrolmentRollup.AvetmissClientId) &
                    (dfCourseEnrolmentAllCurrent.AvetmissCourseCode == dfCourseEnrolmentRollup.AvetmissCourseCode) &
                    (dfCourseEnrolmentAllCurrent.CourseOfferingEnrolmentId == dfCourseEnrolmentRollup.CourseOfferingEnrolmentId), how="left_anti")
  cols = ['TotalHours','RPLHours','TrainingHours','ContinuingHours','ValidUnitCount','UnitCount','FinalGradeUnitCount','PassGradeUnitCount','WithdrawnUnitCount','FailGradeUnitCount','RPLUnitCount','CreditTransferUnitCount','NonAssessableUnitCount','MissingGradeUnitCount','ContinuingUnitCount','RPLGrantedUnitCount','RPLNotGrantedUnitCount','CreditTransferNationalRecognitionUnitCount','SupersededUnitCount','NonAssessableSatisfactorilyCompletedUnitCount','NonAssessableNotSatisfactorilyCompletedUnitCount','PayableUnitCount','Semester1UnitCount','Semester2UnitCount','Semester3UnitCount','TotalSemester1Hours','TotalSemester2Hours','ExemptedDeliveryUnitCount','InClassDeliveryUnitCount','DistanceDeliveryUnitCount','ElectronicDeliveryUnitCount','BlendedDeliveryUnitCount','SelfDeliveryUnitCount','OnTheJobTrainingDeliveryUnitCount','OnTheJobDistanceDeliveryUnitCount','SimulatedDeliveryUnitCount','MissingDeliveryModeUnitCount','OtherDeliveryUnitCount','DeliveredUnitCount', 'ValidFlag']
  for c in cols:
    dfShellEnrolment = dfShellEnrolment.withColumn(c, lit(0))

  dfShellEnrolment = dfShellEnrolment.withColumn("ShellEnrolmentFlag",lit(1))
  dfShellEnrolment = dfShellEnrolment.withColumn("ProgramStartDate", col("CourseEnrolmentStartDate"))
  
  

  return dfShellEnrolment


# COMMAND ----------

def _CourseEnrolment(dfCourseEnrolmentAll, dfCourseEnrolmentRollup, dfShellEnrolment):
  ######################################################################################
  #Objective - determine the course enrolments = rollup + shell
  #accepts the dataframes for course enrolments - all to join with rollups to get to the other columns from course enrolment - all , course enrolment - rollup and course enrolments - shell
  #join course enrolment - rollup with course enrolment - all to get the missing columns from all for rollup enrolments
  #get the required columns for both rollup and shell course enrolments.
  #UNION rollup and shell enrolments to get to the combined course enrolments
  #returns the dataframe with course enrolments
  ######################################################################################
  dfCourseEnrolmentAll = GeneralAliasDataFrameColumns(dfCourseEnrolmentAll, 'All_')
  dfCourseEnrolmentRollup = GeneralAliasDataFrameColumns(dfCourseEnrolmentRollup, 'Rollup_')
  
  dfCourseEnrolmentRollupwithAllColumns = dfCourseEnrolmentRollup.join(dfCourseEnrolmentAll,         (dfCourseEnrolmentRollup.Rollup_CourseOfferingEnrolmentId == dfCourseEnrolmentAll.All_CourseOfferingEnrolmentId) &                          (dfCourseEnrolmentRollup.Rollup_AvetmissCourseCode == dfCourseEnrolmentAll.All_AvetmissCourseCode), how="left").selectExpr("Rollup_InstituteId as InstituteId",
  "Rollup_DeliveryLocationCode as DeliveryLocationCode",
  "Rollup_AvetmissClientId as AvetmissClientId",
  "Rollup_AvetmissClientIdMissingFlag as AvetmissClientIdMissingFlag",
  "Rollup_StudentId as StudentId",
  "Rollup_CourseOfferingEnrolmentId as CourseOfferingEnrolmentId",
  "Rollup_CourseOfferingId as CourseOfferingId",
  "Rollup_CourseOfferingCode as CourseCaloccCode",
  "Rollup_CourseCode as CourseCode",
  "Rollup_NationalCourseCode as NationalCourseCode",
  "Rollup_CourseCodeConverted as CourseCodeConverted",
  "Rollup_AvetmissCourseCode as AvetmissCourseCode",
  "Rollup_CourseEnrolmentStartDate as CourseEnrolmentStartDate",
  "Rollup_CourseEnrolmentEndDate as CourseEnrolmentEndDate",
  "Rollup_CourseProgressCode as CourseProgressCode",
  "Rollup_CourseProgressStatus as CourseProgressStatus",
  "Rollup_CourseProgressReason as CourseProgressReason",
  "Rollup_CourseProgressDate as CourseProgressDate",
  "Rollup_CourseAwardDate as CourseAwardDate",
  "Rollup_CourseAwardDescription as CourseAwardDescription",
  "Rollup_CourseAwardStatus as CourseAwardStatus",
  "Rollup_CourseFundingSourceCode as CourseFundingSourceCode",
  "Rollup_CourseEnrolmentIdTransitionFrom as CourseEnrolmentIdTransitionFrom",
  "Rollup_CourseCodeTransitionFrom as CourseCodeTransitionFrom",
  "Rollup_CourseCaloccCodeTransitionFrom as CourseCaloccCodeTransitionFrom",
  "Rollup_CourseEnrolmentStartDateTransitionFrom as CourseEnrolmentStartDateTransitionFrom",
  "Rollup_CourseOfferingRemarks as CourseOfferingRemarks",
  "Rollup_CourseOfferingFunctionalUnit as CourseOfferingFunctionalUnit",
  "Rollup_ReportingYear as ReportingYear",
  "Rollup_ReportingYearStartDate as ReportingYearStartDate",
  "Rollup_ReportingYearEndDate as ReportingYearEndDate",
  "All_CourseAwardFlag as CourseAwardFlag",
  "CASE \
     WHEN All_CourseAwardFlag = 'Y' AND All_CourseAwardDate >= Rollup_ReportingYearStartDate THEN 1 \
     ELSE 0 \
   END as GraduateFlag",
  "All_CourseProgressRank as CourseProgressRank",
  "All_SubsidisedAmount as SubsidisedAmount",
  "All_StandardSubsidyAmount as StandardSubsidyAmount",
  "All_NeedsLoadingAmount as NeedsLoadingAmount",
  "All_LocationLoadingAmount as LocationLoadingAmount",
  "All_AWARD as AWARD",
  "All_APPRTRAINEE as APPRTRAINEE",
  "All_CommitmentIdentifier as CommitmentIdentifier",
  "Rollup_SponsorDescription as EnrolmentTeachingSectionName",
  "All_HasLongTermUnemploymentEvidence as HasLongTermUnemploymentEvidence",
  "All_CourseEnrolmentOfferingType as CourseEnrolmentOfferingType",
  "All_SponsorOrginationCode as SponsorOrginationCode",
  "All_StudyReason as StudyReason",
  "All_OFFERING_CODE as OFFERING_CODE",
  "All_TrainingContractID as TrainingContractID",
  "All_WelfareStatus as WelfareStatus",
  "All_WhoToPay as WhoToPay",
  "All_PU_ORGANISATION_CODE as PU_ORGANISATION_CODE",
  "All_EmployerPostcode as EmployerPostcode",
  "All_EmployerSuburb as EmployerSuburb",
  "All_WorksInNSW as WorksInNSW",
  "All_UI_UNIT_CATEGORY as UI_UNIT_CATEGORY",
  "All_VFHCourse as VFHCourse",
  "All_CourseEnrolmentCreatedDate as CourseEnrolmentCreatedDate",
  "All_ANZSCO as ANZSCO",
  "All_AvetmissFlag as AvetmissFlag",
  "All_PredominantDeliveryMode as PredominantDeliveryMode",
  "All_SubsidyStatus as SubsidyStatus",
  "All_AwardID as AwardID",
  "All_IssuedFlag as IssuedFlag",
  "ALL_ParchmentIssueDate as ParchmentIssueDate",
  "All_DateConferred as DateConferred",
  "All_ConfigurableStatusID as ConfigurableStatusID",
  "All_AttainmentCreatedDate as AttainmentCreatedDate",
  "All_AttainmentUpdatedDate as AttainmentUpdatedDate",
  "All_EnrolCountPU as EnrolCountPU",
  "All_OriginalFee as OriginalFee",
  "All_ActualFee as ActualFee",
  "All_FeePaid as FeePaid",
  "All_FeeInstalment as FeeInstalment",
  "All_FeeAdjustment as FeeAdjustment",
  "All_FeeOutstanding as FeeOutstanding",
  "Rollup_TotalHours as TotalHours",
  "Rollup_RPLHours as RPLHours",
  "Rollup_TrainingHours as TrainingHours",
  "Rollup_ContinuingHours as ContinuingHours",
  "Rollup_ValidUnitCount as ValidUnitCount",
  "Rollup_UnitCount as UnitCount",
  "Rollup_FinalGradeUnitCount as FinalGradeUnitCount",
  "Rollup_PassGradeUnitCount as PassGradeUnitCount",
  "Rollup_WithdrawnUnitCount as WithdrawnUnitCount",
  "Rollup_FailGradeUnitCount as FailGradeUnitCount",
  "Rollup_RPLUnitCount as RPLUnitCount",
  "Rollup_CreditTransferUnitCount as CreditTransferUnitCount",
  "Rollup_NonAssessableUnitCount as NonAssessableUnitCount",
  "Rollup_MissingGradeUnitCount as MissingGradeUnitCount",
  "Rollup_ContinuingUnitCount as ContinuingUnitCount",
  "Rollup_RPLGrantedUnitCount as RPLGrantedUnitCount",
  "Rollup_RPLNotGrantedUnitCount as RPLNotGrantedUnitCount",
  "Rollup_CreditTransferNationalRecognitionUnitCount as CreditTransferNationalRecognitionUnitCount",
  "Rollup_SupersededUnitCount as SupersededUnitCount",
  "Rollup_NonAssessableSatisfactorilyCompletedUnitCount as NonAssessableSatisfactorilyCompletedUnitCount",
  "Rollup_NonAssessableNotSatisfactorilyCompletedUnitCount as NonAssessableNotSatisfactorilyCompletedUnitCount",
  "Rollup_PayableUnitCount as PayableUnitCount",
  "Rollup_Semester1UnitCount as Semester1UnitCount",
  "Rollup_Semester2UnitCount as Semester2UnitCount",
  "Rollup_Semester3UnitCount as Semester3UnitCount",
  "Rollup_TotalSemester1Hours as TotalSemester1Hours",
  "Rollup_TotalSemester2Hours as TotalSemester2Hours",
  "Rollup_ExemptedDeliveryUnitCount as ExemptedDeliveryUnitCount",
  "Rollup_InClassDeliveryUnitCount as InClassDeliveryUnitCount",
  "Rollup_DistanceDeliveryUnitCount as DistanceDeliveryUnitCount",
  "Rollup_ElectronicDeliveryUnitCount as ElectronicDeliveryUnitCount",
  "Rollup_BlendedDeliveryUnitCount as BlendedDeliveryUnitCount",
  "Rollup_SelfDeliveryUnitCount as SelfDeliveryUnitCount",
  "Rollup_OnTheJobTrainingDeliveryUnitCount as OnTheJobTrainingDeliveryUnitCount",
  "Rollup_OnTheJobDistanceDeliveryUnitCount as OnTheJobDistanceDeliveryUnitCount",
  "Rollup_SimulatedDeliveryUnitCount as SimulatedDeliveryUnitCount",
  "Rollup_MissingDeliveryModeUnitCount as MissingDeliveryModeUnitCount",
  "Rollup_OtherDeliveryUnitCount as OtherDeliveryUnitCount",
  "Rollup_DeliveredUnitCount as DeliveredUnitCount",
  "Rollup_ValidFlag as ValidFlag",
  "Rollup_ShellEnrolmentFlag as ShellEnrolmentFlag",
  "Rollup_ProgramStartDate as ProgramStartDate",
  "Rollup_ProgramStream as ProgramStream",
  "Rollup_ReportingDate as ReportingDate"
  )

  dfShellEnrolment = dfShellEnrolment.selectExpr("InstituteId", "DeliveryLocationCode", "AvetmissClientId", "AvetmissClientIdMissingFlag", "StudentId", "CourseOfferingEnrolmentId", "CourseOfferingId", "CourseCaloccCode", "CourseCode", "NationalCourseCode", "CourseCodeConverted", "AvetmissCourseCode", "CourseEnrolmentStartDate", "CourseEnrolmentEndDate", "CourseProgressCode", "CourseProgressStatus", "CourseProgressReason", "CourseProgressDate", "CourseAwardDate", "CourseAwardDescription", "CourseAwardStatus", "CourseFundingSourceCode", "CourseEnrolmentIdTransitionFrom", "CourseCodeTransitionFrom", "CourseCaloccCodeTransitionFrom", "CourseEnrolmentStartDateTransitionFrom", "CourseOfferingRemarks", "CourseOfferingFunctionalUnit", "ReportingYear", "ReportingYearStartDate", "ReportingYearEndDate", "CourseAwardFlag", "GraduateFlag", "CourseProgressRank", "SubsidisedAmount", "StandardSubsidyAmount", "NeedsLoadingAmount", "LocationLoadingAmount", "AWARD", "APPRTRAINEE", "CommitmentIdentifier", "EnrolmentTeachingSectionName", "HasLongTermUnemploymentEvidence", "CourseEnrolmentOfferingType", "SponsorOrginationCode", "StudyReason", "OFFERING_CODE", "TrainingContractID", "WelfareStatus", "WhoToPay", "PU_ORGANISATION_CODE", "EmployerPostcode", "EmployerSuburb", "WorksInNSW", "UI_UNIT_CATEGORY", "VFHCourse", "CourseEnrolmentCreatedDate", "ANZSCO", "AvetmissFlag", "PredominantDeliveryMode", "SubsidyStatus", "AwardID", "IssuedFlag", "ParchmentIssueDate", "DateConferred", "ConfigurableStatusID", "AttainmentCreatedDate", "AttainmentUpdatedDate", "EnrolCountPU", "OriginalFee", "ActualFee", "FeePaid", "FeeInstalment", "FeeAdjustment", "FeeOutstanding", "TotalHours", "RPLHours", "TrainingHours", "ContinuingHours", "ValidUnitCount", "UnitCount", "FinalGradeUnitCount", "PassGradeUnitCount", "WithdrawnUnitCount", "FailGradeUnitCount", "RPLUnitCount", "CreditTransferUnitCount", "NonAssessableUnitCount", "MissingGradeUnitCount", "ContinuingUnitCount", "RPLGrantedUnitCount", "RPLNotGrantedUnitCount", "CreditTransferNationalRecognitionUnitCount", "SupersededUnitCount", "NonAssessableSatisfactorilyCompletedUnitCount", "NonAssessableNotSatisfactorilyCompletedUnitCount", "PayableUnitCount", "Semester1UnitCount", "Semester2UnitCount", "Semester3UnitCount", "TotalSemester1Hours", "TotalSemester2Hours", "ExemptedDeliveryUnitCount", "InClassDeliveryUnitCount", "DistanceDeliveryUnitCount", "ElectronicDeliveryUnitCount", "BlendedDeliveryUnitCount", "SelfDeliveryUnitCount", "OnTheJobTrainingDeliveryUnitCount", "OnTheJobDistanceDeliveryUnitCount", "SimulatedDeliveryUnitCount", "MissingDeliveryModeUnitCount", "OtherDeliveryUnitCount", "DeliveredUnitCount", "ValidFlag", "ShellEnrolmentFlag", "ProgramStartDate", "ProgramStream", "ReportingDate" )
  
  dfCourseEnrolmentCombined = dfCourseEnrolmentRollupwithAllColumns.union(dfShellEnrolment)
  dfCourseEnrolmentCombined = dfCourseEnrolmentCombined.withColumn("ValidUnitExistsFlag", expr("CASE WHEN ValidUnitCount > 0 THEN 1 ELSE 0 END")) 
  
  #dedup course enrolments by ReportingYear, AvetmissClientId, AvetmissCourseCode, DeliveryLocationCode, ValidFlag
  dfCourseEnrolmentDedup = dfCourseEnrolmentCombined.withColumn("rn", expr("row_number() over(partition by ReportingYear, AvetmissClientId, AvetmissCourseCode, DeliveryLocationCode order by ValidFlag desc, coalesce(GraduateFlag, 0) desc, coalesce(CourseProgressRank, 9), PassGradeUnitCount desc, ValidUnitExistsFlag desc, CourseProgressDate desc, CourseCaloccCode desc)")).filter(col("rn") == 1).drop("rn")
  
  #Aggregations grouping by ReportingYear, AvetmissClientId, AvetmissCourseCode, DeliveryLocationCode, ValidFlag
  dfCourseEnrolmentDedupAgg = dfCourseEnrolmentCombined.groupBy("ReportingYear", "AvetmissClientId",  "AvetmissCourseCode", "DeliveryLocationCode", "ValidFlag") \
    .agg(sum(expr("TotalHours")).alias("TotalHours1") \
        ,sum(expr("RPLHours")).alias("RPLHours1") \
        ,sum(expr("TrainingHours")).alias("TrainingHours1") \
        ,sum(expr("ContinuingHours")).alias("ContinuingHours1") \
        ,sum(expr("ValidUnitCount")).alias("ValidUnitCount1") \
        ,sum(expr("UnitCount")).alias("UnitCount1") \
        ,sum(expr("FinalGradeUnitCount")).alias("FinalGradeUnitCount1") \
        ,sum(expr("PassGradeUnitCount")).alias("PassGradeUnitCount1") \
        ,sum(expr("WithdrawnUnitCount")).alias("WithdrawnUnitCount1") \
        ,sum(expr("FailGradeUnitCount")).alias("FailGradeUnitCount1") \
        ,sum(expr("RPLUnitCount")).alias("RPLUnitCount1") \
        ,sum(expr("CreditTransferUnitCount")).alias("CreditTransferUnitCount1") \
        ,sum(expr("NonAssessableUnitCount")).alias("NonAssessableUnitCount1") \
        ,sum(expr("MissingGradeUnitCount")).alias("MissingGradeUnitCount1") \
        ,sum(expr("ContinuingUnitCount")).alias("ContinuingUnitCount1") \
        ,sum(expr("RPLGrantedUnitCount")).alias("RPLGrantedUnitCount1") \
        ,sum(expr("RPLNotGrantedUnitCount")).alias("RPLNotGrantedUnitCount1") \
        ,sum(expr("CreditTransferNationalRecognitionUnitCount")).alias("CreditTransferNationalRecognitionUnitCount1") \
        ,sum(expr("SupersededUnitCount")).alias("SupersededUnitCount1") \
        ,sum(expr("NonAssessableSatisfactorilyCompletedUnitCount")).alias("NonAssessableSatisfactorilyCompletedUnitCount1") \
        ,sum(expr("NonAssessableNotSatisfactorilyCompletedUnitCount")).alias("NonAssessableNotSatisfactorilyCompletedUnitCount1") \
        ,sum(expr("PayableUnitCount")).alias("PayableUnitCount1") \
        ,sum(expr("Semester1UnitCount")).alias("Semester1UnitCount1") \
        ,sum(expr("Semester2UnitCount")).alias("Semester2UnitCount1") \
        ,sum(expr("Semester3UnitCount")).alias("Semester3UnitCount1") \
        ,sum(expr("TotalSemester1Hours")).alias("TotalSemester1Hours1") \
        ,sum(expr("TotalSemester2Hours")).alias("TotalSemester2Hours1") \
        ,sum(expr("ExemptedDeliveryUnitCount")).alias("ExemptedDeliveryUnitCount1") \
        ,sum(expr("InClassDeliveryUnitCount")).alias("InClassDeliveryUnitCount1") \
        ,sum(expr("DistanceDeliveryUnitCount")).alias("DistanceDeliveryUnitCount1") \
        ,sum(expr("ElectronicDeliveryUnitCount")).alias("ElectronicDeliveryUnitCount1") \
        ,sum(expr("BlendedDeliveryUnitCount")).alias("BlendedDeliveryUnitCount1") \
        ,sum(expr("SelfDeliveryUnitCount")).alias("SelfDeliveryUnitCount1") \
        ,sum(expr("OnTheJobTrainingDeliveryUnitCount")).alias("OnTheJobTrainingDeliveryUnitCount1") \
        ,sum(expr("OnTheJobDistanceDeliveryUnitCount")).alias("OnTheJobDistanceDeliveryUnitCount1") \
        ,sum(expr("SimulatedDeliveryUnitCount")).alias("SimulatedDeliveryUnitCount1") \
        ,sum(expr("MissingDeliveryModeUnitCount")).alias("MissingDeliveryModeUnitCount1") \
        ,sum(expr("OtherDeliveryUnitCount")).alias("OtherDeliveryUnitCount1") \
        ,sum(expr("DeliveredUnitCount")).alias("DeliveredUnitCount1") \
        ,sum(expr("GraduateFlag")).alias("Grad1") \
      )
  
  dfCourseEnrolment = dfCourseEnrolmentDedup.join(dfCourseEnrolmentDedupAgg, on=["ReportingYear", "AvetmissClientId",  "AvetmissCourseCode", "DeliveryLocationCode", "ValidFlag"]) \
    .selectExpr("InstituteId", "DeliveryLocationCode", "AvetmissClientId", "AvetmissClientIdMissingFlag", "StudentId", "CourseOfferingEnrolmentId", "CourseOfferingId", "CourseCaloccCode", "CourseCode", "NationalCourseCode", "CourseCodeConverted", "AvetmissCourseCode", "CourseEnrolmentStartDate", "CourseEnrolmentEndDate", "CourseProgressCode", "CourseProgressStatus", "CourseProgressReason", "CourseProgressDate", "CourseAwardDate", "CourseAwardDescription", "CourseAwardStatus", "CourseFundingSourceCode", "CourseEnrolmentIdTransitionFrom", "CourseCodeTransitionFrom", "CourseCaloccCodeTransitionFrom", "CourseEnrolmentStartDateTransitionFrom", "CourseOfferingRemarks", "CourseOfferingFunctionalUnit", "ReportingYear", "ReportingYearStartDate", "ReportingYearEndDate", "CourseAwardFlag", "GraduateFlag", "CourseProgressRank", "SubsidisedAmount", "StandardSubsidyAmount", "NeedsLoadingAmount", "LocationLoadingAmount", "AWARD", "APPRTRAINEE", "CommitmentIdentifier", "EnrolmentTeachingSectionName", "HasLongTermUnemploymentEvidence", "CourseEnrolmentOfferingType", "SponsorOrginationCode", "StudyReason", "OFFERING_CODE", "TrainingContractID", "WelfareStatus", "WhoToPay", "PU_ORGANISATION_CODE", "EmployerPostcode", "EmployerSuburb", "WorksInNSW", "UI_UNIT_CATEGORY", "VFHCourse", "CourseEnrolmentCreatedDate", "ANZSCO", "AvetmissFlag", "PredominantDeliveryMode", "SubsidyStatus", "AwardID", "IssuedFlag", "ParchmentIssueDate", "DateConferred", "ConfigurableStatusID", "AttainmentCreatedDate", "AttainmentUpdatedDate", "EnrolCountPU", "OriginalFee", "ActualFee", "FeePaid", "FeeInstalment", "FeeAdjustment", "FeeOutstanding", "TotalHours1 as TotalHours", "RPLHours1 as RPLHours", "TrainingHours1 as TrainingHours", "ContinuingHours1 as ContinuingHours", "ValidUnitCount1 as ValidUnitCount", "UnitCount1 as UnitCount", "FinalGradeUnitCount1 as FinalGradeUnitCount", "PassGradeUnitCount1 as PassGradeUnitCount", "WithdrawnUnitCount1 as WithdrawnUnitCount", "FailGradeUnitCount1 as FailGradeUnitCount", "RPLUnitCount1 as RPLUnitCount", "CreditTransferUnitCount1 as CreditTransferUnitCount", "NonAssessableUnitCount1 as NonAssessableUnitCount", "MissingGradeUnitCount1 as MissingGradeUnitCount", "ContinuingUnitCount1 as ContinuingUnitCount", "RPLGrantedUnitCount1 as RPLGrantedUnitCount", "RPLNotGrantedUnitCount1 as RPLNotGrantedUnitCount", "CreditTransferNationalRecognitionUnitCount1 as CreditTransferNationalRecognitionUnitCount", "SupersededUnitCount1 as SupersededUnitCount", "NonAssessableSatisfactorilyCompletedUnitCount1 as NonAssessableSatisfactorilyCompletedUnitCount", "NonAssessableNotSatisfactorilyCompletedUnitCount1 as NonAssessableNotSatisfactorilyCompletedUnitCount", "PayableUnitCount1 as PayableUnitCount", "Semester1UnitCount1 as Semester1UnitCount", "Semester2UnitCount1 as Semester2UnitCount", "Semester3UnitCount1 as Semester3UnitCount", "TotalSemester1Hours1 as TotalSemester1Hours", "TotalSemester2Hours1 as TotalSemester2Hours", "ExemptedDeliveryUnitCount1 as ExemptedDeliveryUnitCount", "InClassDeliveryUnitCount1 as InClassDeliveryUnitCount", "DistanceDeliveryUnitCount1 as DistanceDeliveryUnitCount", "ElectronicDeliveryUnitCount1 as ElectronicDeliveryUnitCount", "BlendedDeliveryUnitCount1 as BlendedDeliveryUnitCount", "SelfDeliveryUnitCount1 as SelfDeliveryUnitCount", "OnTheJobTrainingDeliveryUnitCount1 as OnTheJobTrainingDeliveryUnitCount", "OnTheJobDistanceDeliveryUnitCount1 as OnTheJobDistanceDeliveryUnitCount", "SimulatedDeliveryUnitCount1 as SimulatedDeliveryUnitCount", "MissingDeliveryModeUnitCount1 as MissingDeliveryModeUnitCount", "OtherDeliveryUnitCount1 as OtherDeliveryUnitCount", "DeliveredUnitCount1 as DeliveredUnitCount", "Grad1 as TotalGrad", "ValidFlag", "ShellEnrolmentFlag", "ValidUnitExistsFlag", "ProgramStartDate", "ProgramStream", "ReportingDate")
  
  dfCourseEnrolment = dfCourseEnrolment.withColumn("CTOnlyFlag", expr("CASE WHEN UnitCount > 0 and UnitCount = CreditTransferUnitCount THEN 1 ELSE 0 END"))
  #mark the enrolment invalid when the enrolment consists of credit transfer units only  
  dfCourseEnrolment = dfCourseEnrolment.withColumn("ValidFlag", expr("case when CTOnlyFlag = 1 then 0 else ValidFlag end"))
  dfCourseEnrolment = dfCourseEnrolment.withColumn("PayableUnitExistsFlag", expr("CASE WHEN PayableUnitCount > 0 THEN 1 ELSE 0 END")) 
  
  dfCourseEnrolment = dfCourseEnrolment.withColumn("ValidUnitExistsFlag", expr("CASE WHEN ValidUnitCount > 0 THEN 1 ELSE 0 END"))   

  #ENRFLAG
  dfCourseEnrolment = dfCourseEnrolment.withColumn("enrflagrn", expr("row_number() over(partition by ReportingYear, AvetmissClientId, AvetmissCourseCode, ValidFlag order by TotalGrad desc, TotalHours desc, ContinuingHours desc, DeliveryLocationCode desc)")) \
    .withColumn("ENRFLAG", expr("case when enrflagrn = 1 then 1 else 0 end")).drop("enrflagrn")

  #AVETCOUNT
  dfCourseEnrolment = dfCourseEnrolment.withColumn("AVETCOUNT", expr("case when ValidFlag = 1 and ENRFLAG = 1 then 1 else 0 end"))
  
  return dfCourseEnrolment
  

# COMMAND ----------

def _Course (unitInstances0165Df, unitInstances0900Df, unitInstanceLinks0165Df, unitInstanceLinks0900Df, refAvetmissCourseDf, refTrainingPackageISCDf, refFieldOfEducationISCDf, refIndustirySkillsCouncilDf):

  dfUI0165 = unitInstances0165Df.withColumn("PM", lit("0"))
  dfUI0900 = unitInstances0900Df.withColumn("PM", lit("1"))
  dfUI = dfUI0165.union(dfUI0900)
  #dfUI = df

  dfUIL0165 = unitInstanceLinks0165Df.selectExpr("UI_CODE_FROM", "UI_CODE_TO")
  dfUIL0900 = unitInstanceLinks0900Df.selectExpr("UI_CODE_FROM", "UI_CODE_TO")
  dfUIL = dfUIL0165.union(dfUIL0900).withColumn("RN", expr("ROW_NUMBER() OVER(PARTITION BY UI_CODE_TO ORDER BY UI_CODE_FROM)")).filter(col("RN") == 1).drop("RN")

  df = GeneralAliasDataFrameColumns(dfUI.where("CTYPE_CALENDAR_TYPE_CODE = 'COURSE' AND FES_STATUS NOT IN ('TGAREF', 'DRAFT')"), "ui_")
  dfUIL = GeneralAliasDataFrameColumns(dfUIL, "uil_")
  dfAvetCourse = GeneralAliasDataFrameColumns(refAvetmissCourseDf.select("CourseCodeConverted", "AvetmissCourseCode"), "rfac_")
 
  #Join Tables
  df = df.join(dfUIL, dfUIL.uil_UI_CODE_TO == df.ui_FES_UNIT_INSTANCE_CODE, how="left")

  #Select
  df = df.selectExpr(
    "ui_FES_UNIT_INSTANCE_CODE AS CourseCode"
    ,"ui_FES_LONG_DESCRIPTION AS CourseName"
    ,"ui_FES_STATUS AS CourseStatus"
    ,"ui_AWARD_CATEGORY_CODE AS QualificationTypeCode"
    ,"ui_NZSCED AS FieldOfEducationId"
    ,"ui_NATIONAL_COURSE_CODE as NationalCourseCode"
    ,"ui_UNIT_CATEGORY as CourseCategory"
    ,"ui_COURSE_OCCUPATION AS OccupationCode"
    ,"CASE \
       WHEN ui_NATIONAL_COURSE_CODE IS NOT NULL THEN ui_NATIONAL_COURSE_CODE \
       WHEN ui_CTYPE_CALENDAR_TYPE_CODE = 'COURSE' and LEFT(RIGHT(ui_FES_UNIT_INSTANCE_CODE, 3), 1) = 'V' \
               AND LEFT(RIGHT(ui_FES_UNIT_INSTANCE_CODE, 2), 1) IN ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9') THEN \
                      SUBSTRING(ui_FES_UNIT_INSTANCE_CODE, 1, CHAR_LENGTH(ui_FES_UNIT_INSTANCE_CODE)-3) \
       ELSE ui_FES_UNIT_INSTANCE_CODE \
     END AS CourseCodeConverted"
    ,"CASE \
        WHEN ui_UNIT_CATEGORY = 'TPQ' THEN uil_UI_CODE_FROM \
        ELSE NULL \
      END AS TrainingPackageNum"
    ,"ui_PM as PM" \
  )

  df = df.filter(df.CourseCodeConverted != '')
  df = df.filter(instr(df.CourseName, 'DO NOT USE') == 0)
  Order1 = "CASE \
            WHEN CourseCategory = 'GROUP' THEN 0 \
            WHEN CourseCategory IN ('RTOSS','NNRC') AND NationalCourseCode != '' AND CourseStatus='CURRENT' THEN 0 \
            WHEN CourseCategory IN ('TPQ','AC','TPSS') THEN 0 \
            ELSE  \
              CASE \
                WHEN CourseStatus='CURRENT' THEN 4 \
                WHEN CourseStatus='SUPERSEDED' THEN 3 \
                WHEN CourseStatus='OBSOLETE' THEN 2 \
                WHEN CourseStatus='DELETED' THEN 1 \
                ELSE 0 \
              END \
          END"
  df = df.withColumn("Order1", expr(Order1))
  df = df.withColumn("Order2", expr("CASE WHEN QualificationTypeCode = '' THEN 0 ELSE 1 END"))

  #De-Dup
  df = df.withColumn("RN", expr("ROW_NUMBER() OVER(PARTITION BY CourseCodeConverted ORDER BY Order1 DESC, CourseCode DESC, Order2 DESC)"))
  df = df.filter(df.RN == 1)

  df = df.withColumn("RN1", expr("ROW_NUMBER() OVER(PARTITION BY CourseCodeConverted ORDER BY Order1 DESC, PM DESC)"))
  df = df.filter(df.RN1 == 1)

  df = df.join(dfAvetCourse, dfAvetCourse.rfac_CourseCodeConverted == df.CourseCodeConverted, how="left")
  df = df.withColumn("AvetmissCourseCode", expr("CASE WHEN rfac_AvetmissCourseCode IS NOT NULL THEN rfac_AvetmissCourseCode ELSE LEFT(CourseCodeConverted,10) END"))

  df = df.withColumn("Order", expr(" \
      CASE \
        WHEN CourseStatus='CURRENT' THEN 4 \
        WHEN CourseStatus='SUPERSEDED' THEN 3 \
        WHEN CourseStatus='OBSOLETE' THEN 2 \
        WHEN CourseStatus='DELETED' THEN 1 \
        ELSE 0 \
      END"))

  #De-Duplication / ROW_NUMBER
  df = df.withColumn("RN2", expr("ROW_NUMBER() OVER(PARTITION BY AvetmissCourseCode ORDER BY Order DESC, PM DESC)"))
  df = df.filter(df.RN2 == 1)

  df = df.drop("Order", "Order1", "Order2", "RN", "RN1", "RN2", "PM", "rfac_CourseCodeConverted", "rfac_AvetmissCourseCode")
  
  df = df.withColumn("TP3", expr("left(TrainingPackageNum, 3)"))
  df = df.withColumn("CSE3", expr("left(CourseCode, 3)"))
  
  dfTPISC_TP = GeneralAliasDataFrameColumns(refTrainingPackageISCDf, "TP_")
  dfTPISC_CSE = GeneralAliasDataFrameColumns(refTrainingPackageISCDf, "CSE_")
  dfFOEISC = GeneralAliasDataFrameColumns(refFieldOfEducationISCDf, "FOE_")
  
  dfISC = GeneralAliasDataFrameColumns(refIndustirySkillsCouncilDf, "ISC_")
  
  df = df.join(dfTPISC_TP, df.TP3 == trim(dfTPISC_TP.TP_TrainingPackageSubstring), how="left")
  df = df.join(dfTPISC_CSE, df.CSE3 == trim(dfTPISC_CSE.CSE_TrainingPackageSubstring), how="left")
  df = df.join(dfFOEISC, df.FieldOfEducationId == trim(dfFOEISC.FOE_FieldOfEducationID), how="left")
  df = df.withColumn("ISC3", expr("coalesce(TP_IndustrySkillsCouncilID, CSE_IndustrySkillsCouncilID, FOE_IndustrySkillsCouncilID)"))
  
  df = df.join(dfISC, df.ISC3 == dfISC.ISC_IndustrySkillsCouncilID, how="left")
  
  df = df.selectExpr("AvetmissCourseCode", "CourseName", "NationalCourseCode", "OccupationCode", "QualificationTypeCode as AWARD", "left(FieldOfEducationId, 4) as FOE4", "ISC_IndustrySkillsCouncilName as ISC").distinct()

  return df

# COMMAND ----------

def _CourseEnrolmentEnhancement(courseEnrolmentDf, qualificationTypeMappingDf, stratificationGroupDf, locationDf, studentDf, fundingSourceCoreFundDf, fundingSourceDf, fundingSourceVetFeeHelpFundDf, vetFeeHelpFundDf, trainingPackageIscDf, fieldOfEducationIscDf, industirySkillsCouncilDf, deliveryModeDf, avetmissCourseSkillsPointDf, tsNswFundDf, tsNswFundGroupDf, welfareStatusDf, organisationTypeDf, qualificationTypeDf, qualificationGroupDf, fundingSourceSbiRulesDf, sbiSubCategoryDf, sbiCategoryDf, sbiGroupDf, feesListDf, feesListTempDf, webConfigDf, instalmentPlansDf, peopleUnitsDf, unitInstanceOccurrencesDf, unitInstancesDf, peopleDf, attainmentsDf, peopleUnitsSpecialDf, configurableStatusesDf, organisationUnitsDf, feesListWaiversDf, waiverValuesDf, unitInstances0165Df, unitInstances0900Df, uiLinks0165Df, uiLinks0900Df, fieldOfEducationDf, avetCourseDf, pulDf):
  ######################################################################################
  #Objective - enhance the course enrolments with the additional required columns
  #accepts the required dataframes to get the columns from to enhance course enrolments
  #join course enrolment with other dataframes to extract the required columns
  #returns the dataframe consisting course enrolments with the additional required columns
  ######################################################################################  
  df_course_enrolment = courseEnrolmentDf
  
  dfCourse = _Course(unitInstances0165Df, unitInstances0900Df, uiLinks0165Df, uiLinks0900Df, avetCourseDf, trainingPackageIscDf, fieldOfEducationIscDf, industirySkillsCouncilDf)
  
  #Adding ID columns
  df_course_enrolment = df_course_enrolment.withColumn("ID_PC", concat(col("ReportingYear"), col("InstituteId"), col("StudentId").cast('integer')))
  df_course_enrolment = df_course_enrolment.withColumn("ID_UIO", concat(col("ReportingYear"), col("InstituteId"), col("CourseOfferingId").cast('integer')))
  df_course_enrolment = df_course_enrolment.withColumn("ID_CPU", concat(col("ReportingYear"), col("InstituteId"), col("CourseOfferingEnrolmentId").cast('integer')))
  df_course_enrolment = df_course_enrolment.withColumn("ID_NCC", concat(col("ReportingYear"), col("CourseCodeConverted")))
  df_course_enrolment = df_course_enrolment.withColumn("ID_CE", concat(col("ReportingYear"), col("InstituteId"), col("DeliveryLocationCode"), col("StudentId").cast('integer'), col("AvetmissCourseCode")))
  
  #EDWDM-893 AQF
  dfRefQualificationTypeMapping = qualificationTypeMappingDf
  dfRQTM = GeneralAliasDataFrameColumns(dfRefQualificationTypeMapping, "qtm_")
  df_course_enrolment = df_course_enrolment.join(dfRQTM, df_course_enrolment.AWARD == dfRQTM.qtm_QualificationTypeID, how="left")

  df_course_enrolment = df_course_enrolment.withColumn("AQF", when(col("qtm_AQFGroupId")==1, 'AQF').when(col("qtm_AQFGroupId")==2, 'Non AQF').otherwise(lit('')))

  #EDWDM-953 TAFEDigitalArea
  #TNW
  TNW = "CASE WHEN SUBSTRING(CourseOfferingFunctionalUnit,1,4) = '9762' AND COALESCE(SUBSTRING(CourseOfferingFunctionalUnit,5,4), '') NOT IN ('2265','0288') then 1 ELSE 0 END"
  df_course_enrolment = df_course_enrolment.withColumn("TNW", expr(TNW))
  #Open
  #Open = "CASE WHEN SUBSTRING(CourseOfferingFunctionalUnit,5,4) = '0288' AND InstituteId = 160 THEN 1 else 0 end"
  Open = "CASE WHEN SUBSTRING(CourseOfferingFunctionalUnit,5,4) = '0288' AND DeliveryLocationCode IN ('TAR','KEM','GLA','CAS','BAL','MUW','LIS','NCO','B2B','CHE','MLN','GRA','KCL','COF','WCH','PMQ','YAM','TRN','WLB','MAK') THEN 1 else 0 end"
  df_course_enrolment = df_course_enrolment.withColumn("Open", expr(Open))
  #TafeDigital
  TafeDigital = "CASE WHEN TNW = 1 OR DeliveryLocationCode IN ('OTE') OR Open = 1 THEN 1 ELSE 0 end"
  df_course_enrolment = df_course_enrolment.withColumn("TafeDigital", expr(TafeDigital))
  #TOL 
  TOL = "case when OFFERING_CODE like '%TOL%' then 1 else 0 end"
  df_course_enrolment = df_course_enrolment.withColumn("TOL", expr(TOL))
  #OTEN
  OTEN = "case when DeliveryLocationCode = 'OTE' then 1 else 0 end"
  df_course_enrolment = df_course_enrolment.withColumn("OTEN", expr(OTEN))

  #Join with Student table to get the below columns from Student
  dfAS = GeneralAliasDataFrameColumns(studentDf, "AS_")
  df_course_enrolment = df_course_enrolment.join(dfAS, df_course_enrolment.StudentId == dfAS.AS_PersonCode, how="left")
  
  #EDWDM-891 Age
  age = "case \
          when YEAR(AS_DateOfBirth) >= 1903 and YEAR(AS_DateOfBirth) <= YEAR(ReportingYearEndDate) - 5 then \
            case \
              when MONTH(AS_DateOfBirth) <= 6 then YEAR(ReportingYearEndDate) - YEAR(AS_DateOfBirth) \
              when MONTH(AS_DateOfBirth) >= 7 and YEAR(AS_DateOfBirth) != YEAR(ReportingYearEndDate) - 5 then YEAR(ReportingYearEndDate) - YEAR(AS_DateOfBirth) - 1 \
              else 0 \
            end \
          when YEAR(AS_DateOfBirth) in (1901,1902,0) or YEAR(AS_DateOfBirth) >= YEAR(ReportingYearEndDate) - 4 or YEAR(AS_DateOfBirth) = '' or YEAR(AS_DateOfBirth) is null then 0 \
          else YEAR(ReportingYearEndDate) - YEAR(AS_DateOfBirth) \
        end"
  df_course_enrolment = df_course_enrolment.withColumn("Age", expr(age))

  #EDWDM-892 AgeGroup
  dfRefStratificationGroup = stratificationGroupDf
  dfRSG_AG = GeneralAliasDataFrameColumns(dfRefStratificationGroup, "ag_").where(col("ag_Stratification") == 'AgeGroup')
  df_course_enrolment = df_course_enrolment.join(dfRSG_AG, (df_course_enrolment.Age >= dfRSG_AG.ag_LowValue) & (df_course_enrolment.Age <= dfRSG_AG.ag_HighValue), how="left")

  #EDWDM-949 SimpleAgeGroup 
  #It goes in the view

  #EDWDM-894 ANZSCO - added in course-enrolment-all
  #EDWDM-895 APPRTRAINEE - added in course-enrolment-all

  #EDWDM-904 CommencementFlag
  df_course_enrolment = df_course_enrolment.withColumn("CommencementDate", expr("coalesce(CourseEnrolmentStartDateTransitionFrom, CourseEnrolmentStartDate)"))
  df_course_enrolment = df_course_enrolment.withColumn("CommencementFlag", expr("case when ProgramStartDate >= ReportingYear then 'Commencing' else 'Continuing' end"))

  #EDWDM-905 CommencementSemester
  #df_course_enrolment = df_course_enrolment.withColumn("CommencementSemester", expr("concat(YEAR(CommencementDate), ' ', case when MONTH(CommencementDate) <= 6 then 'SEM 1' else 'SEM 2' end)"))
  #PRADA-1640
  df_course_enrolment = df_course_enrolment.withColumn("CommencementSemester", 
                             expr("case \
                                     when left(ReportingYear, 2) = 'CY' and CommencementDate > ReportingYearEndDate then concat(YEAR(ReportingYearEndDate), ' SEM 2') \
                                     when left(ReportingYear, 2) = 'FY' and CommencementDate > ReportingYearEndDate then concat(YEAR(ReportingYearEndDate), ' SEM 1') \
                                     else concat(YEAR(CommencementDate), ' ', case when MONTH(CommencementDate) <= 6 then 'SEM 1' else 'SEM 2' end) \
                                   end"))

  #EDWDM-906 CommencementYear
  #df_course_enrolment = df_course_enrolment.withColumn("CommencementYear", expr("YEAR(ProgramStartDate)"))
  df_course_enrolment = df_course_enrolment.withColumn("CommencementYear", expr("case when CommencementDate > ReportingYearEndDate then YEAR(ReportingYearEndDate) else YEAR(CommencementDate) end"))

  #EDWDM-907 COMMITMENT_IDENTIFIER - added in course-enrolment-all

  #EDWDM-909 GRAD
  #df_course_enrolment = df_course_enrolment.withColumn("GRAD", expr("case when CourseAwardDate is not null and CourseAwardDescription != 'Withdrawn Award' and CourseAwardStatus = 'PUBLISHED' then 1 else 0 end"))
  df_course_enrolment = df_course_enrolment.withColumn("GRAD", expr("case when TotalGrad > 0 and ENRFLAG = 1 then 1 else 0 end"))
  df_course_enrolment = df_course_enrolment.withColumn("PreviousReportingYear", expr("concat(left(ReportingYear, 2), cast(right(ReportingYear, 4)-1 as int))"))
  refCutOffDateDf = DeltaTableAsCurrent("reference.avetmiss_reporting_year_cut_off_date")
  df_course_enrolment = df_course_enrolment.join(GeneralAliasDataFrameColumns(refCutOffDateDf, "cutoff_"), col("PreviousReportingYear") == col("cutoff_ReportingYear"), how="left") \
    .withColumn("PreviousReportingYearCutOffDate", expr("cast(coalesce(cutoff_CubeCutOffReportingDate, add_months(ReportingYearStartDate, 2)) as date)"))
  
  df_course_enrolment = df_course_enrolment.withColumn("GRAD", expr("case when GRAD = 1 and CourseEnrolmentEndDate < ReportingYearStartDate and CourseAwardDate <= PreviousReportingYearCutOffDate then 0 else GRAD end"))

  #EDWDM-910 YEAR_COMPLETE
  df_course_enrolment = df_course_enrolment.withColumn("YEAR_COMPLETE", expr("YEAR(CourseEnrolmentEndDate)"))

  #EDWDM-911 COREFND
  dfRefFundingSourceCoreFund = fundingSourceCoreFundDf
  dfRFSCF = GeneralAliasDataFrameColumns(dfRefFundingSourceCoreFund, "fscf_")
  df_course_enrolment = df_course_enrolment.join(dfRFSCF, df_course_enrolment.CourseFundingSourceCode == dfRFSCF.fscf_FundingSourceCode, how="left")
  df_course_enrolment = df_course_enrolment.withColumn("COREFND", when(col("fscf_CoreFundID")==1, 'Core').when(col("fscf_CoreFundID")==2, 'Non-Core').otherwise(lit('')))

  #Join with CourseMaster(EBCSE)
  dfCourse = GeneralAliasDataFrameColumns(dfCourse, "Course_")
  df_course_enrolment = df_course_enrolment.join(dfCourse, df_course_enrolment.AvetmissCourseCode == dfCourse.Course_AvetmissCourseCode, how="left")
  
  #CSENM
  df_course_enrolment = df_course_enrolment.withColumn("CSENM", col("Course_CourseName"))
  #FOE4
  df_course_enrolment = df_course_enrolment.withColumn("FOE4", col("Course_FOE4"))
  #ISC
  df_course_enrolment = df_course_enrolment.withColumn("ISC", col("Course_ISC"))
  
  #AVFUND
  df_course_enrolment = df_course_enrolment.withColumn("Fund", expr("case when coalesce(CourseFundingSourceCode, '') in ('', '000') then '001' else CourseFundingSourceCode end"))
  dfRFS = GeneralAliasDataFrameColumns(fundingSourceDf, "fs_")
  df_course_enrolment = df_course_enrolment.join(dfRFS, df_course_enrolment.Fund == dfRFS.fs_FundingSourceCode, how="left")
  df_course_enrolment = df_course_enrolment.withColumn("AVFUND", expr("case \
                                                                          when Fund IN ('00110') then 11 \
                                                                          when Fund IN ('843','001OS') then 30 \
                                                                          when DeliveryLocationCode = 'BRA' and Fund = '717' then 11 \
                                                                          when DeliveryLocationCode = 'BRA' and Fund <> '717' then 15 \
                                                                          else fs_FundingSourceNationalID \
                                                                        end"))
  df_course_enrolment = df_course_enrolment.withColumn("AVFUND", expr("case \
                                                                          when AVFUND in (97,98) then 13 \
                                                                          when coalesce(AVFUND, '') = '' then 0 \
                                                                          else AVFUND \
                                                                        end"))
  #FundingVFH
  dfRFSVFHF = GeneralAliasDataFrameColumns(fundingSourceVetFeeHelpFundDf, "fsvfhf_")
  df_course_enrolment = df_course_enrolment.join(dfRFSVFHF, df_course_enrolment.CourseFundingSourceCode == dfRFSVFHF.fsvfhf_FundingSourceCode, how="left")

  dfRefVETFeeHelpFund = vetFeeHelpFundDf
  dfRVFHF = GeneralAliasDataFrameColumns(dfRefVETFeeHelpFund, "vfhf_")
  df_course_enrolment = df_course_enrolment.join(dfRVFHF, df_course_enrolment.fsvfhf_VETFeeHelpFundID == dfRVFHF.vfhf_VETFeeHelpFundID, how="left")
  df_course_enrolment = df_course_enrolment.withColumn("FundingVFH", 
                                     expr("case \
                                             when coalesce(fsvfhf_VETFeeHelpFundID, '') = '' then \
                                               case \
                                                 when AVFUND in (11,13,15) then 'Government Subsidised delivery' \
                                                 when AVFUND in (20,30) then 'Commercial VFH/VSL' \
                                                 when AVFUND in (80) then 'Other Commercial delivery' \
                                               end \
                                             else vfhf_VETFeeHelpFundName \
                                           end"))

  #HIGHED
  df_course_enrolment = df_course_enrolment.withColumn("HIGHED", expr("case when substring(AvetmissCourseCode,1,2) = 'HE' then 'Yes - Higher Ed' else 'No - Higher Ed' end"))

  #HQUALN (HighestPreviousQualification)
  df_course_enrolment = df_course_enrolment.withColumn("HQUALN", col("AS_HighestPreviousQualification"))

  #SECED (HighestSchoolLevel)
  df_course_enrolment = df_course_enrolment.withColumn("SECED", col("AS_HighestSchoolLevel"))

  #EDWDM-938 OfferingType -- added in course-enrolment-all

  #EDWDM-1488 ARIA11B
  df_course_enrolment = df_course_enrolment.withColumn("ARIA11B", expr("case when AVFUND in (31, 32) then 'OVERSEAS' else AS_ARIA11B end"))
  
  #RIND11
  RIND11 = "case when ARIA11B IN ('INNER REGIONAL', 'OUTER REGIONAL', 'REMOTE', 'VERY REMOTE') THEN 'REGIONAL/REMOTE' ELSE 'NOT REGIONAL/REMOTE' end"
  df_course_enrolment = df_course_enrolment.withColumn("RIND11", expr(RIND11))

  #EDWDM-921 EnrolmentTeachingSectionName - added in course-enrolment-all

  #EDWDM-924 FullySubsidised
  FullySubsidised = "case when CourseFundingSourceCode IN ('001J', '001T') then 'Yes - Fully Subsidised' else 'No - Fully Subsidised' end"
  df_course_enrolment = df_course_enrolment.withColumn("FullySubsidised", expr(FullySubsidised))

  #EDWDM-936 LongTermUnemployed
  LongTermUnemployed = "case when HasLongTermUnemploymentEvidence is not null then 'Y' else 'N' end"
  df_course_enrolment = df_course_enrolment.withColumn("LongTermUnemployed", expr(LongTermUnemployed))

  #EDWDM-939 OnlineUnits
  OnlineUnits = "case when ElectronicDeliveryUnitCount > 0 or BlendedDeliveryUnitCount > 0 then 'Yes - Online Units' else 'No - Online Units' end"
  df_course_enrolment = df_course_enrolment.withColumn("OnlineUnits", expr(OnlineUnits))

  #EDWDM-940 PredominantDeliveryMode
  UnitsDeliveryMode = "case \
                          when UnitCount=0 then 'NONE'\
                          when CreditTransferUnitCount=UnitCount then 'CTOD' \
                          when RPLUnitCount=UnitCount then 'RPLO' \
                          when InClassDeliveryUnitCount=DeliveredUnitCount then 'CLAS' \
                          when DistanceDeliveryUnitCount=DeliveredUnitCount then 'DIST' \
                          when ElectronicDeliveryUnitCount=DeliveredUnitCount then 'ELEC' \
                          when BlendedDeliveryUnitCount=DeliveredUnitCount then 'BLND' \
                          when SelfDeliveryUnitCount=DeliveredUnitCount then 'SELF' \
                          when OnTheJobTrainingDeliveryUnitCount=DeliveredUnitCount then 'ONJB' \
                          when OnTheJobDistanceDeliveryUnitCount=DeliveredUnitCount then 'ONJD' \
                          when SimulatedDeliveryUnitCount=DeliveredUnitCount then 'SIMU' \
                          when MissingDeliveryModeUnitCount=DeliveredUnitCount then 'MISS' \
                          when OtherDeliveryUnitCount=DeliveredUnitCount then 'OTHR' \
                          when ElectronicDeliveryUnitCount+BlendedDeliveryUnitCount=DeliveredUnitCount then 'ONLM' \
                          else 'MIXD' \
                        end"
  df_course_enrolment = df_course_enrolment.withColumn("UnitsDeliveryMode", expr(UnitsDeliveryMode))

  dfRefDeliveryMode = deliveryModeDf
  dfRDM = GeneralAliasDataFrameColumns(dfRefDeliveryMode, "dm_")
  df_course_enrolment = df_course_enrolment.join(dfRDM, df_course_enrolment.UnitsDeliveryMode == dfRDM.dm_DeliveryModeCode, how="left")

  #EDWDM-950 SKILLSPOINT
  dfRefCourseSkillsPoint = avetmissCourseSkillsPointDf
  dfRCSP = GeneralAliasDataFrameColumns(dfRefCourseSkillsPoint, "csp_")
  df_course_enrolment = df_course_enrolment.join(dfRCSP, df_course_enrolment.AvetmissCourseCode == dfRCSP.csp_AvetmissCourseCode, how="left")
  df_course_enrolment = df_course_enrolment.withColumn("SKILLSPOINT", expr("coalesce(csp_SkillsPointID, 99)"))

  #EDWDM-951 SponsorCode
  dfOrgUnits = organisationUnitsDf
  dfOU = GeneralAliasDataFrameColumns(dfOrgUnits, "ou_").where(col("ou_ORGANISATION_TYPE") == 'SPONSOR')
  df_course_enrolment = df_course_enrolment.join(dfOU, df_course_enrolment.SponsorOrginationCode == dfOU.ou_ORGANISATION_CODE, how="left")
  SponsorCode = "concat(ou_ORGANISATION_CODE, ' ', ou_FES_FULL_NAME)"
  df_course_enrolment = df_course_enrolment.withColumn("SponsorCode", expr(SponsorCode))

  #EDWDM-959 TrainingContractID - added in course-enrolment-all

  #EDWDM-960 TSNSWFundDescription
  dfRefTSNSWFund = tsNswFundDf
  dfRTSNSWF = GeneralAliasDataFrameColumns(dfRefTSNSWFund, "tsnswf_")
  df_course_enrolment = df_course_enrolment.join(dfRTSNSWF, df_course_enrolment.CourseFundingSourceCode == dfRTSNSWF.tsnswf_FundingSourceCode, how="left")

  dfRefTSNSWFundGroup = tsNswFundGroupDf
  dfRTSNSWFG = GeneralAliasDataFrameColumns(dfRefTSNSWFundGroup, "tsnswfg_")
  df_course_enrolment = df_course_enrolment.join(dfRTSNSWFG, df_course_enrolment.fs_FundingSourceNationalID == dfRTSNSWFG.tsnswfg_FundingSourceNationalID, how="left")

  df_course_enrolment = df_course_enrolment.withColumn("TSNSWFundDescription", expr("coalesce(tsnswf_TSNSWFundDescription, tsnswfg_TSNSWFundGroupDescription)"))

  #EDWDM-961 TSNSWFundGroupDescription - tsnswfg_TSNSWFundGroupDescription
  df_course_enrolment = df_course_enrolment.withColumn("TSNSWFundGroupDescription", col("tsnswfg_TSNSWFundGroupDescription"))

  #EDWDM-962 UnitOnlyFlag
  UnitOnlyFlag = "CASE WHEN AvetmissCourseCode LIKE '%-999999' THEN 'Unit Only Enrolment' ELSE 'Course Enrolment' END"
  df_course_enrolment = df_course_enrolment.withColumn("UnitOnlyFlag", expr(UnitOnlyFlag))

  #EDWDM-963 WaiverCodes
  dfFL = GeneralAliasDataFrameColumns(feesListDf, "FL_")
  dfFLW = GeneralAliasDataFrameColumns(feesListWaiversDf, "FLW_")
  dfWV = GeneralAliasDataFrameColumns(waiverValuesDf, "WV_")
  dfPUL = GeneralAliasDataFrameColumns(pulDf, "PUL_")
  
  df_waiver_codes1 = dfFL.join(dfPUL, col("FL_RUL_CODE") == col("PUL_CHILD_ID"), how="left") \
    .join(dfFLW, col("FL_FEE_RECORD_CODE") == col("FLW_FEE_RECORD_CODE"), how="inner") \
    .join(dfWV, col("FLW_WAIVER_VALUE_NUMBER") == col("WV_WAIVER_VALUE_NUMBER"), how="inner") \
    .filter((col("FL_FES_RECORD_TYPE") == "F") & (col("FL_STATUS") == "F") & (col("FL_FES_WHO_TO_PAY") == "LEARNER")) \
    .selectExpr("PUL_PARENT_ID AS RUL_CODE", "WV_WAIVER_CODE AS WAIVER_CODE").distinct()
  
  df_waiver_codes2 = dfFL.join(dfFLW, col("FL_FEE_RECORD_CODE") == col("FLW_FEE_RECORD_CODE"), how="inner") \
    .join(dfWV, col("FLW_WAIVER_VALUE_NUMBER") == col("WV_WAIVER_VALUE_NUMBER"), how="inner") \
    .filter((col("FL_FES_RECORD_TYPE") == "F") & (col("FL_STATUS") == "F") & (col("FL_FES_WHO_TO_PAY") == "LEARNER")) \
    .selectExpr("FL_RUL_CODE AS RUL_CODE", "WV_WAIVER_CODE AS WAIVER_CODE").distinct()

  df_waiver_codes = df_waiver_codes1.union(df_waiver_codes2).distinct() \
    .groupBy("RUL_CODE") \
    .agg(concat_ws(", ", sort_array(collect_list("WAIVER_CODE"))).alias("WAIVER_CODE"))

  df_course_enrolment = df_course_enrolment.join(df_waiver_codes, df_course_enrolment.CourseOfferingEnrolmentId == df_waiver_codes.RUL_CODE, how="left")


  #EDWDM-964 WelfareStatusDescription
  dfRefWelfareStatus = welfareStatusDf
  dfRWS = GeneralAliasDataFrameColumns(dfRefWelfareStatus, "ws_")
  df_course_enrolment = df_course_enrolment.join(dfRWS, df_course_enrolment.WelfareStatus == dfRWS.ws_WelfareStatusID, how="left")

  #EDWDM-965 WhoToPay - added in course-enrolment-all

  #REGION
  dfRefLocation = locationDf
  dfRL = GeneralAliasDataFrameColumns(dfRefLocation, "l_")
  df_course_enrolment = df_course_enrolment.join(dfRL, df_course_enrolment.DeliveryLocationCode == dfRL.l_LocationCode, how="left")
  df_course_enrolment = df_course_enrolment.withColumn("REGION", col("l_RegionName"))

  #TransitionFlag
  TransitionFlag = "case when CourseCodeTransitionFrom is not null then 'Y' else 'N' end"
  df_course_enrolment = df_course_enrolment.withColumn("TransitionFlag", expr(TransitionFlag))

  #EDWDM-989 FeeHelpFlag
  FeeHelpFlag = "case when UI_UNIT_CATEGORY = 'HES'  THEN  'Y' else 'N' end"
  df_course_enrolment = df_course_enrolment.withColumn("FeeHelpFlag", expr(FeeHelpFlag))

  #EDWDM-991 VSLFlag
  VSLFlag = "case when CourseEnrolmentOfferingType in ('VFH','SMARTVSL','VSL') then 'Y' else 'N' end"
  df_course_enrolment = df_course_enrolment.withColumn("VSLFlag", expr(VSLFlag))

  #EDWDM-980 WorksInNSW - added in course-enrolment-all

  #EDWDM-978 OrganisationTypeDescription
  dfOrgUnitType = GeneralAliasDataFrameColumns(organisationUnitsDf, "out_")
  dfROT = GeneralAliasDataFrameColumns(organisationTypeDf, "rot_")
  
  df_course_enrolment = df_course_enrolment.join(dfOrgUnitType, df_course_enrolment.PU_ORGANISATION_CODE == dfOrgUnitType.out_ORGANISATION_CODE, how="left")
  df_course_enrolment = df_course_enrolment.join(dfROT, df_course_enrolment.out_ORGANISATION_TYPE == dfROT.rot_OrganisationTypeCode, how="left")
  df_course_enrolment = df_course_enrolment.withColumn("OrganisationTypeDescription", col("rot_OrganisationTypeDescription"))

  #EDWDM-979 EmployerPostcode - added in course-enrolment-all
  #EmployerSuburb - added in course-enrolment-all

  #EDWDM-968 QualificationTypeName
  dfRefQualificationType = qualificationTypeDf
  dfRQT = GeneralAliasDataFrameColumns(dfRefQualificationType, "qt_")
  df_course_enrolment = df_course_enrolment.join(dfRQT, df_course_enrolment.AWARD == dfRQT.qt_QualificationTypeID, how="left")

  #EDWDM-899 AwardGroupCode
  dfRefQualificationGroup = qualificationGroupDf
  dfRQG = GeneralAliasDataFrameColumns(dfRefQualificationGroup, "qg_")
  df_course_enrolment = df_course_enrolment.join(dfRQG, df_course_enrolment.qtm_QualificationGroupID == dfRQG.qg_QualificationGroupID, how="left")

  #EDWDM-981 SBICategory
  #EDWDM-982 SBIGroup
  #EDWDM-983 SBISubCategory

  dfRefSBIRules = fundingSourceSbiRulesDf
  dfSBIRules = GeneralAliasDataFrameColumns(dfRefSBIRules, "sbirules_")
  df_course_enrolment = df_course_enrolment.join(dfSBIRules, df_course_enrolment.CourseFundingSourceCode == dfSBIRules.sbirules_FundingSourceCode, how="left")
  SBISubCategoryID = " \
  case \
    when sbirules_Rule1Value is not null and array_contains(split(lower(sbirules_Rule1_FES_UNINS_INSTANCE_CODE), ','), left(lower(CourseCode), 2)) and array_contains(split(sbirules_Rule1_FundingSourceNationalID, ','), fs_FundingSourceNationalID) \
    then sbirules_Rule1Value \
    when sbirules_Rule2Value is not null and array_contains(split(lower(sbirules_Rule2_SLOC_LOCATION_CODE), ','), lower(DeliveryLocationCode)) and array_contains(split(sbirules_Rule2_FundingSourceNationalID, ','), fs_FundingSourceNationalID) \
    then sbirules_Rule2Value \
    when sbirules_Rule3Value is not null and array_contains(split(lower(sbirules_Rule3_USER_1), ','), lower(APPRTRAINEE)) then sbirules_Rule3Value \
    when sbirules_Rule4Value is not null and array_contains(split(sbirules_Rule4_QualificationGroupID, ','), CAST(qtm_QualificationGroupID AS VARCHAR(10))) then sbirules_Rule4Value \
    when sbirules_Rule5Value is not null and array_contains(split(sbirules_Rule5_QualificationGroupID, ','), CAST(qtm_QualificationGroupID AS VARCHAR(10))) then sbirules_Rule5Value \
    when sbirules_Rule6Value is not null and array_contains(split(sbirules_Rule6_QualificationGroupID, ','), CAST(qtm_QualificationGroupID AS VARCHAR(10))) then sbirules_Rule6Value \
    when sbirules_Rule7_Value is not null and YEAR(CourseEnrolmentStartDate) < sbirules_Rule7_CourseEnrolmentStartDateBeforeYear then sbirules_Rule7_Value \
    when sbirules_Rule8Value is not null and array_contains(split(lower(sbirules_Rule8_NationalCourseCode), ','), lower(Course_AvetmissCourseCode)) then sbirules_Rule8Value /*PRADA-1681*/ \
    when sbirules_Rule9Value is not null \
      and array_contains(split(lower(sbirules_Rule9_SBIClassification), ','), lower(sbirules_SBIClassification)) \
      and \
      ( \
        array_contains(split(lower(sbirules_Rule9_Who_To_Pay), ','), lower(WhoToPay)) \
        OR array_contains(split(lower(sbirules_Rule9_SPONSOR_ORG_CODE), ','), lower(SponsorOrginationCode)) \
      ) \
      and array_contains(split(sbirules_Rule9_FundingSourceNationalID, ','), fs_FundingSourceNationalID) \
      and array_contains(split(sbirules_Rule9_AQFGroupID, ','), CAST(qtm_AQFGroupID AS VARCHAR(10))) \
    then sbirules_Rule9Value \
    when sbirules_Rule10Value is not null  \
      and array_contains(split(sbirules_Rule10_SBIClassification, ','), sbirules_SBIClassification) \
      and  \
      ( \
        array_contains(split(lower(sbirules_Rule10_Who_To_Pay), ','), lower(WhoToPay)) \
        OR array_contains(split(lower(sbirules_Rule10_SPONSOR_ORG_CODE), ','), lower(SponsorOrginationCode)) \
      ) \
      and array_contains(split(sbirules_Rule10_FundingSourceNationalID, ','), fs_FundingSourceNationalID) \
      and array_contains(split(sbirules_Rule10_AQFGroupID, ','), CAST(qtm_AQFGroupID AS VARCHAR(10))) \
    then sbirules_Rule10Value \
    when sbirules_SBISubCategoryID is not null then sbirules_SBISubCategoryID \
    else '99' \
  end "

  df_course_enrolment = df_course_enrolment.withColumn("SBISubCategoryID", expr(SBISubCategoryID))

  dfRefSBISubCategory = sbiSubCategoryDf
  dfRSBISubCat = GeneralAliasDataFrameColumns(dfRefSBISubCategory, "sbisubcat_")
  df_course_enrolment = df_course_enrolment.join(dfRSBISubCat, df_course_enrolment.SBISubCategoryID == dfRSBISubCat.sbisubcat_SBISubCategoryID, how="left")
  dfRefSBICategory = sbiCategoryDf
  dfRSBICat = GeneralAliasDataFrameColumns(dfRefSBICategory, "sbicat_")
  df_course_enrolment = df_course_enrolment.join(dfRSBICat, df_course_enrolment.sbisubcat_SBICategoryID == dfRSBICat.sbicat_SBICategoryID, how="left")
  dfRefSBIGroup = sbiGroupDf
  dfRSBIGroup = GeneralAliasDataFrameColumns(dfRefSBIGroup, "sbigroup_")
  df_course_enrolment = df_course_enrolment.join(dfRSBIGroup, df_course_enrolment.sbicat_SBIGroupID == dfRSBIGroup.sbigroup_SBIGroupID, how="left")
  #-----------------------------------------------

    #dedup
  df_course_enrolment = df_course_enrolment.withColumn("rn", expr("row_number() over(partition by ReportingYear, AvetmissClientId, AvetmissCourseCode, DeliveryLocationCode, ValidFlag order by coalesce(GraduateFlag, 0) desc, coalesce(CourseProgressRank, 9), PassGradeUnitCount desc, ValidUnitExistsFlag desc, CourseProgressDate desc, CourseCaloccCode desc)")).filter(col("rn") == 1).drop("rn")
  
  #Apply further rules
  df_course_enrolment = df_course_enrolment.withColumn("AWARD", expr("case when coalesce(Course_AWARD, '') = '' or AvetmissCourseCode like '%-999999' then '99' else Course_AWARD end"))
  df_course_enrolment = df_course_enrolment.withColumn("CSENM", expr("case when AvetmissCourseCode like '%-999999' then 'MISSING COURSE CODE' else UPPER(CSENM) end"))
  df_course_enrolment = df_course_enrolment.withColumn("AVETCOUNT", expr("case when AWARD = '99' then 0 else AVETCOUNT end"))  
  
  dfCourseEnrolmentFinal = df_course_enrolment.selectExpr(
   "ReportingYear"
  ,"CourseOfferingEnrolmentId"
  ,"AvetmissCourseCode"
  ,"DeliveryLocationCode"
  ,"AvetmissClientId"
  ,"InstituteId"
  ,"AvetmissClientIdMissingFlag"
  ,"StudentId"
  ,"CourseOfferingId"
  ,"CourseCaloccCode"
  ,"CourseCode"
  ,"Course_NationalCourseCode as NationalCourseCode"
  ,"CourseCodeConverted"
  ,"CourseEnrolmentStartDate"
  ,"CourseEnrolmentEndDate"
  ,"CourseProgressCode"
  ,"CourseProgressStatus"
  ,"CourseProgressReason"
  ,"CourseProgressDate"
  ,"CourseAwardDate"
  ,"CourseAwardDescription"
  ,"CourseAwardStatus"
  ,"CourseFundingSourceCode"
  ,"fs_FundingSourceNationalID as FundingSourceNationalID"
  ,"CourseEnrolmentIdTransitionFrom"
  ,"CourseCodeTransitionFrom"
  ,"CourseCaloccCodeTransitionFrom"
  ,"CourseEnrolmentStartDateTransitionFrom"
  ,"CourseOfferingRemarks"
  ,"CourseOfferingFunctionalUnit"
  ,"ReportingYearStartDate"
  ,"ReportingYearEndDate"
  ,"CourseAwardFlag"
  ,"case when GraduateFlag = 1 then 'Y' else 'N' end as GraduateFlag"
  ,"CourseProgressRank"
  ,"SubsidisedAmount"
  ,"StandardSubsidyAmount"
  ,"NeedsLoadingAmount"
  ,"LocationLoadingAmount"
  ,"APPRTRAINEE"
  ,"CommitmentIdentifier"
  ,"EnrolmentTeachingSectionName"
  ,"HasLongTermUnemploymentEvidence"
  ,"CourseEnrolmentOfferingType"
  ,"TrainingContractID"
  ,"WhoToPay"
  ,"EmployerPostcode"
  ,"EmployerSuburb"
  ,"WorksInNSW"
  ,"VFHCourse"
  ,"OriginalFee"
  ,"ActualFee"
  ,"FeePaid"
  ,"FeeInstalment"
  ,"FeeAdjustment"
  ,"FeeOutstanding"
  ,"ProgramStartDate"
  ,"TotalHours"
  ,"RPLHours"
  ,"TrainingHours"
  ,"ContinuingHours"
  ,"ValidUnitCount"
  ,"UnitCount"
  ,"FinalGradeUnitCount"
  ,"PassGradeUnitCount"
  ,"WithdrawnUnitCount"
  ,"FailGradeUnitCount"
  ,"RPLUnitCount"
  ,"CreditTransferUnitCount"
  ,"NonAssessableUnitCount"
  ,"MissingGradeUnitCount"
  ,"ContinuingUnitCount"
  ,"RPLGrantedUnitCount"
  ,"RPLNotGrantedUnitCount"
  ,"CreditTransferNationalRecognitionUnitCount"
  ,"SupersededUnitCount"
  ,"NonAssessableSatisfactorilyCompletedUnitCount"
  ,"NonAssessableNotSatisfactorilyCompletedUnitCount"
  ,"PayableUnitCount"
  ,"Semester1UnitCount"
  ,"Semester2UnitCount"
  ,"Semester3UnitCount"
  ,"TotalSemester1Hours as Semester1Hours"
  ,"TotalSemester2Hours as Semester2Hours"
  ,"ExemptedDeliveryUnitCount"
  ,"InClassDeliveryUnitCount"
  ,"DistanceDeliveryUnitCount"
  ,"ElectronicDeliveryUnitCount"
  ,"BlendedDeliveryUnitCount"
  ,"SelfDeliveryUnitCount"
  ,"OnTheJobTrainingDeliveryUnitCount"
  ,"OnTheJobDistanceDeliveryUnitCount"
  ,"SimulatedDeliveryUnitCount"
  ,"MissingDeliveryModeUnitCount"
  ,"OtherDeliveryUnitCount"
  ,"DeliveredUnitCount"
  ,"ValidFlag"
  ,"ValidUnitExistsFlag"
  ,"PayableUnitExistsFlag"
  ,"ShellEnrolmentFlag"
  ,"ID_PC"
  ,"ID_UIO"
  ,"ID_CPU"
  ,"ID_NCC"
  ,"ID_CE"
  ,"TafeDigital"
  ,"OTEN"
  ,"TOL"
  ,"TNW"
  ,"OPEN"
  ,"Age"
  ,"ag_Group as AgeGroup"
  ,"Course_OccupationCode as ANZSCO"
  ,"ARIA11B"
  ,"AWARD"
  ,"CommencementFlag"
  ,"CommencementSemester"
  ,"CommencementYear"
  ,"GRAD"
  ,"YEAR_COMPLETE"
  ,"COREFND"
  ,"AVETCOUNT"
  ,"ENRFLAG"
  ,"AvetmissFlag"
  ,"CSENM"
  ,"FOE4"
  ,"AVFUND"
  ,"FundingVFH"
  ,"HIGHED"
  ,"HQUALN"
  ,"SECED"
  ,"ISC"
  ,"case when AVFUND in (30, 31, 32) then '_OFF-SHORE/MIGRA' else AS_LGA_Name end as LGANAM"
  ,"RIND11"
  ,"SponsorOrginationCode"
  ,"SponsorCode"
  ,"AS_AusState as SA_STATE"
  ,"AS_SA4NAME as SA4_NAME"
  ,"AS_SA2NAME as SA2_NAME"
  ,"AS_SEIFADecile as Decile"
  ,"AS_SEIFAPercentile as Percentile"
  ,"WelfareStatus" 
  ,"UnitsDeliveryMode"
  ,"SkillsPoint"
  ,"StudyReason"
  ,"CTOnlyFlag as CTOnly" 
  ,"Region"
  ,"TransitionFlag"
  ,"FeeHelpFlag"
  ,"VSLFlag"  
  ,"sbigroup_SBIGroupName as SBIGroup"
  ,"sbicat_SBICategoryName as SBICategory"
  ,"sbisubcat_SBISubCategory as SBISubCategory"
  ,"tsnswf_TSNSWFundCode as TSNSWFundCode"
  ,"tsnswfg_TSNSWFundGroupCode as TSNSWFundGroupCode"
  ,"WAIVER_CODE as WaiverCodes"
  ,"OrganisationTypeDescription"
  ,"PredominantDeliveryMode"
  ,"SubsidyStatus"
  ,"AwardID"
  ,"IssuedFlag"
  ,"ParchmentIssueDate"
  ,"DateConferred"
  ,"ConfigurableStatusID"
  ,"AttainmentCreatedDate"
  ,"AttainmentUpdatedDate"
  ,"ProgramStream"
  ,"CASE WHEN TafeDigital = 1 AND (OTEN = 0 or OTEN is null) AND TOL = 1 THEN DeliveryLocationCode + '-TOL' WHEN TafeDigital = 1 AND (OTEN = 0 or OTEN is null) AND TNW = 1 THEN DeliveryLocationCode + '-TNW' WHEN TafeDigital = 1 AND (OTEN = 0 or OTEN is null) AND `OPEN` = 1 THEN DeliveryLocationCode + '-OPEN' ELSE DeliveryLocationCode END AS CourseDeliveryLocation2"
  ,"ReportingDate"
  ,"EnrolCountPU"
  ).dropDuplicates()

  return dfCourseEnrolmentFinal

# COMMAND ----------

###########################################################################################################################
# Function: GetAvetmissCourseEnrolment
#  GETS COURSE ENROLMENT DIMENSION
# Parameters (LOTS OF EM): 
#  unitEnrolmentDf = compliance.unit_enrolment
#  locationDf = compliance.location
#  studentDf = compliance.student
#  courseDf = compliance.course
#  dateDf = reference.date
#  avetmissCourseDf = reference.avetmiss_course
#  qualificationTypeMappingDf = reference.qualification_type_mapping
#  stratificationGroupDf = reference.stratification_group
#  fundingSourceCoreFundDf = reference.funding_source_core_fund
#  fundingSourceDf = reference.funding_source
#  fundingSourceVetFeeHelpFundDf = reference.funding_source_vet_fee_help_fund
#  vetFeeHelpFundDf = reference.vet_fee_help_fund
#  trainingPackageIscDf = reference.training_package_isc
#  fieldOfEducationIscDf = reference.field_of_education_isc
#  industirySkillsCouncilDf = reference.industry_skills_council
#  deliveryModeDf = reference.delivery_mode
#  avetmissCourseSkillsPointDf = reference.avetmiss_course_skills_point
#  tsNswFundDf = reference.ts_nsw_fund
#  tsNswFundGroupDf = reference.ts_nsw_fund_group
#  welfareStatusDf = reference.welfare_status
#  organisationTypeDf = reference.organisation_type
#  qualificationTypeDf = reference.qualification_type
#  qualificationGroupDf = reference.qualification_group
#  fundingSourceSbiRulesDf = reference.funding_source_sbi_rules
#  sbiSubCategoryDf = reference.sbi_sub_category
#  sbiCategoryDf = reference.sbi_category
#  sbiGroupDf = reference.sbi_group
#  feesListDf = trusted.OneEBS_EBS_0165_FEES_LIST
#  feesListTempDf = trusted.OneEBS_EBS_0165_FEES_LIST_TEMP
#  webConfigDf = trusted.OneEBS_EBS_0165_web_config
#  instalmentPlansDf = trusted.OneEBS_EBS_0165_INSTALMENT_PLANS
#  peopleUnitsDf = trusted.OneEBS_EBS_0165_people_units
#  unitInstanceOccurrencesDf = trusted.OneEBS_EBS_0165_unit_instance_occurrences
#  unitInstancesDf = trusted.OneEBS_EBS_0165_unit_instances
#  peopleDf = trusted.oneEBS_EBS_0165_PEOPLE
#  attainmentsDf = trusted.OneEBS_EBS_0165_ATTAINMENTS
#  peopleUnitsSpecialDf = trusted.OneEBS_EBS_0165_PEOPLE_UNITS_SPECIAL
#  configurableStatusesDf = trusted.oneEBS_EBS_0165_configurable_statuses
#  organisationUnitsDf = trusted.oneEBS_EBS_0165_organisation_units
#  feesListWaiversDf = trusted.oneebs_ebs_0165_fees_list_waivers
#  waiverValuesDf = trusted.oneebs_ebs_0165_waiver_values
#  awardPrintedDetailsDf = trusted.oneebs_ebs_0165_awards_printed_details
# Returns:
#  Dataframe of transformed Course Enrolment
#############################################################################################################################
def GetAvetmissCourseEnrolment(unitEnrolmentDf, locationDf, studentDf, currentReportingPeriodDf, avetmissCourseDf, qualificationTypeMappingDf, stratificationGroupDf, fundingSourceCoreFundDf, fundingSourceDf, fundingSourceVetFeeHelpFundDf, vetFeeHelpFundDf, trainingPackageIscDf, fieldOfEducationIscDf, industirySkillsCouncilDf, deliveryModeDf, avetmissCourseSkillsPointDf, tsNswFundDf, tsNswFundGroupDf, welfareStatusDf, organisationTypeDf, qualificationTypeDf, qualificationGroupDf, fundingSourceSbiRulesDf, sbiSubCategoryDf, sbiCategoryDf, sbiGroupDf, feesListDf, feesListTempDf, webConfigDf, instalmentPlansDf, peopleUnitsDf, unitInstanceOccurrencesDf, unitInstancesDf, peopleDf, attainmentsDf, peopleUnitsSpecialDf, configurableStatusesDf, organisationUnitsDf, feesListWaiversDf, waiverValuesDf, awardPrintedDetailsDf, unitInstances0165Df, unitInstances0900Df, uiLinks0165Df, uiLinks0900Df, fieldOfEducationDf, feeTypesDf, whoToPayDf, feeValuesDf, pulDf, RefreshLinkSP):
  
  spark.conf.set("spark.sql.crossJoin.enabled", "true")  
  
  # ==================
  # STEP 1: Build a dataframe containing course enrolments rolled up from unit enrolments by period
  # ==================
  courseEnrolmentRollupDf = _CourseEnrolmentRollup(unitEnrolmentDf)
  
  # ==================
  # STEP 2: Build a dataframe containing all course enrolments from ebs tables, with Fee related columns 
  # ==================
  courseEnrolmentAllDf = _CourseEnrolmentAll(feesListDf, instalmentPlansDf, feesListTempDf, peopleUnitsDf, peopleDf, attainmentsDf, unitInstanceOccurrencesDf, unitInstancesDf, peopleUnitsSpecialDf, configurableStatusesDf, locationDf, avetmissCourseDf, organisationUnitsDf, awardPrintedDetailsDf, webConfigDf, feeTypesDf, whoToPayDf, feeValuesDf, pulDf, RefreshLinkSP)
  
  # ==================
  # STEP 3: Get shell enrolments by reporting year, course enrolment id, avetmiss course code, course delivery location
  # ==================
  courseEnrolmentShellDf = _CourseEnrolmentShell(courseEnrolmentAllDf, courseEnrolmentRollupDf, currentReportingPeriodDf)
  
  # ==================
  # STEP 4: Build the data frame for course enrolments
  # ==================
  courseEnrolmentDf = _CourseEnrolment(courseEnrolmentAllDf, courseEnrolmentRollupDf, courseEnrolmentShellDf)

  # ==================
  # STEP 5: enhance the data frame for course enrolments
  # ==================
  courseEnrolmentEnhancementDf = _CourseEnrolmentEnhancement(courseEnrolmentDf, qualificationTypeMappingDf, stratificationGroupDf, locationDf, studentDf, fundingSourceCoreFundDf, fundingSourceDf, fundingSourceVetFeeHelpFundDf, vetFeeHelpFundDf, trainingPackageIscDf, fieldOfEducationIscDf, industirySkillsCouncilDf, deliveryModeDf, avetmissCourseSkillsPointDf, tsNswFundDf, tsNswFundGroupDf, welfareStatusDf, organisationTypeDf, qualificationTypeDf, qualificationGroupDf, fundingSourceSbiRulesDf, sbiSubCategoryDf, sbiCategoryDf, sbiGroupDf, feesListDf, feesListTempDf, webConfigDf, instalmentPlansDf, peopleUnitsDf, unitInstanceOccurrencesDf, unitInstancesDf, peopleDf, attainmentsDf, peopleUnitsSpecialDf, configurableStatusesDf, organisationUnitsDf, feesListWaiversDf, waiverValuesDf, unitInstances0165Df, unitInstances0900Df, uiLinks0165Df, uiLinks0900Df, fieldOfEducationDf, avetmissCourseDf, pulDf)
  
  return courseEnrolmentEnhancementDf
