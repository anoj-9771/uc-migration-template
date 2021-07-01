# Databricks notebook source
def GetNatBaseErpt(params, exclusionList=True):
  #this definition will be used for returning the base course enrolments to the master notebook.
  NAT_Dates = spark.read.json(sc.parallelize([params]))
  CollectionStart = params["CollectionStartERpt"]
  CollectionEnd = params["CollectionEndERpt"]
  ReportingYear = params["ReportingYearERpt"]
  CollectionEndYear = (datetime.strptime(CollectionEnd, '%Y-%m-%d')).year

  qry="SELECT '90003' as TRAINING_ORGANISATION_ID, \
        PU_COURSE.commitment_identifier as PURCHASE_CON_ID, \
        UIO_COURSE.SLOC_LOCATION_CODE,\
        SPLIT(COALESCE(PEO.REGISTRATION_NO,cast(PU_COURSE.PERSON_CODE as string)), '[.]')[0] REGISTRATION_NO /*PRADA-1616*/, \
        PU_COURSE.PERSON_CODE, \
        UIO_COURSE.UIO_ID, \
        PU_COURSE.ID AS CourseOfferingEnrolmentId, \
        PU_COURSE.UNIT_INSTANCE_CODE SUBJECT_ID, \
        CASE \
          WHEN UI_COURSE.NATIONAL_COURSE_CODE IS NOT NULL THEN UI_COURSE.NATIONAL_COURSE_CODE \
          WHEN LEFT(RIGHT(PU_COURSE.UNIT_INSTANCE_CODE,3),1) = 'V' \
               AND LEFT(RIGHT(PU_COURSE.UNIT_INSTANCE_CODE,2),1) IN ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9') \
                     THEN SUBSTRING(PU_COURSE.UNIT_INSTANCE_CODE, 1, CHAR_LENGTH(PU_COURSE.UNIT_INSTANCE_CODE)-3) \
          ELSE PU_COURSE.UNIT_INSTANCE_CODE \
        END COURSE_CODE, \
        COALESCE(PUS_COURSE.START_DATE, UIO_COURSE.FES_START_DATE) FES_START_DATE, \
        COALESCE(PUS_COURSE.END_DATE, UIO_COURSE.FES_END_DATE) FES_END_DATE, \
        COALESCE(PU_COURSE.STUDY_REASON,'@@') STUDY_REASON, \
        PU_COURSE.NZ_FUNDING, \
        AT.DATE_AWARDED DATE_AWARDED, \
        UI_COURSE.IS_BOS_COURSE, \
        PU_COURSE.TRAINING_CONTRACT_IDENTIFIER, \
        CASE WHEN PU_COURSE.user_1 in ('A','T') THEN COALESCE(PU_COURSE.TRAINING_CONTRACT_IDENTIFIER,'@@@@@@@@@@') \
          WHEN PU_COURSE.PROGRAM_STREAM IN (3,4) THEN COALESCE(PU_COURSE.TRAINING_CONTRACT_IDENTIFIER,'@@@@@@@@@@') \
          ELSE '' END TRAINING_CONT_ID, \
        CASE WHEN PU_COURSE.USER_1 in ('A','T') THEN COALESCE(substr(PU_COURSE.TRAINING_CONTRACT_IDENTIFIER,1,instr(PU_COURSE.TRAINING_CONTRACT_IDENTIFIER,'/')-1),'@@@@@@@@@@') \
          WHEN PU_COURSE.PROGRAM_STREAM IN (3,4) THEN COALESCE(substr(PU_COURSE.TRAINING_CONTRACT_IDENTIFIER,1,instr(PU_COURSE.TRAINING_CONTRACT_IDENTIFIER,'/')-1),'@@@@@@@@@@') \
          ELSE '' END CLIENT_ID_APPRENTICESHIP, \
        CASE WHEN UIO_COURSE.OFFERING_TYPE='TVET' OR PU_COURSE.USER_8='Y' THEN 'Y' \
          ELSE 'N' END VET_IN_SCHOOL_FLAG, \
        AT.RECOGNITION_TYPE RECOGNITION, \
        VP_FUNDING_NATIONAL.PROPERTY_VALUE FUNDING_SPECIFIC, \
        null AS school_type_identifier, \
        PU_COURSE.ID AS COURSE_PU_ID, \
        NULL AS UNIT_PU_ID, \
        CASE WHEN PU_COURSE.DESTINATION IN ('SSDISC','SSTRAN','WDVFHAC','WDVFHBC','WDSAC','SSDiscCOV') THEN 'TNC' \
          WHEN PU_COURSE.DESTINATION IN ('SSDEF','SSDefCOV') AND PU_COURSE.PROGRESS_DATE <= ADD_MONTHS(current_timestamp(),-12) THEN 'TNC' \
          WHEN PU_COURSE.DESTINATION IN ('SSDEF','SSDefCOV') THEN 'D' \
          ELSE NULL END OUTCOME_ID_TRAINING_ORG, \
        PU_COURSE.CALOCC_CODE ASSOC_CRSE_ID, \
        UI_COURSE.IS_VOCATIONAL, \
        PU_COURSE.DESTINATION, \
        AT.AWARD_ID, \
        AT.CONFIGURABLE_STATUS_ID, \
        AT.ATTAINMENT_CODE, \
        UI_COURSE.UI_TYPE, \
        PU_COURSE.PROGRESS_CODE, \
        cast(REFERENCE_LOCATION.InstituteId as int) InstituteId, \
        cast(REFERENCE_LOCATION.RTOCode as int) RTOCode \
        FROM trusted.OneEBS_EBS_0165_PEOPLE_UNITS PU_COURSE \
        INNER JOIN trusted.OneEBS_EBS_0165_PEOPLE PEO \
            ON PEO.PERSON_CODE = PU_COURSE.PERSON_CODE \
            AND PEO._RecordCurrent = 1 \
            AND PEO._RecordDeleted = 0 \
        INNER JOIN trusted.OneEBS_EBS_0165_UNIT_INSTANCE_OCCURRENCES UIO_COURSE \
            ON UIO_COURSE.UIO_ID = PU_COURSE.UIO_ID \
            AND UIO_COURSE._RecordCurrent = 1 \
            AND UIO_COURSE._RecordDeleted = 0 \
        INNER JOIN trusted.OneEBS_EBS_0165_UNIT_INSTANCES UI_COURSE \
            ON UI_COURSE.FES_UNIT_INSTANCE_CODE = PU_COURSE.UNIT_INSTANCE_CODE \
            AND UI_COURSE._RecordCurrent = 1 \
            AND UI_COURSE._RecordDeleted = 0 \
        LEFT JOIN trusted.OneEBS_EBS_0165_PEOPLE_UNITS_SPECIAL PUS_COURSE \
            ON PUS_COURSE.PEOPLE_UNITS_ID = PU_COURSE.ID \
            AND PUS_COURSE._RecordCurrent = 1 \
            AND PUS_COURSE._RecordDeleted = 0 \
        INNER JOIN \
        ( \
            select * \
            from \
            ( \
                select *, row_number() over(partition by PEOPLE_UNITS_ID order by _transaction_date desc) rn \
                from trusted.OneEBS_EBS_0165_ATTAINMENTS \
                where ATYPE_ATTAINMENT_TYPE_CODE='AWARD' \
                  AND _RecordCurrent = 1 \
                  AND _RecordDeleted = 0 \
            ) x \
            where x.rn = 1 \
        ) AT ON AT.PEOPLE_UNITS_ID = PU_COURSE.ID \
        left join trusted.OneEBS_EBS_0165_VERIFIER_PROPERTIES VP_FUNDING_NATIONAL \
          on PU_COURSE.NZ_FUNDING = VP_FUNDING_NATIONAL.LOW_VALUE \
            and VP_FUNDING_NATIONAL.RV_DOMAIN = 'FUNDING' \
            AND VP_FUNDING_NATIONAL.PROPERTY_NAME = 'DEC_FUNDING_SOURCE_NATIONAL_2015' \
            and VP_FUNDING_NATIONAL._RecordCurrent=1 and VP_FUNDING_NATIONAL._RecordDeleted=0 \
        INNER JOIN compliance.avetmiss_location REFERENCE_LOCATION \
          ON UIO_COURSE.SLOC_LOCATION_CODE = REFERENCE_LOCATION.LocationCode \
          AND REFERENCE_LOCATION._RecordCurrent = 1 \
          AND REFERENCE_LOCATION._RecordDeleted = 0 \
        WHERE 1=1 \
              AND PU_COURSE.UNIT_TYPE = 'R' \
              AND UI_COURSE.UI_LEVEL='COURSE' \
              AND (PU_COURSE.progress_status  in ('A','F','W','T') AND PU_COURSE.PROGRESS_CODE NOT IN ('3.4NS','3.3WN'))  \
              and (PU_COURSE.destination not in ('WDEC','WDSBC','CNPORT','WDCC') OR PU_COURSE.destination is null) \
              and trim(PU_COURSE.commitment_identifier) is not null  \
              and not(LEFT(trim(PU_COURSE.COMMITMENT_IDENTIFIER),2) IN ('C6','C7','C8','C9')) \
              and coalesce(PU_COURSE.SUBSIDY_STATUS,'SS') in ('Data Submitted', 'Notified','SS') \
              AND PU_COURSE._RecordCurrent = 1 \
              AND PU_COURSE._RecordDeleted = 0 \
  "
  df= spark.sql(qry)

  #PRADA-1711
  df = df if exclusionList == False else df.join(DeltaTableAsCurrent("reference.ereporting_cid_exclusion_list"), expr("LOWER(reference.ereporting_cid_exclusion_list.COMMITMENT_ID) = LOWER(PURCHASE_CON_ID)"),how="left").where("reference.ereporting_cid_exclusion_list.COMMITMENT_ID IS NULL")
  
  qry_case="CASE \
                           WHEN substr(PURCHASE_CON_ID,2,2)*1+2000 < %s      THEN '4' \
                           WHEN substr(PURCHASE_CON_ID,2,2)*1+2000 >= %s      THEN '3'  \
                           ELSE '8'  END" % (CollectionEndYear,CollectionEndYear)
  df = df.withColumn("COMMENCING_PROG_ID", \
                     expr(qry_case))
  #AND coalesce(pus_course.start_date, uio_course.fes_start_date) between '&COLLECTION_PERIOD_START_DATE' and '&COLLECTION_PERIOD_END_DATE'
  df=df.filter(col("FES_START_DATE").between(lit(CollectionStart),lit(CollectionEnd)))
  #PRADA-1628
  df=df.withColumn("COURSE_CODE", expr("TRIM(COURSE_CODE)"))
  return df

# COMMAND ----------


