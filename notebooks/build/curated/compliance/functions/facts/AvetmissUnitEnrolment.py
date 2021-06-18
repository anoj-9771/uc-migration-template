# Databricks notebook source
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.window import *
from pyspark.sql.types import *

# COMMAND ----------

def _PeopleUnit():
  # ##########################################################################################################################  
  # Description: Returns a dataframe that contains unit enrolments picking data from PEOPLE_UNITS as the primary table
  # ############################################################################################################################

  dfPeopleUnit = spark.sql(" \
    SELECT DISTINCT \
      'Unit Offering Enrolment' AS RecordSource /*01M-EDWDM-106*/ \
      ,cast(WEB_CONFIG.PARAMETER_VALUE as int) as InstituteId /*02M-EDWDM-107*/ \
      ,CAST(PEOPLE_UNITS.ID as INT) UnitEnrolmentId /*PRADA-889*/ \
      ,UNIT_INSTANCE_OCCURRENCES.SLOC_LOCATION_CODE UnitDeliveryLocationCode /*03M-EDWDM-108*/ \
      ,COALESCE(CAST(PEOPLE.REGISTRATION_NO as BIGINT), concat(cast(WEB_CONFIG.PARAMETER_VALUE as int), cast(PEOPLE.PERSON_CODE as int)))  AvetmissClientId /*04T-EDWDM-109*/ \
      ,CASE WHEN PEOPLE.REGISTRATION_NO IS NULL THEN 'Y' ELSE 'N' END AvetmissClientIdMissingFlag /*05T-EDWDM-110*/ \
      ,PEOPLE.PERSON_CODE StudentId /*06M-EDWDM-111*/ \
      ,CAST(PEOPLE_UNITS.ID as INT) UnitOfferingEnrolmentId /*07M-EDWDM-112*/ \
      ,PEOPLE_UNITS.CALOCC_CODE UnitOfferingCode /*08M-EDWDM-113*/ \
      ,COALESCE(UNIT_INSTANCES.NATIONAL_COURSE_CODE, UNIT_INSTANCES.FES_UNIT_INSTANCE_CODE) AvetmissUnitCode /*09T-EDWDM-114*/ \
      ,UNIT_INSTANCES.FES_UNIT_INSTANCE_CODE UnitCode /*10M-EDWDM-115*/ \
      ,UNIT_INSTANCES.NATIONAL_COURSE_CODE NationalUnitCode /*11M-EDWDM-116*/ \
      ,COALESCE(UNIT_INSTANCE_OCCURRENCES.FES_MOA_CODE, PEOPLE_UNITS_COURSE.FES_MOA_CODE) DeliveryMode /*12T-EDWDM-117*/ \
      ,CAST(COALESCE(PEOPLE_UNITS_SPECIAL.START_DATE, UNIT_INSTANCE_OCCURRENCES.FES_START_DATE) AS DATE) UnitEnrolmentStartDate /*13T-EDWDM-118*/ \
      ,CAST(CASE  \
        WHEN ATTAINMENTS.DATE_AWARDED > '1753-01-01'  \
          AND ATTAINMENTS.DATE_AWARDED < COALESCE(PEOPLE_UNITS_SPECIAL.END_DATE, UNIT_INSTANCE_OCCURRENCES.FES_END_DATE)  \
          AND ATTAINMENTS.DATE_AWARDED >=  COALESCE(PEOPLE_UNITS_SPECIAL.START_DATE, UNIT_INSTANCE_OCCURRENCES.FES_START_DATE)  \
          AND COALESCE(ATTAINMENTS.ADJUSTED_GRADE, ATTAINMENTS.GRADE, '') NOT IN ('CU', 'PW') THEN ATTAINMENTS.DATE_AWARDED  \
        ELSE COALESCE(PEOPLE_UNITS_SPECIAL.END_DATE, UNIT_INSTANCE_OCCURRENCES.FES_END_DATE) \
       END AS DATE) UnitEnrolmentEndDate /*14T-EDWDM-119*/ \
      ,CASE \
        WHEN COALESCE(ATTAINMENTS.ADJUSTED_GRADE, ATTAINMENTS.GRADE) IN ('RPL','CT','PW','CU') THEN '90' \
        ELSE  \
          CASE  \
            WHEN UNIT_INSTANCE_OCCURRENCES.FES_MOA_CODE IS NOT NULL THEN REFERENCE_DELIVERY_MODE.AvetmissDeliveryModeId \
            ELSE '90' \
          END \
       END AvetmissDeliveryMode /*15T-EDWDM-120*/ \
      ,UNIT_INSTANCES.MAXIMUM_HOURS UnitScheduledHours /*16M-EDWDM-121*/ \
      ,PEOPLE_UNITS.NZ_FUNDING UnitFundingSourceCode /*17M-EDWDM-122*/ \
      ,REFERENCE_FUNDING_SOURCE.SpecificFundingCodeID AvetmissSpecificFundingId /*18M-EDWDM-123*/ \
      ,REFERENCE_FUNDING_SOURCE.FundingSourceNationalID AvetmissFundingSourceNationalId /*19T-EDWDM-124*/ \
      ,ATTAINMENTS.RECOGNITION_TYPE UnitRecognitionType /*20M-EDWDM-125*/ \
      ,CASE  \
        WHEN ATTAINMENTS.DATE_AWARDED <= '1753-01-01' THEN NULL \
        ELSE ATTAINMENTS.DATE_AWARDED \
       END UnitAwardDate /*21M-EDWDM-126*/ \
      ,ATTAINMENTS.GRADE AS Grade /*PRADA-814*/ \
      ,ATTAINMENTS.ADJUSTED_GRADE AS AdjustedGrade /*PRADA-814*/ \
      ,COALESCE(ATTAINMENTS.ADJUSTED_GRADE, ATTAINMENTS.GRADE) UnitOutcome /*22T-EDWDM-127*/ \
      ,ATTAINMENTS_COURSE.DATE_CONFERRED AS DateConferred /*PRADA-814*/ \
      ,GRADING_SCHEME_GRADES.SDR_COMPLETION_CODE SDRCompletionCode /*23M-EDWDM-128*/ \
      ,PEOPLE_UNITS.HOURS_ATTENDED_ON_WITHDRAWAL UnitHoursAttended /*24M-EDWDM-129*/ \
      ,PEOPLE_UNITS.UIO_ID UnitOfferingId /*25M-EDWDM-130*/ \
      ,PEOPLE_UNITS.PROGRESS_CODE UnitProgressCode /*26M-EDWDM-131*/ \
      ,PEOPLE_UNITS.PROGRESS_STATUS UnitProgressStatus /*27M-EDWDM-132*/ \
      ,PEOPLE_UNITS.DESTINATION UnitProgressReason /*28M-EDWDM-133*/ \
      ,CASE  \
        WHEN PEOPLE_UNITS.PROGRESS_DATE <= '1753-01-01' THEN NULL \
        ELSE PEOPLE_UNITS.PROGRESS_DATE \
       END UnitProgressDate /*29M-EDWDM-134*/ \
      ,PEOPLE_UNITS.SPONSOR_ORG_CODE UnitSponsorOrganisationCode /*30M-EDWDM-135*/ \
      ,UNIT_INSTANCE_OCCURRENCES.OWNING_ORGANISATION UnitOfferingFunctionalUnit /*31M-EDWDM-136*/ \
      ,ATTAINMENTS.ATTAINMENT_CODE AttainmentCode /*32M-EDWDM-137*/ \
      ,CASE \
        WHEN ATTAINMENTS.ATTAINMENT_CODE IS NULL THEN 1 \
        ELSE 0 \
       END AcademicHistoryMissingFlag /*33T-EDWDM-138*/ \
      ,CONFIGURABLE_STATUSES_UNIT.STATUS_CODE AS UnitLevelStatus /*PRADA-814*/ \
      ,CONFIGURABLE_STATUSES_UNIT.ACTIVE AS UnitLevelStatusActive /*PRADA-1668*/ \
      ,CONFIGURABLE_STATUSES_UNIT.STATUS_TYPE AS UnitLevelStatusType /*PRADA-1668*/ \
      ,COALESCE(cast(PEOPLE_UNIT_LINKS.CourseEnrolmentId as int), concat(cast(WEB_CONFIG.PARAMETER_VALUE as int), cast(PEOPLE.PERSON_CODE as int))) CourseOfferingEnrolmentId /*34T-EDWDM-139*/ \
      ,PEOPLE_UNITS_COURSE.CALOCC_CODE CourseOfferingCode /*35M-EDWDM-140*/ \
      ,PEOPLE_UNITS_COURSE.UIO_ID CourseOfferingId /*36M-EDWDM-141*/ \
      ,PEOPLE_UNITS_COURSE.SLOC_LOCATION_CODE CourseDeliveryLocationCode \
      ,COALESCE(PEOPLE_UNITS_COURSE.UNIT_INSTANCE_CODE, concat(cast(WEB_CONFIG.PARAMETER_VALUE as int), '-999999')) CourseCode /*37M-EDWDM-143*/ \
      ,PEOPLE_UNITS_COURSE.NATIONAL_COURSE_CODE NationalCourseCode /*38M-EDWDM-144*/ \
      ,COALESCE \
       ( \
          CASE \
            WHEN PEOPLE_UNITS.ID = PEOPLE_UNITS_COURSE.ID THEN CAST(NULL AS VARCHAR(20))  \
            WHEN PEOPLE_UNITS_COURSE.ID IS NULL THEN CAST(NULL AS VARCHAR(20))  \
            WHEN PEOPLE_UNITS_COURSE.NATIONAL_COURSE_CODE IS NOT NULL THEN PEOPLE_UNITS_COURSE.NATIONAL_COURSE_CODE \
            WHEN LEFT(RIGHT(PEOPLE_UNITS_COURSE.UNIT_INSTANCE_CODE,3),1) = 'V' \
                 AND LEFT(RIGHT(PEOPLE_UNITS_COURSE.UNIT_INSTANCE_CODE,2),1) IN ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9') \
                       THEN SUBSTRING(PEOPLE_UNITS_COURSE.UNIT_INSTANCE_CODE, 1, CHAR_LENGTH(PEOPLE_UNITS_COURSE.UNIT_INSTANCE_CODE)-3) \
            ELSE PEOPLE_UNITS_COURSE.UNIT_INSTANCE_CODE \
          END \
          ,concat(cast(WEB_CONFIG.PARAMETER_VALUE as int), '-999999') \
       ) CourseCodeConverted /*39T-EDWDM-145*/ \
      ,PEOPLE_UNITS_COURSE.PROGRESS_CODE CourseProgressCode /*40M-EDWDM-146*/ \
      ,PEOPLE_UNITS_COURSE.DESTINATION CourseProgressReason /*41M-EDWDM-147*/ \
      ,PEOPLE_UNITS_COURSE.PROGRESS_STATUS CourseProgressStatus /*42M-EDWDM-148*/ \
      ,CASE  \
        WHEN PEOPLE_UNITS_COURSE.PROGRESS_DATE <= '1753-01-01' THEN NULL \
        ELSE PEOPLE_UNITS_COURSE.PROGRESS_DATE \
       END CourseProgressDate /*43M-EDWDM-149*/ \
      ,CASE  \
        WHEN ATTAINMENTS_COURSE.DATE_AWARDED <= '1753-01-01' THEN NULL \
        ELSE ATTAINMENTS_COURSE.DATE_AWARDED \
       END CourseAwardDate /*44M-EDWDM-150*/ \
      ,CAST(PEOPLE_UNITS_COURSE.COURSE_ENROLMENT_START_DATE AS DATE) CourseEnrolmentStartDate /*45T-EDWDM-151*/ \
      ,CAST(PEOPLE_UNITS_COURSE.COURSE_ENROLMENT_END_DATE AS DATE) CourseEnrolmentEndDate /*46T-EDWDM-152*/ \
      ,PEOPLE_UNITS_COURSE.NZ_FUNDING CourseFundingSourceCode /*47M-EDWDM-153*/ \
      ,ATTAINMENTS_COURSE.DESCRIPTION CourseAwardDescription /*48M-EDWDM-154*/ \
      ,CONFIGURABLE_STATUSES.STATUS_CODE CourseAwardStatus /*49M-EDWDM-155*/ \
      ,PEOPLE_UNITS_COURSE.FES_USER_1 CourseOfferingRemarks /*50M-EDWDM-156*/ \
      ,PEOPLE_UNITS_COURSE.OWNING_ORGANISATION CourseOfferingFunctionalUnit /*51M-EDWDM-157*/ \
      ,PEOPLE_UNITS_COURSE.TRANSITION_FROM_PEOPLE_UNIT_ID CourseEnrolmentIdTransitionFrom /*52M-EDWDM-242*/ \
      ,PEOPLE_UNITS_COURSE_TRA.CourseCodeTransitionFrom /*53M-EDWDM-243*/ \
      ,PEOPLE_UNITS_COURSE_TRA.CourseCaloccCodeTransitionFrom /*54M-EDWDM-244*/ \
      ,PEOPLE_UNITS_COURSE_TRA.CourseEnrolmentStartDateTransitionFrom /*55T-EDWDM-245*/ \
      ,PEOPLE_UNITS_COURSE.CREATED_DATE CourseEnrolmentCreatedDate \
      ,PEOPLE_UNITS.Study_Reason StudyReasonID /*PRADA-814*/ \
      ,StudyReason.StudyReasonDescription StudyReasonDescription \
      ,REFERENCE_LOCATION.LocationName DeliveryLocationName \
      ,CASE WHEN UNIT_INSTANCE_OCCURRENCES.OFFERING_TYPE = 'TVET' OR PEOPLE_UNITS.USER_8 = 'Y' THEN 'Y' ELSE 'N' END VetInSchoolFlag \
      ,OrgUnits.FES_FULL_NAME AS SponsorDescription \
      ,PEOPLE_UNITS.PROGRAM_STREAM ProgramStream \
      ,CAST(COALESCE(PEOPLE_UNITS_SPECIAL.END_DATE, UNIT_INSTANCE_OCCURRENCES.FES_END_DATE) AS DATE) UnitEndDate \
      ,ATTAINMENTS.CREATED_DATE AttainmentCreatedDate \
      ,ATTAINMENTS.UPDATED_DATE AttainmentUpdatedDate \
      ,CONCAT(OrgUnitSponsorCode.Organisation_code, ' ', OrgUnitSponsorCode.FES_FULL_NAME) SponsorCode /* PRADA-1788 */ \
    FROM trusted.OneEBS_EBS_0165_PEOPLE_UNITS PEOPLE_UNITS \
      INNER JOIN trusted.OneEBS_EBS_0165_PEOPLE PEOPLE  \
          ON PEOPLE.PERSON_CODE = PEOPLE_UNITS.PERSON_CODE \
          AND PEOPLE._RecordCurrent = 1 AND PEOPLE._RecordDeleted = 0 \
      INNER JOIN trusted.OneEBS_EBS_0165_UNIT_INSTANCE_OCCURRENCES UNIT_INSTANCE_OCCURRENCES  \
          ON UNIT_INSTANCE_OCCURRENCES.UIO_ID = PEOPLE_UNITS.UIO_ID \
          AND UNIT_INSTANCE_OCCURRENCES._RecordCurrent = 1 AND UNIT_INSTANCE_OCCURRENCES._RecordDeleted = 0 \
      INNER JOIN trusted.OneEBS_EBS_0165_UNIT_INSTANCES UNIT_INSTANCES  \
          ON UNIT_INSTANCES.FES_UNIT_INSTANCE_CODE = PEOPLE_UNITS.UNIT_INSTANCE_CODE  \
          AND UNIT_INSTANCES._RecordCurrent = 1 AND UNIT_INSTANCES._RecordDeleted = 0 \
      INNER JOIN compliance.avetmiss_location REFERENCE_LOCATION \
          ON UNIT_INSTANCE_OCCURRENCES.SLOC_LOCATION_CODE = REFERENCE_LOCATION.LocationCode \
          AND REFERENCE_LOCATION._RecordCurrent = 1 AND REFERENCE_LOCATION._RecordDeleted = 0 \
      CROSS JOIN trusted.OneEBS_EBS_0165_WEB_CONFIG WEB_CONFIG \
        ON WEB_CONFIG.PARAMETER = 'INST_CODE' \
        AND WEB_CONFIG._RecordCurrent = 1 AND WEB_CONFIG._RecordDeleted = 0 \
      LEFT JOIN trusted.OneEBS_EBS_0165_PEOPLE_UNITS_SPECIAL PEOPLE_UNITS_SPECIAL  \
          ON PEOPLE_UNITS_SPECIAL.PEOPLE_UNITS_ID = PEOPLE_UNITS.ID \
          AND PEOPLE_UNITS_SPECIAL._RecordCurrent = 1 AND PEOPLE_UNITS_SPECIAL._RecordDeleted = 0 \
      LEFT JOIN \
        ( \
            select * \
            from \
            ( \
                select *, row_number() over(partition by PEOPLE_UNITS_ID order by coalesce(UPDATED_DATE, CREATED_DATE) desc, ATTAINMENT_CODE desc) rn \
                from trusted.OneEBS_EBS_0165_ATTAINMENTS \
                where _RecordCurrent = 1 AND _RecordDeleted = 0 \
            ) x \
            where x.rn = 1 \
        ) ATTAINMENTS ON ATTAINMENTS.PEOPLE_UNITS_ID = PEOPLE_UNITS.ID \
      LEFT JOIN trusted.OneEBS_EBS_0165_CONFIGURABLE_STATUSES CONFIGURABLE_STATUSES_UNIT  \
          ON CONFIGURABLE_STATUSES_UNIT.ID = ATTAINMENTS.CONFIGURABLE_STATUS_ID \
          AND CONFIGURABLE_STATUSES_UNIT._RecordCurrent = 1 AND CONFIGURABLE_STATUSES_UNIT._RecordDeleted = 0 \
      LEFT JOIN reference.funding_source REFERENCE_FUNDING_SOURCE \
          ON PEOPLE_UNITS.NZ_FUNDING = REFERENCE_FUNDING_SOURCE.FundingSourceCode \
          AND REFERENCE_FUNDING_SOURCE.Active = 'Y' \
          AND REFERENCE_FUNDING_SOURCE._RecordCurrent = 1 AND REFERENCE_FUNDING_SOURCE._RecordDeleted = 0 \
      LEFT JOIN reference.delivery_mode REFERENCE_DELIVERY_MODE \
          ON UNIT_INSTANCE_OCCURRENCES.FES_MOA_CODE = REFERENCE_DELIVERY_MODE.DeliveryModeCode \
          AND REFERENCE_DELIVERY_MODE._RecordCurrent = 1 AND REFERENCE_DELIVERY_MODE._RecordDeleted = 0 \
      LEFT JOIN trusted.OneEBS_EBS_0165_GRADING_SCHEME_GRADES GRADING_SCHEME_GRADES  \
          ON GRADING_SCHEME_GRADES.GRADING_SCHEME_ID = ATTAINMENTS.GRADING_SCHEME_ID  \
          AND GRADING_SCHEME_GRADES.GRADE = COALESCE(ATTAINMENTS.ADJUSTED_GRADE, ATTAINMENTS.GRADE) \
          AND GRADING_SCHEME_GRADES._RecordCurrent = 1 AND GRADING_SCHEME_GRADES._RecordDeleted = 0 \
      LEFT JOIN AvetmissPeopleUnitLinks PEOPLE_UNIT_LINKS \
          ON PEOPLE_UNIT_LINKS.UnitEnrolmentId = PEOPLE_UNITS.ID \
      LEFT JOIN \
      ( \
        SELECT \
          PEOPLE_UNITS_COURSE.ID \
          ,PEOPLE_UNITS_COURSE.UNIT_INSTANCE_CODE \
          ,PEOPLE_UNITS_COURSE.CALOCC_CODE \
          ,PEOPLE_UNITS_COURSE.UIO_ID \
          ,PEOPLE_UNITS_COURSE.PROGRESS_CODE \
          ,PEOPLE_UNITS_COURSE.DESTINATION \
          ,PEOPLE_UNITS_COURSE.PROGRESS_STATUS \
          ,PEOPLE_UNITS_COURSE.PROGRESS_DATE \
          ,PEOPLE_UNITS_COURSE.NZ_FUNDING \
          ,PEOPLE_UNITS_COURSE.TRANSITION_FROM_PEOPLE_UNIT_ID \
          ,PEOPLE_UNITS_COURSE.CREATED_DATE \
          ,PEOPLE_UNITS_COURSE.SPONSOR_ORG_CODE \
          ,UNIT_INSTANCES_COURSE.NATIONAL_COURSE_CODE \
          ,COALESCE(PEOPLE_UNITS_SPECIAL_COURSE.START_DATE, UNIT_INSTANCE_OCCURRENCES_COURSE.FES_START_DATE) AS COURSE_ENROLMENT_START_DATE \
          ,COALESCE(PEOPLE_UNITS_SPECIAL_COURSE.END_DATE, UNIT_INSTANCE_OCCURRENCES_COURSE.FES_END_DATE) AS COURSE_ENROLMENT_END_DATE \
          ,UNIT_INSTANCE_OCCURRENCES_COURSE.FES_MOA_CODE \
          ,UNIT_INSTANCE_OCCURRENCES_COURSE.UIO_ID CourseOfferingId \
          ,UNIT_INSTANCE_OCCURRENCES_COURSE.SLOC_LOCATION_CODE \
          ,UNIT_INSTANCE_OCCURRENCES_COURSE.FES_USER_1 \
          ,UNIT_INSTANCE_OCCURRENCES_COURSE.OWNING_ORGANISATION \
        FROM trusted.OneEBS_EBS_0165_PEOPLE_UNITS PEOPLE_UNITS_COURSE \
          JOIN trusted.OneEBS_EBS_0165_UNIT_INSTANCES UNIT_INSTANCES_COURSE  \
            ON UNIT_INSTANCES_COURSE.FES_UNIT_INSTANCE_CODE = PEOPLE_UNITS_COURSE.UNIT_INSTANCE_CODE  \
          JOIN trusted.OneEBS_EBS_0165_UNIT_INSTANCE_OCCURRENCES UNIT_INSTANCE_OCCURRENCES_COURSE  \
            ON UNIT_INSTANCE_OCCURRENCES_COURSE.UIO_ID = PEOPLE_UNITS_COURSE.UIO_ID \
          LEFT JOIN trusted.OneEBS_EBS_0165_PEOPLE_UNITS_SPECIAL PEOPLE_UNITS_SPECIAL_COURSE  \
            ON PEOPLE_UNITS_SPECIAL_COURSE.PEOPLE_UNITS_ID = PEOPLE_UNITS_COURSE.ID \
            AND PEOPLE_UNITS_SPECIAL_COURSE._RecordCurrent = 1 AND PEOPLE_UNITS_SPECIAL_COURSE._RecordDeleted = 0 \
        WHERE 1=1 \
          AND PEOPLE_UNITS_COURSE.UNIT_TYPE = 'R' \
		  AND UNIT_INSTANCES_COURSE.CTYPE_CALENDAR_TYPE_CODE = 'COURSE' \
          AND PEOPLE_UNITS_COURSE._RecordCurrent = 1 AND PEOPLE_UNITS_COURSE._RecordDeleted = 0 \
          AND UNIT_INSTANCES_COURSE._RecordCurrent = 1 AND UNIT_INSTANCES_COURSE._RecordDeleted = 0 \
          AND UNIT_INSTANCE_OCCURRENCES_COURSE._RecordCurrent = 1 AND UNIT_INSTANCE_OCCURRENCES_COURSE._RecordDeleted = 0 \
      ) PEOPLE_UNITS_COURSE ON PEOPLE_UNITS_COURSE.ID = PEOPLE_UNIT_LINKS.CourseEnrolmentId \
      LEFT JOIN \
        ( \
            select * \
            from \
            ( \
                select *, row_number() over(partition by PEOPLE_UNITS_ID order by coalesce(UPDATED_DATE, CREATED_DATE) desc, ATTAINMENT_CODE desc) rn \
                from trusted.OneEBS_EBS_0165_ATTAINMENTS \
                where _RecordCurrent = 1 AND _RecordDeleted = 0 \
            ) x \
            where x.rn = 1 \
        ) ATTAINMENTS_COURSE ON ATTAINMENTS_COURSE.PEOPLE_UNITS_ID = PEOPLE_UNITS_COURSE.ID \
      LEFT JOIN trusted.OneEBS_EBS_0165_CONFIGURABLE_STATUSES CONFIGURABLE_STATUSES  \
          ON CONFIGURABLE_STATUSES.ID = ATTAINMENTS_COURSE.CONFIGURABLE_STATUS_ID \
          AND CONFIGURABLE_STATUSES._RecordCurrent = 1 AND CONFIGURABLE_STATUSES._RecordDeleted = 0 \
      LEFT JOIN  \
      ( \
        SELECT  \
          PEOPLE_UNITS.ID,  \
          PEOPLE_UNITS.UNIT_INSTANCE_CODE CourseCodeTransitionFrom,  \
          PEOPLE_UNITS.CALOCC_CODE CourseCaloccCodeTransitionFrom,  \
          COALESCE(PEOPLE_UNITS_SPECIAL.START_DATE, UNIT_INSTANCE_OCCURRENCES.FES_START_DATE) CourseEnrolmentStartDateTransitionFrom \
        FROM trusted.OneEBS_EBS_0165_PEOPLE_UNITS PEOPLE_UNITS \
          INNER JOIN trusted.OneEBS_EBS_0165_UNIT_INSTANCE_OCCURRENCES UNIT_INSTANCE_OCCURRENCES \
                        ON PEOPLE_UNITS.UIO_ID = UNIT_INSTANCE_OCCURRENCES.UIO_ID \
                        AND UNIT_INSTANCE_OCCURRENCES._RecordCurrent = 1 AND UNIT_INSTANCE_OCCURRENCES._RecordDeleted = 0 \
          LEFT JOIN trusted.OneEBS_EBS_0165_PEOPLE_UNITS_SPECIAL PEOPLE_UNITS_SPECIAL \
                        ON PEOPLE_UNITS.ID = PEOPLE_UNITS_SPECIAL.PEOPLE_UNITS_ID \
                        AND PEOPLE_UNITS_SPECIAL._RecordCurrent = 1 AND PEOPLE_UNITS_SPECIAL._RecordDeleted = 0 \
        WHERE 1=1 \
        AND PEOPLE_UNITS._RecordCurrent = 1 AND PEOPLE_UNITS._RecordDeleted = 0 \
      ) PEOPLE_UNITS_COURSE_TRA ON PEOPLE_UNITS_COURSE.TRANSITION_FROM_PEOPLE_UNIT_ID = PEOPLE_UNITS_COURSE_TRA.ID  \
      LEFT JOIN Reference.Study_Reason StudyReason \
          ON StudyReason.StudyReasonID = PEOPLE_UNITS.Study_Reason \
          AND StudyReason._RecordCurrent = 1 AND StudyReason._RecordDeleted = 0 \
      LEFT JOIN trusted.OneEBS_EBS_0165_ORGANISATION_UNITS OrgUnits \
          ON OrgUnits.organisation_code = PEOPLE_UNITS_COURSE.OWNING_ORGANISATION \
          AND OrgUnits._RecordCurrent = 1 AND OrgUnits._RecordDeleted = 0 \
      LEFT JOIN trusted.OneEBS_EBS_0165_ORGANISATION_UNITS OrgUnitSponsorCode \
          ON OrgUnitSponsorCode.organisation_code = PEOPLE_UNITS_COURSE.SPONSOR_ORG_CODE \
          AND OrgUnitSponsorCode.ORGANISATION_TYPE = 'SPONSOR' \
          AND OrgUnitSponsorCode._RecordCurrent = 1 \
          AND OrgUnitSponsorCode._RecordDeleted = 0 \
    WHERE 1=1 \
    AND PEOPLE_UNITS.UNIT_TYPE = 'R' \
    AND UNIT_INSTANCES.UI_LEVEL = 'UNIT'  \
    AND COALESCE(UNIT_INSTANCES.UNIT_CATEGORY, '') <> 'BOS' \
    AND COALESCE(PEOPLE_UNITS_SPECIAL.START_DATE, UNIT_INSTANCE_OCCURRENCES.FES_START_DATE) > '1753-01-01'  \
    AND COALESCE(PEOPLE_UNITS_SPECIAL.END_DATE, UNIT_INSTANCE_OCCURRENCES.FES_END_DATE) > '1753-01-01' \
    AND PEOPLE_UNITS._RecordCurrent = 1 AND PEOPLE_UNITS._RecordDeleted = 0 \
  ")

  return dfPeopleUnit

# COMMAND ----------

def _Attainments():
  # ##########################################################################################################################  
  # Description: Returns a dataframe that contains unit enrolments picking data from ATTAINMENTS as the primary table
  # ############################################################################################################################
  
  dfAttainments = spark.sql(" \
    SELECT DISTINCT \
      'Academic Record' AS RecordSource /*01M-EDWDM-158*/ \
      ,cast(WEB_CONFIG.PARAMETER_VALUE as int) as InstituteId /*02M-EDWDM-159*/ \
      ,CAST(ATTAINMENTS_UNIT.ATTAINMENT_CODE as INT) UnitEnrolmentId /*PRADA-889*/ \
      ,UNIT_INSTANCE_OCCURRENCES_COURSE.SLOC_LOCATION_CODE UnitDeliveryLocationCode /*M-EDWDM-160*/ \
      ,COALESCE(cast(PEOPLE.REGISTRATION_NO as bigint), concat(cast(WEB_CONFIG.PARAMETER_VALUE as int), cast(PEOPLE.PERSON_CODE as int))) AvetmissClientId /*04T-EDWDM-161*/ \
      ,CASE WHEN PEOPLE.REGISTRATION_NO IS NULL THEN 'Y' ELSE 'N' END AvetmissClientIdMissingFlag /*05T-EDWDM-162*/ \
      ,PEOPLE.PERSON_CODE StudentId /*06M-EDWDM-163*/ \
      ,CAST(NULL AS int) UnitOfferingEnrolmentId /*07T-EDWDM-164*/ \
      ,CAST(NULL AS VARCHAR(50)) UnitOfferingCode /*08T-EDWDM-165*/ \
      ,COALESCE(UNIT_INSTANCES_UNIT.NATIONAL_COURSE_CODE, UNIT_INSTANCES_UNIT.FES_UNIT_INSTANCE_CODE) AvetmissUnitCode /*09T-EDWDM-166*/ \
      ,UNIT_INSTANCES_UNIT.FES_UNIT_INSTANCE_CODE UnitCode /*10M-EDWDM-167*/ \
      ,UNIT_INSTANCES_UNIT.NATIONAL_COURSE_CODE NationalUnitCode /*11M-EDWDM-168*/ \
      ,UNIT_INSTANCE_OCCURRENCES_COURSE.FES_MOA_CODE DeliveryMode /*12M-EDWDM-169*/ \
      ,CAST(CASE \
              WHEN ATTAINMENTS_UNIT.RECOGNITION_TYPE IN ('1','2','3') THEN ATTAINMENTS_UNIT.CREATED_DATE \
              WHEN ATTAINMENTS_UNIT.RECOGNITION_TYPE = '4' THEN ATTAINMENTS_UNIT.DATE_AWARDED \
            END AS DATE) UnitEnrolmentStartDate /*13T-EDWDM-170*/ \
      ,CAST(CASE \
              WHEN ATTAINMENTS_UNIT.RECOGNITION_TYPE IN ('1','2','3') THEN ATTAINMENTS_UNIT.CREATED_DATE \
              WHEN ATTAINMENTS_UNIT.RECOGNITION_TYPE = '4' THEN ATTAINMENTS_UNIT.DATE_AWARDED \
            END AS DATE) UnitEnrolmentEndDate /*14T-EDWDM-171*/ \
      ,'90' AvetmissDeliveryMode /*15T-EDWDM-172*/ \
      ,CASE  \
        WHEN ATTAINMENTS_UNIT.RECOGNITION_TYPE IN (1,2,3) THEN 0 \
        ELSE UNIT_INSTANCES_UNIT.MAXIMUM_HOURS \
       END UnitScheduledHours /*16T-EDWDM-173*/ \
      ,PEOPLE_UNITS_COURSE.NZ_FUNDING UnitFundingSourceCode /*17M-EDWDM-174*/ \
      ,REFERENCE_FUNDING_SOURCE.SpecificFundingCodeID as AvetmissSpecificFundingId /*18M-EDWDM-175*/ \
      ,REFERENCE_FUNDING_SOURCE.FundingSourceNationalID as AvetmissFundingSourceNationalId /*19M-EDWDM-176*/ \
      ,ATTAINMENTS_UNIT.RECOGNITION_TYPE UnitRecognitionType /*20M-EDWDM-177*/ \
      ,CASE  \
        WHEN ATTAINMENTS_UNIT.DATE_AWARDED <= '1753-01-01' THEN NULL \
        ELSE ATTAINMENTS_UNIT.DATE_AWARDED \
       END UnitAwardDate /*21M-EDWDM-178*/ \
      ,ATTAINMENTS_UNIT.GRADE AS Grade /*PRADA-814*/ \
      ,ATTAINMENTS_UNIT.ADJUSTED_GRADE AS AdjustedGrade /*PRADA-814*/ \
      ,COALESCE(ATTAINMENTS_UNIT.ADJUSTED_GRADE, ATTAINMENTS_UNIT.GRADE) UnitOutcome /*22T-EDWDM-179*/ \
      ,ATTAINMENTS_COURSE.DATE_CONFERRED AS DateConferred /*PRADA-814*/ \
      ,CASE \
        WHEN ATTAINMENTS_UNIT.RECOGNITION_TYPE IN (1,2,3) THEN '60' \
        WHEN ATTAINMENTS_UNIT.RECOGNITION_TYPE = 4 THEN '51' \
        ELSE '' \
       END SDRCompletionCode /*23T-EDWDM-180*/ \
      ,CAST(NULL AS INT) UnitHoursAttended /*24T-EDWDM-181*/ \
      ,CAST(NULL AS BIGINT) UnitOfferingId /*25T-EDWDM-182*/ \
      ,CAST(NULL AS VARCHAR(50)) UnitProgressCode /*26T-EDWDM-183*/ \
      ,CAST(NULL AS VARCHAR(50)) UnitProgressStatus /*27T-EDWDM-184*/ \
      ,CAST(NULL AS VARCHAR(50)) UnitProgressReason /*28T-EDWDM-185*/ \
      ,CAST(NULL AS VARCHAR(50)) UnitProgressDate /*29T-EDWDM-186*/ \
      ,CAST(NULL AS VARCHAR(50)) UnitSponsorOrganisationCode /*30T-EDWDM-187*/ \
      ,UNIT_INSTANCE_OCCURRENCES_COURSE.OWNING_ORGANISATION UnitOfferingFunctionalUnit /*31M-EDWDM-188*/ \
      ,ATTAINMENTS_UNIT.ATTAINMENT_CODE AttainmentCode /*32M-EDWDM-189*/ \
      ,0 AcademicHistoryMissingFlag /*33T-EDWDM-190*/ \
      ,CONFIGURABLE_STATUSES_UNIT.STATUS_CODE AS UnitLevelStatus /*PRADA-814*/ \
      ,CONFIGURABLE_STATUSES_UNIT.ACTIVE AS UnitLevelStatusActive /*PRADA-1668*/ \
      ,CONFIGURABLE_STATUSES_UNIT.STATUS_TYPE AS UnitLevelStatusType /*PRADA-1668*/ \
      ,CAST(COALESCE(cast(PEOPLE_UNIT_ATTAINMENTS.PEOPLE_UNITS_ID as int), concat(cast(WEB_CONFIG.PARAMETER_VALUE as int), cast(PEOPLE.PERSON_CODE as int))) AS INT) CourseOfferingEnrolmentId /*34T-EDWDM-191*/ \
      ,PEOPLE_UNITS_COURSE.CALOCC_CODE CourseOfferingCode /*35M-EDWDM-192*/ \
      ,UNIT_INSTANCE_OCCURRENCES_COURSE.UIO_ID CourseOfferingId /*36M-EDWDM-193*/ \
      ,UNIT_INSTANCE_OCCURRENCES_COURSE.SLOC_LOCATION_CODE CourseDeliveryLocationCode \
      ,COALESCE(UNIT_INSTANCES_COURSE.FES_UNIT_INSTANCE_CODE, concat(cast(WEB_CONFIG.PARAMETER_VALUE as int), '-999999')) CourseCode /*37M-EDWDM-195*/ \
      ,UNIT_INSTANCES_COURSE.NATIONAL_COURSE_CODE NationalCourseCode /*38M-EDWDM-196*/ \
      ,COALESCE \
       ( \
          CASE \
            WHEN UNIT_INSTANCES_COURSE.NATIONAL_COURSE_CODE IS NOT NULL THEN UNIT_INSTANCES_COURSE.NATIONAL_COURSE_CODE \
            WHEN LEFT(RIGHT(PEOPLE_UNITS_COURSE.UNIT_INSTANCE_CODE,3),1) = 'V'  \
                  AND LEFT(RIGHT(PEOPLE_UNITS_COURSE.UNIT_INSTANCE_CODE,2),1) IN ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9') THEN \
                        SUBSTRING(PEOPLE_UNITS_COURSE.UNIT_INSTANCE_CODE, 1, CHAR_LENGTH(PEOPLE_UNITS_COURSE.UNIT_INSTANCE_CODE)-3) \
            ELSE PEOPLE_UNITS_COURSE.UNIT_INSTANCE_CODE \
          END \
          , concat(cast(WEB_CONFIG.PARAMETER_VALUE as int), '-999999') \
       ) CourseCodeConverted /*39T-EDWDM-197*/ \
      ,PEOPLE_UNITS_COURSE.PROGRESS_CODE CourseProgressCode /*40M-EDWDM-198*/ \
      ,PEOPLE_UNITS_COURSE.DESTINATION CourseProgressReason /*41M-EDWDM-199*/ \
      ,PEOPLE_UNITS_COURSE.PROGRESS_STATUS CourseProgressStatus /*42M-EDWDM-200*/ \
      ,CASE  \
        WHEN PEOPLE_UNITS_COURSE.PROGRESS_DATE <= '1753-01-01' THEN NULL \
        ELSE PEOPLE_UNITS_COURSE.PROGRESS_DATE \
       END CourseProgressDate /*43M-EDWDM-201*/ \
      ,CASE  \
        WHEN ATTAINMENTS_COURSE.DATE_AWARDED <= '1753-01-01' THEN NULL \
        ELSE ATTAINMENTS_COURSE.DATE_AWARDED \
       END CourseAwardDate /*44M-EDWDM-202*/ \
      ,CAST(COALESCE(PEOPLE_UNITS_SPECIAL_COURSE.START_DATE, UNIT_INSTANCE_OCCURRENCES_COURSE.FES_START_DATE) AS DATE) CourseEnrolmentStartDate /*45T-EDWDM-203*/ \
      ,CAST(COALESCE(PEOPLE_UNITS_SPECIAL_COURSE.END_DATE, UNIT_INSTANCE_OCCURRENCES_COURSE.FES_END_DATE) AS DATE) CourseEnrolmentEndDate /*46T-EDWDM-204*/ \
      ,PEOPLE_UNITS_COURSE.NZ_FUNDING CourseFundingSourceCode /*47M-EDWDM-205*/ \
      ,ATTAINMENTS_COURSE.DESCRIPTION CourseAwardDescription /*48M-EDWDM-206*/ \
      ,CONFIGURABLE_STATUSES.STATUS_CODE CourseAwardStatus /*49M-EDWDM-207*/ \
      ,UNIT_INSTANCE_OCCURRENCES_COURSE.FES_USER_1 CourseOfferingRemarks /*50M-EDWDM-208*/ \
      ,UNIT_INSTANCE_OCCURRENCES_COURSE.OWNING_ORGANISATION CourseOfferingFunctionalUnit /*51M-EDWDM-209*/ \
      ,PEOPLE_UNITS_COURSE.TRANSITION_FROM_PEOPLE_UNIT_ID CourseEnrolmentIdTransitionFrom /*52M-EDWDM-246*/ \
      ,PEOPLE_UNITS_TRA.CourseCodeTransitionFrom /*53M-EDWDM-247*/ \
      ,PEOPLE_UNITS_TRA.CourseCaloccCodeTransitionFrom /*54M-EDWDM-248*/ \
      ,PEOPLE_UNITS_TRA.CourseEnrolmentStartDateTransitionFrom /*55T-EDWDM-249*/ \
      ,PEOPLE_UNITS_COURSE.CREATED_DATE CourseEnrolmentCreatedDate \
      ,PEOPLE_UNITS_COURSE.Study_Reason StudyReasonID /*PRADA-814*/ \
      ,StudyReason.StudyReasonDescription \
      ,REFERENCE_LOCATION.LocationName as DeliveryLocationName \
      ,CASE WHEN UNIT_INSTANCE_OCCURRENCES_COURSE.OFFERING_TYPE = 'TVET' OR PEOPLE_UNITS_COURSE.USER_8 = 'Y' THEN 'Y' ELSE 'N' END AS VetInSchoolFlag \
      ,OrgUnits.FES_FULL_NAME AS SponsorDescription \
      ,PEOPLE_UNITS_COURSE.PROGRAM_STREAM ProgramStream \
      ,CAST(CASE \
              WHEN ATTAINMENTS_UNIT.RECOGNITION_TYPE IN ('1','2','3') THEN ATTAINMENTS_UNIT.CREATED_DATE \
              WHEN ATTAINMENTS_UNIT.RECOGNITION_TYPE = '4' THEN ATTAINMENTS_UNIT.DATE_AWARDED \
            END AS DATE) UnitEndDate \
      ,ATTAINMENTS_UNIT.CREATED_DATE AttainmentCreatedDate \
      ,ATTAINMENTS_UNIT.UPDATED_DATE AttainmentUpdatedDate \
      ,NULL AS SponsorCode /* PRADA-1788 */ \
    FROM \
      trusted.OneEBS_EBS_0165_ATTAINMENTS ATTAINMENTS_UNIT \
      INNER JOIN trusted.OneEBS_EBS_0165_UNIT_INSTANCES UNIT_INSTANCES_UNIT  \
                        ON ATTAINMENTS_UNIT.SDR_COURSE_CODE = UNIT_INSTANCES_UNIT.FES_UNIT_INSTANCE_CODE \
                        AND UNIT_INSTANCES_UNIT._RecordCurrent = 1 AND UNIT_INSTANCES_UNIT._RecordDeleted = 0 \
      INNER JOIN trusted.OneEBS_EBS_0165_PEOPLE_UNIT_ATTAINMENTS PEOPLE_UNIT_ATTAINMENTS  \
                        ON ATTAINMENTS_UNIT.ATTAINMENT_CODE = PEOPLE_UNIT_ATTAINMENTS.ATTAINMENT_CODE \
                        AND PEOPLE_UNIT_ATTAINMENTS._RecordCurrent = 1 AND PEOPLE_UNIT_ATTAINMENTS._RecordDeleted = 0 \
      INNER JOIN trusted.OneEBS_EBS_0165_PEOPLE_UNITS PEOPLE_UNITS_COURSE  \
                        ON PEOPLE_UNITS_COURSE.ID = PEOPLE_UNIT_ATTAINMENTS.PEOPLE_UNITS_ID  \
                        AND PEOPLE_UNITS_COURSE._RecordCurrent = 1 AND PEOPLE_UNITS_COURSE._RecordDeleted = 0 \
      INNER JOIN trusted.OneEBS_EBS_0165_PEOPLE PEOPLE  \
                        ON PEOPLE_UNITS_COURSE.PERSON_CODE = PEOPLE.PERSON_CODE \
                        AND PEOPLE._RecordCurrent = 1 AND PEOPLE._RecordDeleted = 0 \
      INNER JOIN trusted.OneEBS_EBS_0165_UNIT_INSTANCE_OCCURRENCES UNIT_INSTANCE_OCCURRENCES_COURSE  \
                        ON PEOPLE_UNITS_COURSE.UIO_ID = UNIT_INSTANCE_OCCURRENCES_COURSE.UIO_ID \
                        AND UNIT_INSTANCE_OCCURRENCES_COURSE._RecordCurrent = 1 AND UNIT_INSTANCE_OCCURRENCES_COURSE._RecordDeleted = 0 \
      INNER JOIN trusted.OneEBS_EBS_0165_UNIT_INSTANCES UNIT_INSTANCES_COURSE  \
                        ON PEOPLE_UNITS_COURSE.UNIT_INSTANCE_CODE = UNIT_INSTANCES_COURSE.FES_UNIT_INSTANCE_CODE  \
                        AND UNIT_INSTANCES_COURSE.UI_LEVEL = 'COURSE' \
                        AND UNIT_INSTANCES_COURSE._RecordCurrent = 1 AND UNIT_INSTANCES_COURSE._RecordDeleted = 0 \
      INNER JOIN compliance.avetmiss_location REFERENCE_LOCATION  \
                        ON UNIT_INSTANCE_OCCURRENCES_COURSE.SLOC_LOCATION_CODE = REFERENCE_LOCATION.LocationCode \
                        AND REFERENCE_LOCATION._RecordCurrent = 1 AND REFERENCE_LOCATION._RecordDeleted = 0 \
      CROSS JOIN trusted.OneEBS_EBS_0165_WEB_CONFIG WEB_CONFIG \
                        ON WEB_CONFIG.PARAMETER = 'INST_CODE' \
                        AND WEB_CONFIG._RecordCurrent = 1 AND WEB_CONFIG._RecordDeleted = 0 \
      LEFT JOIN trusted.OneEBS_EBS_0165_PEOPLE_UNITS_SPECIAL PEOPLE_UNITS_SPECIAL_COURSE  \
                        ON PEOPLE_UNITS_COURSE.ID = PEOPLE_UNITS_SPECIAL_COURSE.PEOPLE_UNITS_ID \
                        AND PEOPLE_UNITS_SPECIAL_COURSE._RecordCurrent = 1 AND PEOPLE_UNITS_SPECIAL_COURSE._RecordDeleted = 0 \
      LEFT JOIN \
        ( \
            select * \
            from \
            ( \
                select *, row_number() over(partition by PEOPLE_UNITS_ID order by coalesce(UPDATED_DATE, CREATED_DATE) desc, ATTAINMENT_CODE desc) rn \
                from trusted.OneEBS_EBS_0165_ATTAINMENTS \
                where _RecordCurrent = 1 AND _RecordDeleted = 0 \
            ) x \
            where x.rn = 1 \
        ) ATTAINMENTS_COURSE ON PEOPLE_UNITS_COURSE.ID = ATTAINMENTS_COURSE.PEOPLE_UNITS_ID \
      LEFT JOIN reference.funding_source REFERENCE_FUNDING_SOURCE \
                        ON PEOPLE_UNITS_COURSE.NZ_FUNDING = REFERENCE_FUNDING_SOURCE.FundingSourceCode \
                        AND REFERENCE_FUNDING_SOURCE.Active = 'Y' \
                        AND REFERENCE_FUNDING_SOURCE._RecordCurrent = 1 AND REFERENCE_FUNDING_SOURCE._RecordDeleted = 0 \
      LEFT JOIN trusted.OneEBS_EBS_0165_CONFIGURABLE_STATUSES CONFIGURABLE_STATUSES  \
                    ON ATTAINMENTS_COURSE.CONFIGURABLE_STATUS_ID = CONFIGURABLE_STATUSES.ID  \
                    AND CONFIGURABLE_STATUSES._RecordCurrent = 1 AND CONFIGURABLE_STATUSES._RecordDeleted = 0 \
      LEFT JOIN Reference.Study_Reason StudyReason \
                  ON StudyReason.StudyReasonID = PEOPLE_UNITS_COURSE.Study_Reason \
                  AND StudyReason._RecordCurrent = 1 AND StudyReason._RecordDeleted = 0 \
      LEFT JOIN trusted.OneEBS_EBS_0165_ORGANISATION_UNITS OrgUnits \
          ON OrgUnits.organisation_code = UNIT_INSTANCE_OCCURRENCES_COURSE.OWNING_ORGANISATION \
          AND OrgUnits._RecordCurrent = 1 AND OrgUnits._RecordDeleted = 0 \
      LEFT JOIN  \
      ( \
        SELECT  \
          PEOPLE_UNITS.ID,  \
          PEOPLE_UNITS.UNIT_INSTANCE_CODE CourseCodeTransitionFrom,  \
          PEOPLE_UNITS.CALOCC_CODE CourseCaloccCodeTransitionFrom,  \
          COALESCE(PEOPLE_UNITS_SPECIAL.START_DATE, UNIT_INSTANCE_OCCURRENCES.FES_START_DATE) CourseEnrolmentStartDateTransitionFrom \
        FROM trusted.OneEBS_EBS_0165_PEOPLE_UNITS PEOPLE_UNITS \
          INNER JOIN trusted.OneEBS_EBS_0165_UNIT_INSTANCE_OCCURRENCES UNIT_INSTANCE_OCCURRENCES \
                        ON PEOPLE_UNITS.UIO_ID = UNIT_INSTANCE_OCCURRENCES.UIO_ID \
                        AND UNIT_INSTANCE_OCCURRENCES._RecordCurrent = 1 AND UNIT_INSTANCE_OCCURRENCES._RecordDeleted = 0 \
          LEFT JOIN trusted.OneEBS_EBS_0165_PEOPLE_UNITS_SPECIAL PEOPLE_UNITS_SPECIAL \
                        ON PEOPLE_UNITS.ID = PEOPLE_UNITS_SPECIAL.PEOPLE_UNITS_ID \
                        AND PEOPLE_UNITS_SPECIAL._RecordCurrent = 1 AND PEOPLE_UNITS_SPECIAL._RecordDeleted = 0 \
        WHERE 1=1 \
        AND PEOPLE_UNITS._RecordCurrent = 1 AND PEOPLE_UNITS._RecordDeleted = 0 \
      ) PEOPLE_UNITS_TRA ON PEOPLE_UNITS_COURSE.TRANSITION_FROM_PEOPLE_UNIT_ID = PEOPLE_UNITS_TRA.ID  \
      LEFT JOIN trusted.OneEBS_EBS_0165_CONFIGURABLE_STATUSES CONFIGURABLE_STATUSES_UNIT  \
                  ON CONFIGURABLE_STATUSES_UNIT.ID = ATTAINMENTS_UNIT.CONFIGURABLE_STATUS_ID \
                  AND CONFIGURABLE_STATUSES_UNIT._RecordCurrent = 1 AND CONFIGURABLE_STATUSES_UNIT._RecordDeleted = 0 \
    WHERE 1=1 \
      AND ATTAINMENTS_UNIT.ATYPE_ATTAINMENT_TYPE_CODE = 'UNIT' \
      AND ATTAINMENTS_UNIT.RECOGNITION_TYPE IN ('1','2','3','4') \
      AND PEOPLE_UNITS_COURSE.UNIT_TYPE = 'R' \
      AND CASE \
            WHEN ATTAINMENTS_UNIT.RECOGNITION_TYPE IN ('1','2','3') THEN ATTAINMENTS_UNIT.CREATED_DATE \
            WHEN ATTAINMENTS_UNIT.RECOGNITION_TYPE = '4' THEN ATTAINMENTS_UNIT.DATE_AWARDED \
          END > '1753-01-01' \
      AND ATTAINMENTS_UNIT._RecordCurrent = 1 AND ATTAINMENTS_UNIT._RecordDeleted = 0 \
    ")
  
  return dfAttainments

# COMMAND ----------

def GetAvetmissUnitEnrolment(dfAvetCourse, dfCurrentReportingPeriod, RefreshLinkSP):
  # ##########################################################################################################################  
  # Description - Returns a dataframe which contains Unit Enrolments 
  # ##########################################################################################################################  
  LogEtl("Starting Avetmiss Unit Enrolment...")
  
  if RefreshLinkSP:
    LogEtl("Collecting unit enrolment to course enrolment links recursively - starts...")
    AzSqlExecTSQL("exec compliance.PopulatePeopleUnitLinks")
    LogEtl("Collecting unit enrolment to course enrolment links recursively - ends")
    
  dfPUL = AzSqlGetData("compliance.AvetmissPeopleUnitLinks")
  dfPUL.createOrReplaceTempView("AvetmissPeopleUnitLinks")
  
  dfPeopleUnit = _PeopleUnit()
  #add reporting year and reporting date
  dfPeopleUnit = dfPeopleUnit.join(dfCurrentReportingPeriod, ((dfPeopleUnit.UnitEnrolmentStartDate < dfCurrentReportingPeriod.ReportingYearEndDate) & (dfPeopleUnit.UnitEnrolmentStartDate <= dfPeopleUnit.UnitEnrolmentEndDate) & (dfPeopleUnit.UnitEnrolmentEndDate >= dfCurrentReportingPeriod.ReportingYearStartDate)), how="inner")
  
  dfAttainments = _Attainments()
  #add reporting year and reporting date
  dfAttainments = dfAttainments.join(dfCurrentReportingPeriod, ((dfAttainments.UnitEnrolmentStartDate >= dfCurrentReportingPeriod.ReportingYearStartDate) & (dfAttainments.UnitEnrolmentStartDate <= dfCurrentReportingPeriod.ReportingYearEndDate)), how="inner")
    
  #combine the two
  dfUnitEnrolment = dfPeopleUnit.union(dfAttainments)
  
  #Save the data frame to the Stage Database. The performance for this query is very slow. The explicit persist makes it easier.
  #It takes around 40 mintues to save the dataframe. But then it is quick.
  LogEtl("Saving to stage database - Start")
  stage_table_name = "stage.avetmiss_unit_enrolment_temp"
  spark.sql(f"DROP TABLE IF EXISTS {stage_table_name}")
  #Save the dataframe temporarily to Stage database
  dfUnitEnrolment.write.saveAsTable(stage_table_name)
  dfUnitEnrolment = spark.table(stage_table_name)
  LogEtl("Saving to stage database - Complete")
  
  #add course10
  dfReferenceAvetmissCourse = GeneralAliasDataFrameColumns(dfAvetCourse, 'RAC_')
  dfUnitEnrolment = dfUnitEnrolment.join(dfReferenceAvetmissCourse, dfUnitEnrolment.CourseCodeConverted == dfReferenceAvetmissCourse.RAC_CourseCodeConverted, how="left") \
        .withColumn("AvetmissCourseCode", when(col("RAC_CourseCodeConverted").isNotNull(), col("RAC_AvetmissCourseCode")).otherwise(dfUnitEnrolment.CourseCodeConverted.substr(1,10)))
  
  #flag the rows which need to be excluded from taking forward
  DeleteRowsFlag = "case \
                      when \
                        /*d.*/ (UnitOutcome = 'NS' ) \
                        OR /*e.i.*/ (RecordSource = 'Academic Record' AND UnitRecognitionType IN ('1','2','3') AND (UnitEnrolmentStartDate NOT BETWEEN ReportingYearStartDate AND ReportingYearEndDate OR UnitEnrolmentStartDate > CourseEnrolmentEndDate)) \
                        OR /*e.ii.*/ (RecordSource = 'Academic Record' AND UnitRecognitionType = '4' AND UnitEnrolmentStartDate NOT BETWEEN ReportingYearStartDate AND ReportingYearEndDate ) \
                        OR (datediff(ReportingYearEndDate, UnitEnrolmentStartDate)/365.25 > 5) \
                        OR (UnitRecognitionType = '5') then 1 \
                      else 0 \
                    end "
  dfUnitEnrolment = dfUnitEnrolment.withColumn("DeleteRowsFlag", expr(DeleteRowsFlag))

  #add calculated columns
  dfUnitEnrolment = dfUnitEnrolment \
        .withColumn("CommencingProgramIdentifier", \
              when(((col("CourseOfferingEnrolmentID").isNull()) | (col("UnitOfferingEnrolmentId") == col("CourseOfferingEnrolmentID"))), "Unit of competency or module enrolment only") \
              .when(col("CourseEnrolmentStartDate") < col("ReportingYearStartDate"), "Continuing enrolment in the program from a previous year")
              .when(col("CourseEnrolmentStartDate") >= col("ReportingYearStartDate"), "Commencing enrolment in the program"))
  
  dfUnitEnrolment = dfUnitEnrolment \
        .withColumn("ID_UUIO", concat(col("ReportingYear"), col("InstituteId"), col("UnitOfferingId").cast('integer'))) \
        .withColumn("ID_UPU", concat(col("ReportingYear"), col("InstituteId"), col("UnitOfferingEnrolmentId"))) \
        .withColumn("ID_CPU", concat(col("ReportingYear"), col("InstituteId"), col("UnitDeliveryLocationCode"), col("StudentId").cast('integer'), col("AvetmissCourseCode"))) \
        .withColumn("ID_PC", concat(col("ReportingYear"), col("InstituteId"), col("StudentId").cast('integer')))
  
  #EDWDM-254
  OutcomeNational = "CASE \
            WHEN RecordSource = 'Unit Offering Enrolment' THEN /* Data is collected from PEOPLE_UNITS */ \
                CASE \
                    WHEN UnitRecognitionType IN (1,2,3) THEN '60' \
                    WHEN UnitRecognitionType = 4 THEN '51' \
                    WHEN (coalesce(UnitOutcome, '') = '' AND UnitEndDate > ReportingYearEndDate) \
                            OR (UnitAwardDate > ReportingYearEndDate AND YEAR(UnitAwardDate) <= YEAR(UnitEndDate)) THEN '70' \
                    WHEN coalesce(UnitOutcome, '') = '' AND UnitEndDate >= ReportingDate THEN '90' \
                    ELSE SDRCompletionCode \
                END \
            ELSE /* Data is collected from ATTAINMENTS */ \
                CASE \
                    WHEN UnitRecognitionType IN ('1','2','3') THEN '60' \
                    WHEN UnitRecognitionType = '4' THEN '51' \
                    ELSE '' \
                END  \
            END"

  dfUnitEnrolment = dfUnitEnrolment.withColumn("OutcomeNational", expr(OutcomeNational))
  
  OutcomeNational1 = "case \
                        when UnitEndDate between ReportingYearStartDate and ReportingYearEndDate \
                                AND OutcomeNational = '70' \
                                AND coalesce(UnitOutcome, '') = '' then '' \
                        when UnitOutcome = 'RPL' then '51' \
                        else OutcomeNational \
                      end"

  dfUnitEnrolment = dfUnitEnrolment.withColumn("OutcomeNational", expr(OutcomeNational1))
  
  #EDWDM-255
  ValidFlag ="\
        CASE \
          WHEN /*a.i.*/ CourseProgressCode IN ('0.0 CANCRG','0.1 STBY','0.2 AP', '0.2 ROI','0.3 TVHPEN','0.4 AS','0.5 AO','0.5 INC','0.6 AD','0.7PAIDNS','0.8 A/T UN','0.9 RENR','1.0 UNPAID','3.3WN','3.4NS','0.71TFCMAN','0.72TFCERR','0.25 VSLAE','0.23 TESTE','0.24 EVIE','0.25VSLAEE','0.12 AW','0.21 EP','0.23 TESTE','0.24 EVIE','0.7 AA') \
          OR /*a.ii.*/ (CourseProgressStatus IN ('L','N')) \
          OR /*b.i.*/ (CourseProgressStatus = 'W' AND CourseProgressDate BETWEEN ReportingYearStartDate AND ReportingYearEndDate AND UnitEnrolmentEndDate > ReportingYearEndDate AND COALESCE(OutcomeNational, '') = '') \
          OR /*b.ii.*/ (CourseProgressStatus = 'W' AND CourseProgressDate < ReportingYearStartDate AND UnitEnrolmentEndDate > ReportingYearStartDate AND COALESCE(OutcomeNational, '') = '') \
          OR /*c.*/ (CourseProgressCode IN ('3.1 WD','2.1 WDTFR') AND CourseProgressDate < ReportingYearStartDate) \
          OR /*d.*/ (CourseProgressReason IN ('WDSBC', 'CNPORT','WDCC','WDEC')) /*('Student Withdraws Before Class','Cancelled via Portal','Withdrawal Course Cancelled','Withdrawal Error Correct')*/ \
          OR (AvetmissCourseCode in ('168-LSAB','168-LSD','168-LSFS','168-LSG','161-N1000','161-N2000','161-N2001','161-N2002')) /*PRADA-1318*/ \
          THEN 0 \
        ELSE 1 END"
  dfUnitEnrolment = dfUnitEnrolment.withColumn("ValidFlag", expr(ValidFlag))

  #EDWDM-260
  dfUnitEnrolment = dfUnitEnrolment.withColumn("AvetmissScheduledHours", 
                           expr("CAST(CASE WHEN OutcomeNational IN ('60','61') THEN 0 ELSE UnitScheduledHours END AS INT)"))

  #EDWDM-261
  dfUnitEnrolment = dfUnitEnrolment.withColumn("Semester1Hours", 
                           expr("CAST(CASE \
                                        WHEN DATE_FORMAT(UnitEnrolmentEndDate, 'MMdd') < 0701 THEN AvetmissScheduledHours \
                                        WHEN DATE_FORMAT(UnitEnrolmentStartDate, 'MMdd') < 0701 \
                                              AND DATE_FORMAT(UnitEnrolmentEndDate, 'MMdd') >= 0701 \
                                          THEN AvetmissScheduledHours * 0.5 \
                                        ELSE 0 \
                                      END AS INT)"))
 
  #EDWDM-262
  dfUnitEnrolment = dfUnitEnrolment.withColumn("Semester2Hours", 
                           expr("CAST(CASE \
                                        WHEN DATE_FORMAT(UnitEnrolmentStartDate, 'MMdd') >= 0701 THEN AvetmissScheduledHours \
                                        WHEN DATE_FORMAT(UnitEnrolmentStartDate, 'MMdd') < 0701 \
                                              AND DATE_FORMAT(UnitEnrolmentEndDate, 'MMdd') >= 0701 \
                                          THEN AvetmissScheduledHours * 0.5 \
                                        ELSE 0 \
                                      END AS INT)"))

  dfUnitEnrolment = dfUnitEnrolment.withColumn("OriginalDeliveryMode", col("DeliveryMode"))
  dfUnitEnrolment = dfUnitEnrolment.withColumn("DeliveryMode", 
                           expr("CASE WHEN OutcomeNational IN ('51','52','60','61') THEN 'ADVD' ELSE DeliveryMode END"))

  dfUnitEnrolment = dfUnitEnrolment.withColumn("FinalUnit", when(col("OutcomeNational").isin(['20','30','40','51','52','81','82','70']), 1).otherwise(lit("0")))
  
  #De-Duplication / Ranking
  Rank = "ROW_NUMBER() OVER(PARTITION BY ReportingYear, AvetmissClientId, AvetmissCourseCode, UnitDeliveryLocationCode, AvetmissUnitCode, UnitEnrolmentStartDate ORDER BY DeleteRowsFlag, ValidFlag DESC, \
    CASE \
      WHEN OutcomeNational='20' THEN 1 \
      WHEN OutcomeNational='51' THEN 2 \
      WHEN OutcomeNational='60' THEN 3 \
      WHEN OutcomeNational='30' THEN 4 \
      WHEN OutcomeNational='40' THEN 5 \
      WHEN OutcomeNational='70' THEN 6 \
      WHEN OutcomeNational='81' THEN 7 \
      WHEN OutcomeNational='82' THEN 8 \
      ELSE 9 \
    END ASC \
    , CourseOfferingEnrolmentId DESC \
    , UnitAwardDate DESC \
    , UnitScheduledHours DESC \
    , UnitEnrolmentId DESC )"
  dfUnitEnrolment = dfUnitEnrolment.withColumn("Rank", expr(Rank)).filter(col("Rank") == 1)

  #EDWDM-256
  IncludeFlag = "\
        CASE \
          WHEN \
            /*a.*/ (AcademicHistoryMissingFlag = 1) \
            OR /*b.i.*/ (CourseAwardDate < ReportingYearStartDate AND COALESCE(OutcomeNational, '') IN ('90','0','','70','71') AND coalesce(CourseProgressStatus, '') <> 'A') \
            OR /*b.ii.*/ (CourseAwardDate between ReportingYearStartDate and ReportingYearEndDate AND UnitEnrolmentEndDate > ReportingYearEndDate AND COALESCE(OutcomeNational, '') IN ('90','0','','70','71') AND coalesce(CourseProgressStatus, '') <> 'A') \
            OR /*c.*/ (UnitAwardDate IS NOT NULL AND UnitAwardDate < ReportingYearStartDate AND COALESCE(OutcomeNational, '') not in ('51','52','0','90',' ','70','71') ) \
            OR DeleteRowsFlag = 1 \
          THEN 0 \
          ELSE 1 \
        END"
  dfUnitEnrolment = dfUnitEnrolment.withColumn("IncludeFlag", expr(IncludeFlag))
    
  #add ProgramStartDate and other related columns
  dfCourseCommencement = dfUnitEnrolment.filter((col("ValidFlag") == 1)) \
      .groupBy("AvetmissClientId", "AvetmissCourseCode", "CourseEnrolmentIdTransitionFrom", "RecordSource") \
      .agg(min(col("UnitEnrolmentStartDate")).alias("CC_ProgramStartDate"))
  
  #Get any Transition Dates for Units
  dfCourseCommencement = GetTransitionDateUnits(dfCourseCommencement)
  #Get any Transition Dates for Attainments
  dfCourseCommencement = GetTransitionDateAttainments(dfCourseCommencement)
  #Store the updated date from Transition
  dfCourseCommencement = dfCourseCommencement.withColumn("CC_ProgramStartDate", coalesce("UpdatedProgramDateUnit", "UpdatedProgramDateAttainments", "CC_ProgramStartDate").cast(DateType())) 

  dfCourseCommencement = dfCourseCommencement \
    .withColumn("CC_CourseCommencementYear", year(col("CC_ProgramStartDate"))) \
    .withColumn("CC_CourseCommencementSemester", when(date_format(col("CC_ProgramStartDate"), "MMdd").between("0101", "0630"), concat(year(col("CC_ProgramStartDate")), lit(" Sem 1")))\
               .otherwise(concat(year(col("CC_ProgramStartDate")), lit(" Sem 2"))))
  
  dfCourseCommencement = dfCourseCommencement.selectExpr(
        "AvetmissClientId as CC_AvetmissClientId", 
        "AvetmissCourseCode as CC_AvetmissCourseCode", 
        "CC_ProgramStartDate", 
        "CC_CourseCommencementYear", 
        "CC_CourseCommencementSemester")
  
  dfUnitEnrolment = dfUnitEnrolment.join(dfCourseCommencement, (dfUnitEnrolment.AvetmissClientId == dfCourseCommencement.CC_AvetmissClientId) & (dfUnitEnrolment.AvetmissCourseCode == dfCourseCommencement.CC_AvetmissCourseCode), how="left")
  
  

  #select columns
  dfUnitEnrolment = dfUnitEnrolment.selectExpr('RecordSource', 'InstituteId', 'UnitEnrolmentId', 'UnitDeliveryLocationCode', 'AvetmissClientId', 'AvetmissClientIdMissingFlag', 'StudentId', 'UnitOfferingEnrolmentId', 'UnitOfferingCode', 'AvetmissUnitCode', 'UnitCode', 'NationalUnitCode', 'DeliveryMode', 'OriginalDeliveryMode', 'UnitEnrolmentStartDate', 'UnitEnrolmentEndDate', 'AvetmissDeliveryMode', 'UnitScheduledHours', 'UnitFundingSourceCode', 'AvetmissSpecificFundingId', 'AvetmissFundingSourceNationalId', 'UnitRecognitionType', 'UnitAwardDate', 'Grade', 'AdjustedGrade', 'UnitOutcome', 'DateConferred', 'SDRCompletionCode', 'UnitHoursAttended', 'UnitOfferingId', 'UnitProgressCode', 'UnitProgressStatus', 'UnitProgressReason', 'UnitProgressDate', 'UnitSponsorOrganisationCode', 'UnitOfferingFunctionalUnit', 'AttainmentCode', 'AcademicHistoryMissingFlag', 'UnitLevelStatus', 'UnitLevelStatusActive', 'UnitLevelStatusType', 'CourseOfferingEnrolmentId', 'CourseOfferingCode', 'CourseOfferingId', 'CourseDeliveryLocationCode', 'CourseCode', 'NationalCourseCode', 'CourseCodeConverted', 'CourseProgressCode', 'CourseProgressReason', 'CourseProgressStatus', 'CourseProgressDate', 'CourseAwardDate', 'CourseEnrolmentStartDate', 'CourseEnrolmentEndDate', 'CourseFundingSourceCode', 'CourseAwardDescription', 'CourseAwardStatus', 'CourseOfferingRemarks', 'CourseOfferingFunctionalUnit', 'CourseEnrolmentIdTransitionFrom', 'CourseCodeTransitionFrom', 'CourseCaloccCodeTransitionFrom', 'CourseEnrolmentStartDateTransitionFrom', 'CourseEnrolmentCreatedDate', 'StudyReasonID', 'StudyReasonDescription', 'DeliveryLocationName', 'VetInSchoolFlag', 'ReportingYear', 'ReportingYearStartDate', 'ReportingYearEndDate', 'AvetmissCourseCode', 'CC_ProgramStartDate as ProgramStartDate', 'CC_CourseCommencementYear as CourseCommencementYear', 'CC_CourseCommencementSemester as CourseCommencementSemester', 'CommencingProgramIdentifier', 'OutcomeNational', 'ValidFlag', 'IncludeFlag', 'AvetmissScheduledHours', 'Semester1Hours', 'Semester2Hours', 'FinalUnit', 'ID_UUIO', 'ID_UPU', 'ID_CPU', 'ID_PC', 'SponsorDescription', 'ProgramStream', 'ReportingDate', 'UnitEndDate', 'AttainmentCreatedDate', 'AttainmentUpdatedDate', 'SponsorCode').dropDuplicates()  
  
  return dfUnitEnrolment

# COMMAND ----------

def GetTransitionDateUnits(dfCourse):
  
  sql = "select \
  pu_course.id as CourseID \
  ,min(coalesce(pus.start_date,uio_unit.fes_start_date)) as StartDate \
  from trusted.oneebs_ebs_0165_people_units pu_units \
  inner join trusted.oneebs_ebs_0165_unit_instance_occurrences uio_unit on uio_unit.uio_id=pu_units.uio_id \
  inner join trusted.oneebs_ebs_0165_unit_instances ui_unit on ui_unit.fes_unit_instance_code=uio_unit.fes_uins_instance_code \
  left join trusted.oneebs_ebs_0165_people_units_special pus on pus.people_units_id=pu_units.id \
  inner join trusted.oneebs_ebs_0165_people_unit_links pul on pul.child_id=pu_units.id \
  inner join trusted.oneebs_ebs_0165_people_units pu_course on pu_course.id=pul.parent_id \
  inner join trusted.oneebs_ebs_0165_unit_instance_occurrences uio_course on uio_course.uio_id=pu_units.uio_id \
  inner join trusted.oneebs_ebs_0165_unit_instances ui_course on ui_course.fes_unit_instance_code=uio_unit.fes_uins_instance_code \
  left join trusted.oneebs_ebs_0165_attainments att on att.people_units_id = pu_units.id and att._RecordCurrent = 1 and att._RecordDeleted = 0 \
  where 1 = 1 \
  and pu_units._RecordCurrent = 1 and pu_units._RecordDeleted = 0 \
  and uio_unit._RecordCurrent = 1 and uio_unit._RecordDeleted = 0 \
  and ui_unit._RecordCurrent = 1 and ui_unit._RecordDeleted = 0 \
  and pus._RecordCurrent = 1 and pus._RecordDeleted = 0 \
  and pul._RecordCurrent = 1 and pul._RecordDeleted = 0 \
  and pu_course._RecordCurrent = 1 and pu_course._RecordDeleted = 0 \
  and uio_course._RecordCurrent = 1 and uio_course._RecordDeleted = 0 \
  and ui_course._RecordCurrent = 1 and ui_course._RecordDeleted = 0 \
  and ui_unit.ui_level='UNIT' \
  and att.RECOGNITION_TYPE IN ('4') \
  group by pu_course.id "
  dfCourseTransition = spark.sql(sql)
  
  dfCourseUnit = dfCourse.join(dfCourseTransition, (dfCourse.CourseEnrolmentIdTransitionFrom == dfCourseTransition.CourseID) & (dfCourse.RecordSource == "Unit Offering Enrolment"), how="left")
  dfCourseUnit = dfCourseUnit \
    .withColumn("UpdatedProgramDateUnit", dfCourseUnit.StartDate) \
    .drop("CourseID", "StartDate")
  
  return dfCourseUnit

  

# COMMAND ----------

def GetTransitionDateAttainments(dfCourse):
  
  sql = "select \
  pu_course.id as CourseID \
  ,min(AT.DATE_AWARDED) AS StartDate \
  FROM trusted.oneebs_ebs_0165_ATTAINMENTS AT \
    LEFT JOIN trusted.oneebs_ebs_0165_GRADING_SCHEME_GRADES GSG ON GSG.GRADING_SCHEME_ID = AT.GRADING_SCHEME_ID AND COALESCE(AT.ADJUSTED_GRADE,AT.GRADE) = GSG.GRADE \
    INNER JOIN trusted.oneebs_ebs_0165_UNIT_INSTANCES UI_UNIT ON UI_UNIT.FES_UNIT_INSTANCE_CODE = AT.SDR_COURSE_CODE \
    INNER JOIN trusted.oneebs_ebs_0165_PEOPLE_UNIT_ATTAINMENTS PUA ON PUA.ATTAINMENT_CODE = AT.ATTAINMENT_CODE \
    INNER JOIN trusted.oneebs_ebs_0165_PEOPLE_UNITS PU_COURSE ON PU_COURSE.ID = PUA.PEOPLE_UNITS_ID AND PU_COURSE.UNIT_TYPE = 'R' \
    LEFT JOIN trusted.oneebs_ebs_0165_PEOPLE_UNITS_SPECIAL PUS_COURSE ON PU_COURSE.ID = PUS_COURSE.PEOPLE_UNITS_ID \
  where 1 = 1 \
  and AT._RecordCurrent = 1 and AT._RecordDeleted = 0 \
  and GSG._RecordCurrent = 1 and GSG._RecordDeleted = 0 \
  and UI_UNIT._RecordCurrent = 1 and UI_UNIT._RecordDeleted = 0 \
  and PUA._RecordCurrent = 1 and PUA._RecordDeleted = 0 \
  and PU_COURSE._RecordCurrent = 1 and PU_COURSE._RecordDeleted = 0 \
  and PUS_COURSE._RecordCurrent = 1 and PUS_COURSE._RecordDeleted = 0 \
  and AT.RECOGNITION_TYPE IN ('4') \
  group by pu_course.id"
  dfCourseTransition = spark.sql(sql)
  
  dfCourseAt = dfCourse.join(dfCourseTransition, (dfCourse.CourseEnrolmentIdTransitionFrom == dfCourseTransition.CourseID) & (dfCourse.RecordSource == "Academic Record"), how="left")
  dfCourseAt = dfCourseAt \
    .withColumn("UpdatedProgramDateAttainments", dfCourseAt.StartDate) \
    .drop("CourseID", "StartDate")
  
  return dfCourseAt
  
