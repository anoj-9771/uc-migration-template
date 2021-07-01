# Databricks notebook source
def GetNatUnits_PeopleUnits(params,dfBase,rtoCode):
  dfPUL=GeneralAliasDataFrameColumns(spark.table("trusted.OneEBS_EBS_0165_PEOPLE_UNIT_LINKS").filter(col("_RecordCurrent") == 1).filter(col("_RecordDeleted") == 0), "PUL_")
  dfPU=GeneralAliasDataFrameColumns(spark.table("trusted.oneebs_ebs_0165_people_units").filter(col("_RecordCurrent") == 1).filter(col("_RecordDeleted") == 0), "PU_")
  dfUIO=GeneralAliasDataFrameColumns(spark.table("trusted.OneEBS_EBS_0165_UNIT_INSTANCE_OCCURRENCES").filter(col("_RecordCurrent") == 1).filter(col("_RecordDeleted") == 0), "UIO_")
  dfUI=GeneralAliasDataFrameColumns(spark.table("trusted.OneEBS_EBS_0165_UNIT_INSTANCES").filter(col("_RecordCurrent") == 1).filter(col("_RecordDeleted") == 0), "UI_")
  dfPUS=GeneralAliasDataFrameColumns(spark.table("trusted.OneEBS_EBS_0165_PEOPLE_UNITS_SPECIAL").filter((col("_RecordCurrent") == 1)).filter((col("_RecordDeleted") == 0)), "PUS_")
  dfAT=GeneralAliasDataFrameColumns(spark.table("trusted.OneEBS_EBS_0165_ATTAINMENTS").filter((col("_RecordCurrent") == 1)).filter((col("_RecordDeleted") == 0)), "AT_")
  dfCS=GeneralAliasDataFrameColumns(spark.table("trusted.OneEBS_EBS_0165_CONFIGURABLE_STATUSES").filter((col("_RecordCurrent") == 1)).filter((col("_RecordDeleted") == 0)).filter(~(col("STATUS_CODE").isin('INVALID'))), "CS_")
  dfGSG=GeneralAliasDataFrameColumns(spark.table("trusted.OneEBS_EBS_0165_GRADING_SCHEME_GRADES").filter((col("_RecordCurrent") == 1)).filter((col("_RecordDeleted") == 0)), "GSG_")
  dfVP=GeneralAliasDataFrameColumns(spark.table("trusted.OneEBS_EBS_0165_VERIFIER_PROPERTIES") \
                                    .filter((col("_RecordCurrent") == 1)) \
                                    .filter((col("_RecordDeleted") == 0)) \
                                    .filter(col("RV_DOMAIN")=='FUNDING') \
                                    .filter(col("PROPERTY_NAME")=='DEC_FUNDING_SOURCE_NATIONAL_2015'), "VP_")
  
  #joins
  df=dfPUL.join(dfBase,dfBase.BS_CourseOfferingEnrolmentId==dfPUL.PUL_PARENT_ID,how="inner")
  df=df.join(dfPU ,df.PUL_CHILD_ID == dfPU.PU_ID,how="inner")
  df=df.join(dfUIO,df.PU_UIO_ID == dfUIO.UIO_UIO_ID,how="inner") 
  df=df.join(dfUI ,df.PU_UNIT_INSTANCE_CODE == dfUI.UI_FES_UNIT_INSTANCE_CODE,how="inner") 
  df=df.join(dfPUS,df.PU_ID == dfPUS.PUS_PEOPLE_UNITS_ID,how="left") 
  df=df.join(dfAT ,df.PU_ID == dfAT.AT_PEOPLE_UNITS_ID,how="left") 
  df=df.join(dfCS ,df.AT_CONFIGURABLE_STATUS_ID == dfCS.CS_ID,how="left") 
  df=df.join(dfGSG,(df.AT_GRADING_SCHEME_ID == dfGSG.GSG_GRADING_SCHEME_ID) & (coalesce(col("AT_ADJUSTED_GRADE"),col("AT_GRADE"))==dfGSG.GSG_GRADE),how="left") 
  df=df.join(dfVP ,dfVP.VP_LOW_VALUE==df.PU_NZ_FUNDING,how="left")
  
  #where clause
  df=df.where(expr("(CS_STATUS_CODE IS NOT NULL OR (CS_STATUS_CODE is null and COALESCE(AT_ADJUSTED_GRADE,AT_GRADE) IS NULL)) \
        AND CASE WHEN COALESCE(COALESCE(AT_ADJUSTED_GRADE,AT_GRADE),'ZZZ') = 'CT' AND AT_RECOGNITION_TYPE <> '3' THEN 'IGNORE'  \
            WHEN COALESCE(COALESCE(AT_ADJUSTED_GRADE,AT_GRADE),'ZZZ') = 'NS' AND COALESCE(BS_OUTCOME_ID_TRAINING_ORG, 'ZZ') NOT IN('TNC','D') THEN 'IGNORE'  \
            WHEN COALESCE(COALESCE(AT_ADJUSTED_GRADE,AT_GRADE),'ZZZ') = 'SU' \
                 and cast(COALESCE(PUS_START_DATE, UIO_FES_START_DATE) as date) < cast('1-JAN-2018' as date) THEN 'IGNORE' \
            ELSE 'ALLGOOD' \
          END <> 'IGNORE' \
        AND COALESCE(AT_RECOGNITION_TYPE,'ZZ') IN ('4','3','ZZ')  \
        AND UI_UI_LEVEL = 'UNIT' \
        AND PU_UNIT_TYPE= 'R' \
        AND UI_UNIT_CATEGORY <> 'BOS'  \
        AND PU_progress_status not in ('T') \
        AND PU_NZ_FUNDING NOT IN ('8411','841','001Z', '00123', '00126', '00127', '00129')"))
  df=df.withColumn("FUNDING_SPECIFIC",lit('11'))
  #select columns
  df=df.selectExpr("BS_TRAINING_ORGANISATION_ID TRAINING_ORGANISATION_ID"
        ,"BS_RTOCode RTO_CODE" 
        ,"UIO_SLOC_LOCATION_CODE SLOC_LOCATION_CODE"
        ,"BS_REGISTRATION_NO REGISTRATION_NO"
        ,"PU_PERSON_CODE PERSON_CODE"
        ,"PU_UIO_ID UIO_ID"
        ,"PU_ID ID"
        ,"COALESCE(UI_NATIONAL_COURSE_CODE,UI_FES_UNIT_INSTANCE_CODE) AS SUBJECT_ID"
        ,"BS_COURSE_CODE COURSE_CODE"
        ,"CASE WHEN coalesce(COALESCE(AT_ADJUSTED_GRADE,AT_GRADE),'ZZZ') = 'CT' THEN cast(BS_FES_START_DATE as date) \
            WHEN (AT_RECOGNITION_TYPE = '4' OR COALESCE(AT_ADJUSTED_GRADE,AT_GRADE) = 'RPL') \
                  and COALESCE(PUS_START_DATE, UIO_FES_START_DATE) > current_timestamp() \
                  THEN cast(BS_FES_START_DATE as date) \
            ELSE cast(COALESCE(PUS_START_DATE, UIO_FES_START_DATE) as date) \
        END AS FES_START_DATE"
        ,"CASE WHEN coalesce(AT_ADJUSTED_GRADE,AT_GRADE,'ZZZ') = 'NS' THEN cast(COALESCE(PUS_END_DATE, UIO_FES_END_DATE) as date) \
              WHEN COALESCE(PUS_END_DATE, UIO_FES_END_DATE) < AT_DATE_AWARDED AND COALESCE(AT_ADJUSTED_GRADE, AT_GRADE) NOT IN ('CU', 'PW') \
              THEN cast(COALESCE(PUS_END_DATE, UIO_FES_END_DATE) as date) \
              WHEN COALESCE(COALESCE(AT_ADJUSTED_GRADE,AT_GRADE),'ZZZ') = 'CT' THEN date_add(cast(BS_FES_START_DATE as date),1) \
              WHEN COALESCE(AT_ADJUSTED_GRADE, AT_GRADE) IN ('CU','PW') THEN cast(COALESCE(PUS_END_DATE, UIO_FES_END_DATE) as date) \
              WHEN AT_DATE_AWARDED IS NOT NULL AND COALESCE(AT_ADJUSTED_GRADE, AT_GRADE) IS NULL THEN cast(COALESCE(PUS_END_DATE, UIO_FES_END_DATE) as date) \
              ELSE cast(COALESCE(AT_DATE_AWARDED, PUS_END_DATE, UIO_FES_END_DATE) as date) \
        END AS FES_END_DATE"
        ,"COALESCE(PU_STUDY_REASON, BS_STUDY_REASON,'@@') AS STUDY_REASON"
        ,"BS_NZ_FUNDING NZ_FUNDING"
        ,"COALESCE(AT_ADJUSTED_GRADE,AT_GRADE) AS GRADE"
        ,"AT_GRADING_SCHEME_ID GRADING_SCHEME_ID"
        ,"CASE \
              WHEN at_recognition_type ='3' and COALESCE(AT_ADJUSTED_GRADE,AT_GRADE) = 'RPL' THEN '60' \
              WHEN COALESCE(COALESCE(AT_ADJUSTED_GRADE,AT_GRADE),'ZZZ') = 'CT' then '60' \
              WHEN AT_RECOGNITION_TYPE = '4' OR COALESCE(AT_ADJUSTED_GRADE,AT_GRADE) = 'RPL' THEN '51' \
              WHEN COALESCE(AT_ADJUSTED_GRADE,AT_GRADE) = 'NS' and BS_DESTINATION in ('SSDEF','SSDefCOV') and cast(COALESCE(PUS_START_DATE, UIO_FES_START_DATE) as date) > current_timestamp() THEN '85' \
              WHEN COALESCE(AT_ADJUSTED_GRADE,AT_GRADE) = 'NS' THEN NULL \
              WHEN COALESCE(COALESCE(AT_ADJUSTED_GRADE,AT_GRADE),'ZZZ') = 'PW' AND cast(COALESCE(PUS_END_DATE, UIO_FES_END_DATE) as date) >= current_timestamp() THEN '70' \
              WHEN COALESCE(COALESCE(AT_ADJUSTED_GRADE,AT_GRADE),'ZZZ') = 'PW' THEN NULL \
              WHEN COALESCE(AT_ADJUSTED_GRADE,AT_GRADE) IS NOT NULL and cs_status_code in ('PUBLISHED','LOCKED') THEN GSG_SDR_COMPLETION_CODE \
              WHEN cast(COALESCE(PUS_START_DATE, UIO_FES_START_DATE) as date) < current_timestamp() and cast(COALESCE(PUS_END_DATE, UIO_FES_END_DATE) as date) >= current_timestamp() THEN '70' \
              WHEN COALESCE(AT_ADJUSTED_GRADE,AT_GRADE) IS NULL and cast(COALESCE(PUS_START_DATE, UIO_FES_START_DATE) as date) > current_timestamp() THEN '85' \
              when BS_OUTCOME_ID_TRAINING_ORG in ('D') THEN '85' \
              when BS_OUTCOME_ID_TRAINING_ORG in ('TNC') THEN NULL \
              WHEN cast(COALESCE(PUS_START_DATE, UIO_FES_START_DATE) as date) > current_timestamp() THEN '85' \
              WHEN cs_status_code not in ('PUBLISHED' , 'LOCKED') THEN NULL \
              ELSE GSG_SDR_COMPLETION_CODE \
        END AS OUTCOME_ID_NATIONAL"
        ,"cast(AT_DATE_AWARDED as date) AS DATE_AWARDED"
        ,"case \
              when at_recognition_type='4' or coalesce(at_adjusted_grade, at_grade) in ('RPL','CT') then 'NNN' \
              when uio_fes_moa_code='CLAS' then 'YNN' \
              when uio_fes_moa_code='SELF' then 'YYN' \
              when uio_fes_moa_code='ELEC' then 'NYN' \
              when uio_fes_moa_code='BLND' then 'YYN' \
              when uio_fes_moa_code='ONJB' then 'NNY' \
              when uio_fes_moa_code='ONJD' then 'NYY' \
              when uio_fes_moa_code='SIMU' then 'NYY' \
              when uio_fes_moa_code='DIST' then 'NYN' \
              when uio_fes_moa_code='CLON' then 'YYN' \
              when uio_fes_moa_code='COMB' then 'YYY' \
              when uio_fes_moa_code='ONJO' then 'NYY' \
              when uio_fes_moa_code='ONLN' then 'NYN' \
              else 'NNN' \
        end as fes_moa_code"
        ,"UI_IS_BOS_COURSE IS_BOS_COURSE"
        ,"CASE WHEN COALESCE(COALESCE(AT_ADJUSTED_GRADE,AT_GRADE),'ZZZ') = 'CT' THEN '0000' \
              WHEN at_recognition_type ='3' and COALESCE(AT_ADJUSTED_GRADE,AT_GRADE) = 'RPL' THEN '0000' \
              ELSE LPAD(UI_MAXIMUM_HOURS,4,'0') \
        END AS MAXIMUM_HOURS"
        ,"BS_TRAINING_CONTRACT_IDENTIFIER TRAINING_CONTRACT_IDENTIFIER"
        ,"BS_COMMENCING_PROG_ID COMMENCING_PROG_ID"
        ,"BS_TRAINING_CONT_ID TRAINING_CONT_ID"
        ,"BS_CLIENT_ID_APPRENTICESHIP CLIENT_ID_APPRENTICESHIP"
        ,"BS_VET_IN_SCHOOL_FLAG VET_IN_SCHOOL_FLAG"
        ,"AT_RECOGNITION_TYPE AS RECOGNITION"
        ,"VP_PROPERTY_VALUE as FUNDING_NATIONAL"
        ,"FUNDING_SPECIFIC"
        ,"null as school_type_identifier"
        ,"BS_COURSE_PU_ID COURSE_PU_ID"
        ,"PU_ID AS UNIT_PU_ID"
        ,"BS_OUTCOME_ID_TRAINING_ORG"
        ,"null FUND_SOURCE_STA"
        ,"null CLIENT_TUIT_FEE"
        ,"null FEE_EXEMPT"
        ,"BS_PURCHASE_CON_ID PURCHASE_CON_ID"
        ,"null PURCHASE_CON_SCHED"
        ,"Null HRS_ATT"
        ,"BS_ASSOC_CRSE_ID ASSOC_CRSE_ID"
        ,"BS_IS_VOCATIONAL IS_VOCATIONAL"
        ,"case when at_recognition_type='4' or coalesce(at_adjusted_grade, at_grade) in ('RPL','CT') then 'N' \
              when uio_fes_moa_code='CLAS' then 'I' \
              when uio_fes_moa_code='SELF' then 'I' \
              when uio_fes_moa_code='ELEC' then 'E' \
              when uio_fes_moa_code='BLND' then 'E' \
              when uio_fes_moa_code='ONJB' then 'W' \
              when uio_fes_moa_code='ONJD' then 'W' \
              when uio_fes_moa_code='SIMU' then 'I' \
              when uio_fes_moa_code='DIST' then 'E' \
              when uio_fes_moa_code='CLON' then 'I' \
              when uio_fes_moa_code='ONJO' then 'W' \
              when uio_fes_moa_code='ONLN' then 'E' \
              when uio_fes_moa_code='COMB' then left(uio_predominant_delivery_mode,1) \
              else 'N' \
        end as predominant_del_mode"
        ,"BS_DESTINATION DESTINATION"
        ,"PU_NZ_FUNDING UNIT_NZ_FUNDING")
  return df

# COMMAND ----------

def GetNatUnits_Attainments(params,dfBase,rtoCode):
  NAT_Dates = spark.read.json(sc.parallelize([params]))
  CollectionStart = params["CollectionStartERpt"]
  CollectionEnd = params["CollectionEndERpt"]
  ReportingYear = params["ReportingYearERpt"]
  CollectionEndYear = (datetime.strptime(CollectionEnd, '%Y-%m-%d')).year
  CollectionEndYear = (datetime.strptime(CollectionEnd, '%Y-%m-%d')).year
  dfPUL=GeneralAliasDataFrameColumns(spark.table("trusted.OneEBS_EBS_0165_PEOPLE_UNIT_LINKS").filter(col("_RecordCurrent") == 1).filter(col("_RecordDeleted") == 0), "PUL_")
  dfPUA=GeneralAliasDataFrameColumns(spark.table("trusted.OneEBS_EBS_0165_PEOPLE_UNIT_ATTAINMENTS").filter(col("_RecordCurrent") == 1).filter(col("_RecordDeleted") == 0), "PUA_")
  dfPPL=GeneralAliasDataFrameColumns(spark.table("trusted.OneEBS_EBS_0165_PEOPLE").filter(col("_RecordCurrent") == 1).filter(col("_RecordDeleted") == 0), "PPL_")
  dfPUC=GeneralAliasDataFrameColumns(spark.table("trusted.oneebs_ebs_0165_people_units").filter(col("_RecordCurrent") == 1).filter(col("_RecordDeleted") == 0).filter(col("UNIT_TYPE")=='R'), "PUC_")
  dfUIOC=GeneralAliasDataFrameColumns(spark.table("trusted.OneEBS_EBS_0165_UNIT_INSTANCE_OCCURRENCES").filter(col("_RecordCurrent") == 1).filter(col("_RecordDeleted") == 0), "UIOC_")
  dfUIU=GeneralAliasDataFrameColumns(spark.table("trusted.OneEBS_EBS_0165_UNIT_INSTANCES").filter(col("_RecordCurrent") == 1).filter(col("_RecordDeleted") == 0).filter(col("UI_LEVEL")=='UNIT'), "UIU_")
  dfUIC=GeneralAliasDataFrameColumns(spark.table("trusted.OneEBS_EBS_0165_UNIT_INSTANCES").filter(col("_RecordCurrent") == 1).filter(col("_RecordDeleted") == 0).filter(col("UI_LEVEL")=='COURSE'), "UIC_")
  dfPUS=GeneralAliasDataFrameColumns(spark.table("trusted.OneEBS_EBS_0165_PEOPLE_UNITS_SPECIAL").filter((col("_RecordCurrent") == 1)).filter((col("_RecordDeleted") == 0)), "PUS_")
  dfAT=GeneralAliasDataFrameColumns(spark.table("trusted.OneEBS_EBS_0165_ATTAINMENTS").filter((col("_RecordCurrent") == 1)).filter((col("_RecordDeleted") == 0)).filter(~(col("RECOGNITION_TYPE")=='5')), "AT_")
  dfCS=GeneralAliasDataFrameColumns(spark.table("trusted.OneEBS_EBS_0165_CONFIGURABLE_STATUSES").filter((col("_RecordCurrent") == 1)).filter((col("_RecordDeleted") == 0)).filter(~(col("STATUS_CODE").isin('INVALID'))), "CS_")
  dfGSG=GeneralAliasDataFrameColumns(spark.table("trusted.OneEBS_EBS_0165_GRADING_SCHEME_GRADES").filter((col("_RecordCurrent") == 1)).filter((col("_RecordDeleted") == 0)), "GSG_")
  dfV=GeneralAliasDataFrameColumns(spark.table("trusted.OneEBS_EBS_0165_VERIFIERS").filter((col("_RecordCurrent") == 1)).filter((col("_RecordDeleted") == 0)).filter(col("RV_DOMAIN")=='RECOGNITION_TYPES'),"V_")
  dfVPFundNat=GeneralAliasDataFrameColumns(spark.table("trusted.OneEBS_EBS_0165_VERIFIER_PROPERTIES") \
                                    .filter((col("_RecordCurrent") == 1)) \
                                    .filter((col("_RecordDeleted") == 0)) \
                                    .filter(col("RV_DOMAIN")=='FUNDING') \
                                    .filter(col("PROPERTY_NAME")=='DEC_FUNDING_SOURCE_NATIONAL_2015'), "VPFN_")
  dfVPFundSpe=GeneralAliasDataFrameColumns(spark.table("trusted.OneEBS_EBS_0165_VERIFIER_PROPERTIES") \
                                    .filter((col("_RecordCurrent") == 1)) \
                                    .filter((col("_RecordDeleted") == 0)) \
                                    .filter(col("RV_DOMAIN")=='FUNDING') \
                                    .filter(col("PROPERTY_NAME")=='DEC_SPECIFIC_FUNDING_ID_2015'), "VPFS_")
  
  #Joins
  df=dfAT.join(dfGSG,(dfAT.AT_GRADING_SCHEME_ID == dfGSG.GSG_GRADING_SCHEME_ID) & (coalesce(col("AT_ADJUSTED_GRADE"),col("AT_GRADE"))==dfGSG.GSG_GRADE),how="left")
  df=df.join(dfUIU,dfUIU.UIU_FES_UNIT_INSTANCE_CODE == df.AT_SDR_COURSE_CODE,how="inner") 
  df=df.join(dfPUA,dfPUA.PUA_ATTAINMENT_CODE==df.AT_ATTAINMENT_CODE,how="inner")
  df=df.join(dfBase,dfBase.BS_CourseOfferingEnrolmentId==df.PUA_PEOPLE_UNITS_ID,how="inner")
  df=df.join(dfPUC,dfPUC.PUC_ID==dfPUA.PUA_PEOPLE_UNITS_ID,how="inner")
  df=df.join(dfPUS,dfPUS.PUS_PEOPLE_UNITS_ID==df.PUC_ID,how="left")
  df=df.join(dfUIOC,(dfUIOC.UIOC_UIO_ID==df.PUC_UIO_ID) & (col("UIOC_OFFERING_TYPE").isin(['SMART','SMARTNVH']) | ~isnull(col("PUC_COMMITMENT_IDENTIFIER"))),how="inner")
  df=df.join(dfUIC,dfUIC.UIC_FES_UNIT_INSTANCE_CODE==df.PUC_UNIT_INSTANCE_CODE,how="inner")
  df=df.join(dfPPL,dfPPL.PPL_PERSON_CODE==dfPUC.PUC_PERSON_CODE,how="inner")
  df=df.join(dfV,dfV.V_LOW_VALUE==df.AT_RECOGNITION_TYPE,how="left")
  df=df.join(dfVPFundNat,dfVPFundNat.VPFN_LOW_VALUE==df.PUC_NZ_FUNDING,how="left")
  df=df.join(dfVPFundSpe,dfVPFundSpe.VPFS_LOW_VALUE==df.PUC_NZ_FUNDING,how="left")
  
  #Where clause
  df=df.filter(expr("(COALESCE(AT_RECOGNITION_TYPE,'ZZ') IN ('1','2','3') OR COALESCE(AT_ADJUSTED_GRADE,AT_GRADE)='RPL')"))
  
  #Select columns
#   "COALESCE(UIC_NATIONAL_COURSE_CODE,SUBSTR(PUC_UNIT_INSTANCE_CODE,0,CHARINDEX(PUC_UNIT_INSTANCE_CODE,'V',7)-1),PUC_UNIT_INSTANCE_CODE) COURSE_CODE"
  df=df.withColumn("COURSE_CODE",col("BS_COURSE_CODE") )
  df=df.withColumn("OUTCOME_ID_NATIONAL", \
                     when(( col("AT_RECOGNITION_TYPE").isin(['1','2','3']) ) & ( coalesce(col("AT_ADJUSTED_GRADE"),col("AT_GRADE")) == 'RPL' ), '60' ) \
                     .when(( col("AT_RECOGNITION_TYPE").isin(['1','2','3']) ), '60' ) \
                     .when(col("AT_RECOGNITION_TYPE").isin(['1','2','3']),'51') \
                     .otherwise(''))
  
#   CASE WHEN AT_RECOGNITION_TYPE IN ('1','2','3') AND COALESCE(AT_ADJUSTED_GRADE,AT_GRADE) = 'RPL' then '0000'  \
#       WHEN AT_RECOGNITION_TYPE IN ('1','2','3') THEN '0000' \
#       ELSE LPAD(UIU_MAXIMUM_HOURS,4,'0') \
#   END AS MAXIMUM_HOURS
  df=df.withColumn("MAXIMUM_HOURS", \
                     when(( col("AT_RECOGNITION_TYPE").isin(['1','2','3']) ) & ( coalesce(col("AT_ADJUSTED_GRADE"),col("AT_GRADE")) == 'RPL' ), '0000') \
                     .when(( col("AT_RECOGNITION_TYPE").isin(['1','2','3']) ), '0000') \
                     .when(col("AT_RECOGNITION_TYPE").isin(['1','2','3']),'51') \
                     .otherwise('UIU_MAXIMUM_HOURS'))
# CASE WHEN PUC_USER_1 IN ('A', 'T') THEN COALESCE(PUC_TRAINING_CONTRACT_IDENTIFIER,'@@@@@@@@@@') \
#     WHEN PUC_PROGRAM_STREAM IN (3,4) THEN COALESCE(PUC_TRAINING_CONTRACT_IDENTIFIER,'@@@@@@@@@@') \
#     ELSE '' \
# END AS TRAINING_CONT_ID                                       
  df=df.withColumn("TRAINING_CONT_ID", \
                     when(col("PUC_USER_1").isin(['A','T']),coalesce(col("PUC_TRAINING_CONTRACT_IDENTIFIER"),lit('@@@@@@@@@@'))) \
                     .when(col("PUC_PROGRAM_STREAM").isin([3,4]), coalesce(col("PUC_TRAINING_CONTRACT_IDENTIFIER"),lit('@@@@@@@@@@'))) \
                     .otherwise(''))
#   "CASE WHEN PUC_USER_1 IN ('A', 'T') THEN COALESCE(substr(PUC_TRAINING_CONTRACT_IDENTIFIER,1,CHARINDEX(PUC_TRAINING_CONTRACT_IDENTIFIER,'/')-1),'@@@@@@@@@@')         \
#       WHEN PUC_PROGRAM_STREAM IN (3,4) THEN COALESCE(substr(PUC_TRAINING_CONTRACT_IDENTIFIER,1,CHARINDEX(PUC_TRAINING_CONTRACT_IDENTIFIER,'/')-1),'@@@@@@@@@@')   \
#       ELSE '' \
#   END AS CLIENT_ID_APPRENTICESHIP"
  df=df.withColumn("CLIENT_ID_APPRENTICESHIP", \
                     when(col("PUC_USER_1").isin(['A','T']),coalesce(col("PUC_TRAINING_CONTRACT_IDENTIFIER").substr(lit(1),instr(col("PUC_TRAINING_CONTRACT_IDENTIFIER"),"/")-1),lit('@@@@@@@@@@'))) \
                     .when(col("PUC_PROGRAM_STREAM").isin([3,4]), coalesce(col("PUC_TRAINING_CONTRACT_IDENTIFIER").substr(lit(1),instr(col("PUC_TRAINING_CONTRACT_IDENTIFIER"),"/")-1),lit('@@@@@@@@@@'))) \
                     .otherwise(''))
  df=df.withColumn("VET_IN_SCHOOL_FLAG",lit('N'))
  df=df.withColumn("predominant_del_mode",lit('N'))
  df=df.withColumn("FUNDING_SPECIFIC",lit('N'))
  df=df.withColumn("school_type_identifier",lit('N'))
#   "CASE \
#       WHEN PUC_DESTINATION in ('SSDISC','SSTRAN','WDVFHAC','WDVFHBC','WDSAC','SSDiscCOV') THEN 'TNC'  \
#       WHEN PUC_DESTINATION = 'SSDEF' AND cast(PUC_PROGRESS_DATE as date) <= month_add(currentdatetime() ,-12) THEN 'TNC'   \
#       WHEN PUC_DESTINATION in ('SSDEF','SSDefCOV') THEN 'D'  \
#       ELSE NULL \
#   END AS OUTCOME_ID_TRAINING_ORG"
  df = df.withColumn("OUTCOME_ID_TRAINING_ORG",\
                     when(col("PUC_DESTINATION").isin(['SSDISC','SSTRAN','WDVFHAC','WDVFHBC','WDSAC','SSDiscCOV']),'TNC') \
                     .when((col("PUC_DESTINATION")=='SSDEF') & (col("PUC_PROGRESS_DATE")<=add_months(current_date(),-12)), 'TNC' ) \
                     .when(col("PUC_DESTINATION").isin(['SSDEF', 'SSDefCOV']),'D')\
                     .otherwise(''))
  df=df.withColumn("COMMITMENT_IDENTIFIER",col("BS_PURCHASE_CON_ID")) \
       .withColumn("ASSOC_CRSE_ID",col("PUC_CALOCC_CODE")) \
       .withColumn("IS_VOCATIONAL",col("UIC_IS_VOCATIONAL")) \
       .withColumn("PURCHASE_CON_ID",col("BS_PURCHASE_CON_ID"))
  
  
  
  df=df.selectExpr("BS_TRAINING_ORGANISATION_ID TRAINING_ORGANISATION_ID"
                  ,"BS_RTOCode RTO_CODE"
                  ,"UIOC_SLOC_LOCATION_CODE SLOC_LOCATION_CODE"
                  ,"BS_REGISTRATION_NO REGISTRATION_NO"
                  ,"PPL_PERSON_CODE PERSON_CODE"
                  ,"NULL UIO_ID"
                  ,"NULL ID"
                  ,"COALESCE(UIU_NATIONAL_COURSE_CODE,UIU_FES_UNIT_INSTANCE_CODE) SUBJECT_ID"
                  ,"COURSE_CODE"
                  ,"CASE WHEN COALESCE(AT_ADJUSTED_GRADE,AT_GRADE)='RPL' then CAST(BS_FES_START_DATE AS DATE) \
                         ELSE CAST(COALESCE(PUS_start_date,UIOC_fes_start_date) as DATE) \
                  END FES_START_DATE"
                  ,"DATE_ADD(CAST(coalesce(PUS_start_date,UIOC_fes_start_date) AS DATE),1) FES_END_DATE"
                  ,"PUC_STUDY_REASON STUDY_REASON"
                  ,"PUC_NZ_FUNDING NZ_FUNDING"
                  ,"COALESCE(AT_ADJUSTED_GRADE,AT_GRADE) GRADE"
                  ,"AT_GRADING_SCHEME_ID GRADING_SCHEME_ID"
                  ,"OUTCOME_ID_NATIONAL"
                  ,"cast(AT_DATE_AWARDED as date) DATE_AWARDED"
                  ,"'NNN' FES_MOA_CODE"
                  ,"UIC_IS_BOS_COURSE IS_BOS_COURSE"
                  ,"MAXIMUM_HOURS"
                  ,"PUC_TRAINING_CONTRACT_IDENTIFIER TRAINING_CONTRACT_IDENTIFIER"
                  ,"BS_COMMENCING_PROG_ID COMMENCING_PROG_ID"
                  ,"TRAINING_CONT_ID"
                  ,"CLIENT_ID_APPRENTICESHIP"
                  ,"VET_IN_SCHOOL_FLAG"
                  ,"AT_RECOGNITION_TYPE RECOGNITION"
                  ,"VPFN_PROPERTY_VALUE FUNDING_NATIONAL"
                  ,"FUNDING_SPECIFIC"
                  ,"school_type_identifier"
                  ,"PUC_ID COURSE_PU_ID"
                  ,"NULL UNIT_PU_ID"
                  ,"OUTCOME_ID_TRAINING_ORG"
                  ,"NULL FUND_SOURCE_STA"
                  ,"NULL CLIENT_TUIT_FEE"
                  ,"NULL FEE_EXEMPT"
                  ,"PURCHASE_CON_ID"
                  ,"NULL PURCHASE_CON_SCHED"
                  ,"NULL HRS_ATT"
                  ,"ASSOC_CRSE_ID"
                  ,"IS_VOCATIONAL"
                  ,"predominant_del_mode"
                  ,"BS_DESTINATION DESTINATION"
                  ,"NULL UNIT_NZ_FUNDING")
                 
  
  return df



# COMMAND ----------

def GetNAT120ERpt(dfNATBase,params,rtoCode):
  #dfNATBase = GetNatBaseErpt(paramsERpt).cache()
  dfBase=GeneralAliasDataFrameColumns(dfNATBase,"BS_")
  dfBase=dfBase.withColumn("RTO_CODE", col("BS_RTOCode"))
  if rtoCode is None:
    rtoCode=90003
  else :
    dfBase=dfBase.filter(col("BS_RTOCode")==lit(rtoCode))
  dfPU  = GetNatUnits_PeopleUnits(params,dfBase,rtoCode)
  dfAtt = GetNatUnits_Attainments(params,dfBase,rtoCode)
  df=dfPU.union(dfAtt)
  #PRADA-1629
  df=df.withColumn("TRAINING_CONT_ID", expr("REPLACE(TRAINING_CONT_ID, '\\\\', '/')"))
  df=df.withColumn("CLIENT_ID_APPRENTICESHIP", expr("CASE WHEN TRAINING_CONT_ID IS NOT NULL THEN SPLIT(TRAINING_CONT_ID, '/')[0] ELSE CLIENT_ID_APPRENTICESHIP END "))
  df=df.withColumn("CLIENT_IDENTIFER", expr("COALESCE(REGISTRATION_NO, PERSON_CODE)"))
  df=df.sort("PURCHASE_CON_ID")
  df=df.withColumn("Output", \
                  concat( \
                         rpad(lit(rtoCode),10,' '), \
                         rpad(coalesce(col("SLOC_LOCATION_CODE"),lit(' ')),10,' ') , \
                         rpad(col("CLIENT_IDENTIFER"),10,' '), \
                         rpad(col("SUBJECT_ID"),12,' '), \
                         rpad(col("COURSE_CODE"),10,' '), \
                         lpad(date_format(col("FES_START_DATE"), "ddMMyyyy"),8,'0'), \
                         lpad(date_format(col("FES_END_DATE"), "ddMMyyyy"),8,'0'), \
                         rpad(coalesce(col("FES_MOA_CODE"),lit(' ')),3,' '), \
                         rpad(coalesce(col("OUTCOME_ID_NATIONAL"),lit(' ')),2,' '), \
                         rpad(lit('11'),2,' '), \
                         rpad(coalesce(col("COMMENCING_PROG_ID"),lit(' ')),1,' '), \
                         rpad(coalesce(col("TRAINING_CONT_ID"),lit(' ')),10,' '), \
                         rpad(coalesce(col("CLIENT_ID_APPRENTICESHIP"),lit(' ')),10,' '), \
                         rpad(coalesce(col("STUDY_REASON"),lit(' ')),2,' '), \
                         rpad(coalesce(col("VET_IN_SCHOOL_FLAG"),lit(' ')),1,' '), \
                         rpad(lit(' '),10,' '), \
                         rpad(lit(' '),2,' '), \
                         rpad(coalesce(col("BS_OUTCOME_ID_TRAINING_ORG"),lit(' ')),3,' '), \
                         rpad(coalesce(col("FUND_SOURCE_STA"),lit(' ')),3,' '), \
                         rpad(coalesce(col("CLIENT_TUIT_FEE"),lit(' ')),5,' '), \
                         rpad(coalesce(col("FEE_EXEMPT"),lit(' ')),2,' '), \
                         rpad(coalesce(col("PURCHASE_CON_ID")),12,' '), \
                         rpad(coalesce(col("PURCHASE_CON_SCHED"),lit(' ')),3,' '), \
                         rpad(coalesce(col("HRS_ATT"),lit(' ')),4,' '), \
                         rpad(coalesce(col("ASSOC_CRSE_ID"),lit(' ')),10,' '), \
                         lpad(coalesce(col("MAXIMUM_HOURS").cast(DecimalType(4,0)),lit('0')),4,'0'), \
                         rpad(coalesce(col("predominant_del_mode"),lit(' ')),1,' ')
                        ))
  return df

# COMMAND ----------


