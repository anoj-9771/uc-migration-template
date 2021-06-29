# Databricks notebook source
def GetNAT080Avetmiss(dfNAT120Avetmiss,dfNAT130Avetmiss,dfAvetmissStudent,dfAvetmissDisabilityType,dfPeopleASR,params):
  #Params
  CollectionEnd=params["CollectionEndAvt"]
  CollectionEndYear=(datetime.strptime(CollectionEnd,'%Y-%m-%d')).year
  
  #PRADA-1733
  dfNAT120=dfNAT120Avetmiss.selectExpr("CLIENT_ID").distinct()
  dfNAT130=dfNAT130Avetmiss.selectExpr("CLIENT_ID").distinct()
  df=dfNAT120.union(dfNAT130).distinct()

  dfStudent=GeneralAliasDataFrameColumns(dfAvetmissStudent,"S_")
  dfStudent=dfStudent.withColumn("S_CLIENT_ID",coalesce(col("S_RegistrationNo"), concat(lit("165"), col("S_PersonCode"))))
  df=df.join(dfStudent,df.CLIENT_ID==dfStudent.S_CLIENT_ID)
  
  dfDisabilityType=GeneralAliasDataFrameColumns(dfAvetmissDisabilityType,"DT_")
  df=df.join(dfDisabilityType,(df.S_PersonCode==dfDisabilityType.DT_PersonCode)&(dfDisabilityType.DT_ReportingYear==CollectionEndYear),how="left")
  
  dfASR=GeneralAliasDataFrameColumns(dfPeopleASR, "ASR_")
  df=df.join(dfASR,df.S_PersonCode==dfASR.ASR_PERSON_CODE,how="left")
  
  df=df.selectExpr("cast(CLIENT_ID as long) as CLIENT_IDENTIFER"
                  ,"concat(coalesce(S_Surname,''),', ',coalesce(S_Forename,S_Surname,''),' ',coalesce(S_MiddleNames,'')) as NAME_FOR_ENCRYPTION"
                  ,"CASE S_HighestSchoolLevel \
                      when 'DID NOT GO TO SCHOOL' then '02' \
                      When 'YEAR 08 OR BELOW' Then '08' \
                      When 'YEAR 09' Then '09' \
                      When 'YEAR 10' Then '10' \
                      When 'YEAR 11' Then '11' \
                      When 'YEAR 12' Then '12' \
                      ELSE '@@' \
                    END as HIGH_SCHOOL_LVL_COMP"
                   ,"coalesce( \
                           case \
                             when S_HighestSchoolLevel = 'DID NOT GO TO SCHOOL' and LEFT(S_StillAttendSchool, 1)='N' then '@@@@' \
                             else S_YearSchoolCompleted \
                           end,'@@@@') as YEAR_HIGH_SCHOOL_COMP"
                   ,"coalesce( \
                           Case \
                             when S_gender is null then 'X' \
                             else Left(S_gender,1) \
                           end,'@') as SEX"
                   ,"coalesce(date_format(S_dateofbirth,'ddMMyyyy'),'@@@@@@@@') as DATE_OF_BIRTH"
                   ,"coalesce(S_HomePostCode,' ') as POST_CODE"
                   ,"coalesce(S_ATSICode, '@') as INDEGENIOUS_STATUS_ID /*PRADA-1678*/"
                   ,"coalesce( \
                       case \
                         when S_LanguageSpoken='9998' then '0000' \
                         when S_LanguageSpoken='9999' then '@@@@' \
                         else S_LanguageSpoken \
                       end,'@@@@') as LANGUAGE_IDENTIFIER"
                   ,"coalesce(S_EmploymentStatus,'@@') as LABOUR_FORCE_STATUS_ID"
                   ,"coalesce( \
                       case \
                         when S_CountryOfBirth='9998' then '0000' \
                         when S_CountryOfBirth='9999' then '@@@@' \
                         else S_CountryOfBirth \
                       end,'@@@@') as COUNTRY_IDENTIFIER"
                   ,"Case \
                       when S_DisabilityFlag='2' OR LEFT(S_DisabilityNeedHelp, 1)='Y' or DT_PersonCode is not null THEN 'Y' \
                       when S_DisabilityFlag='' AND S_DisabilityNeedHelp is null AND DT_PersonCode is null THEN '@' \
                       else 'N' \
                     END as DISABILITY_FLG /*PRADA-1392*/"
                   ,"CASE S_HighestPreviousQualification \
                      WHEN '0. NO PREVIOUS QUAL' THEN 'N' \
                      ELSE 'Y' \
                     END as PRIOR_ED_ACHIEVEMENT_FLG"
                   ,"coalesce(Left(S_StillAttendSchool, 1), '@') as AT_SCHOOL_FLAG" 
                   ,"CASE \
                      WHEN S_LanguageSpoken IN ('1201', '9700', '9701', '9702', '9799', '9999') THEN '' \
                      WHEN S_LanguageSpoken IS NULL THEN '' \
                      ELSE COALESCE(S_EnglishLevel, '@') \
                    END as PROF_IN_ENG_ID"
                   ,"coalesce(S_HomePostcodeName,' ') as ADR_SUB_LOC_TOWN"
                   ,"coalesce(S_USI,' ') as UNIQUE_STUDENT_ID"
                   ,"CASE WHEN S_AusState = 'NSW' THEN '01' \
                          WHEN S_AusState = 'VIC' THEN '02' \
                          WHEN S_AusState = 'QLD' THEN '03'\
                          WHEN S_AusState = 'SA' THEN '04' \
                          WHEN S_AusState = 'WA' THEN '05' \
                          WHEN S_AusState = 'TAS' THEN '06' \
                          WHEN S_AusState = 'NT' THEN '07' \
                          WHEN S_AusState = 'ACT' THEN '08'\
                          WHEN S_HomePostCode Between 2000 and 2999 THEN '01' \
                          ELSE '99' \
                      end as STATE_ID"
                   ,"coalesce(S_ResidentialAddressLine3,' ') as ADR_BUILDING_PROP_NAME"
                   ,"coalesce(S_ResidentialAddressLine4,' ') as ADR_FLAT_UNIT"
                   ,"coalesce(CASE WHEN S_ResidentialAddressLine2 ='' THEN NULL ELSE S_ResidentialAddressLine2 END, 'Not Specified') as ADR_STREET_NO /*PRADA-1410*/"
                   ,"coalesce(S_ResidentialAddressLine1,' ') as ADR_STREET_NAME"
                   ,"coalesce(cast(S_PersonCode as int),' ') as PERSON_CODE"
                   ,"coalesce(S_ResidentialAvetmissCountry,' ') as AVETMISS_COUNTRY_REF"
                   ,"coalesce(CASE \
                      WHEN ASR_SURVEY_CONTACT_STATUS = 'AFS' THEN 'A' \
                      WHEN ASR_SURVEY_CONTACT_STATUS = 'CF' THEN 'C' \
                      WHEN ASR_SURVEY_CONTACT_STATUS = 'DECD' THEN 'D' \
                      WHEN ASR_SURVEY_CONTACT_STATUS = 'EXC' THEN 'E' \
                      WHEN ASR_SURVEY_CONTACT_STATUS = 'INVADD' THEN 'I' \
                      WHEN ASR_SURVEY_CONTACT_STATUS = 'MINOR' THEN 'M' \
                      WHEN ASR_SURVEY_CONTACT_STATUS = 'OSEAS' THEN 'O' \
                    END,' ') as SURVEY_CONTACT_STATUS"
                  ).distinct()
  #PRADA-1412, PRADA-1607, PRADA-1181
  for c in df.columns:
    df=df.withColumn(c, NatSpecialCharacterReplace(c))
  df = GlobalSpecialCharacterReplacement(df)

  df=df.withColumn("Output", 
                      concat 
                        ( 
                           rpad(df.CLIENT_IDENTIFER,10,' ') 
                          ,rpad(df.NAME_FOR_ENCRYPTION,60,' ') 
                          ,rpad(df.HIGH_SCHOOL_LVL_COMP,2,' ') 
                          ,rpad(df.YEAR_HIGH_SCHOOL_COMP,4,' ') 
                          ,rpad(df.SEX,1,' ') 
                          ,rpad(df.DATE_OF_BIRTH,8,' ') 
                          ,rpad(df.POST_CODE,4,' ') 
                          ,rpad(df.INDEGENIOUS_STATUS_ID,1,' ') 
                          ,rpad(df.LANGUAGE_IDENTIFIER,4,' ') 
                          ,rpad(df.LABOUR_FORCE_STATUS_ID,2,' ') 
                          ,rpad(df.COUNTRY_IDENTIFIER,4,' ') 
                          ,rpad(df.DISABILITY_FLG,1,' ') 
                          ,rpad(df.PRIOR_ED_ACHIEVEMENT_FLG,1,' ') 
                          ,rpad(df.AT_SCHOOL_FLAG,1,' ') 
                          ,rpad(df.PROF_IN_ENG_ID,1,' ') 
                          ,rpad(df.ADR_SUB_LOC_TOWN,50,' ') 
                          ,rpad(df.UNIQUE_STUDENT_ID,10,' ') 
                          ,rpad(df.STATE_ID,2,' ') 
                          ,rpad(df.ADR_BUILDING_PROP_NAME,50,' ') 
                          ,rpad(df.ADR_FLAT_UNIT,30,' ') 
                          ,rpad(df.ADR_STREET_NO,15,' ') 
                          ,rpad(df.ADR_STREET_NAME,70,' ') 
                          ,rpad(df.PERSON_CODE,10,' ') 
                          ,rpad(df.AVETMISS_COUNTRY_REF,5,' ') 
                          ,rpad(df.SURVEY_CONTACT_STATUS,1,' ') 
                        ) 
                   ).dropDuplicates()
  
  return df

# COMMAND ----------


