# Databricks notebook source
def GetNAT080ERpt(dfBase, dfAvetmissStudent, dfStudentStatusLog, dfDisabilities, params):
  #Params
  CollectionEnd=params["CollectionEndERpt"]
  CollectionEndYear=(datetime.strptime(CollectionEnd,'%Y-%m-%d')).year
  #NAT120
  dfBase=dfBase.withColumnRenamed("FES_END_DATE","COURSE_END_DATE")
  dfBase=dfBase.withColumn("FND_SRC",when(col("UNIT_NZ_FUNDING").isin(['8451','845']),lit('1')).otherwise(lit('0')))
  dfNAT120=dfBase.selectExpr("CLIENT_IDENTIFER","FND_SRC","COURSE_END_DATE").distinct()
  
  #STUDENT
  dfStudent=GeneralAliasDataFrameColumns(dfAvetmissStudent, "S_")
  dfStudent=dfStudent.withColumn("S_CLIENT_ID", expr("SPLIT(COALESCE(S_RegistrationNo, S_PersonCode), '[.]')[0]"))
  df=dfNAT120.join(dfStudent,expr("TRIM(CLIENT_IDENTIFER)==TRIM(S_CLIENT_ID)"))
  
  #STUDENT STATUS
  dfSSL=GeneralAliasDataFrameColumns(dfStudentStatusLog, "SSL_")
  df=df.join(dfSSL,df.S_PersonCode==dfSSL.SSL_PersonCode,how="left")
  
  #DISABILITY
  dfDisabilities=GeneralAliasDataFrameColumns(dfDisabilities,"DT_")
  df=df.join(dfDisabilities, (df.S_PersonCode == dfDisabilities.DT_PER_PERSON_CODE) & (lit(CollectionEnd).between(dfDisabilities.DT_START_DATE, dfDisabilities.DT_END_DATE) | dfDisabilities.DT_END_DATE.isNull()), how="left")
  df=df.withColumn("ADR_BUILDING_PROP_NAME",coalesce(regexp_replace('S_ResidentialAddressLine3', '"', ''),lit(' '))) 
  df=df.selectExpr("cast(CLIENT_IDENTIFER as int) as CLIENT_IDENTIFER"
                  ,"concat(coalesce(S_Surname,''),', ',coalesce(S_Forename,''),' ',coalesce(S_MiddleNames,'')) as NAME_FOR_ENCRYPTION"
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
                           Case \
                             when S_gender is null then 'X' \
                             else Left(S_gender,1) \
                           end,'@') as SEX"
                   ,"coalesce(date_format(S_dateofbirth,'ddMMyyyy'),'@@@@@@@@') as DATE_OF_BIRTH"
                   ,"coalesce(S_HomePostCode,' ') as POST_CODE"
                   ,"coalesce( \
                       Case S_IndigenousFlag \
                         When 'Yes, Aboriginal' then '1' \
                         When 'Yes, Aboriginal and Torres Strait Islanderr' then '3' \
                         When 'Yes, Torres Strait Islander' then '2' \
                         When 'No' then '4' \
                         ELSE '@' \
                       end, '@') as INDEGENIOUS_STATUS_ID"
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
                       when S_DisabilityFlag='2' or S_DisabilityNeedHelp='Y' or DT_PER_PERSON_CODE is not null THEN 'Y' \
                       when S_DisabilityFlag='' AND DT_PER_PERSON_CODE is null THEN '@' \
                       else 'N' \
                     END as DISABILITY_FLG"
                   ,"DT_DISABILITY_TYPE DISABILITY_TYPE"
                   ,"CASE S_HighestPreviousQualification \
                      WHEN '0. NO PREVIOUS QUAL' THEN 'N' \
                      ELSE 'Y' \
                     END as PRIOR_ED_ACHIEVEMENT_FLG"
                   ,"CASE WHEN S_EmploymentStatus = '01' THEN 'N' ELSE COALESCE(left(S_StillAttendSchool,1), '@') END as AT_SCHOOL_FLAG" 
                   ,"Case when S_HomePostcode in ('@@@@', '0000') then 'Not specified' When S_HomePostcodeName is null then 'Not specified' else S_HomePostcodeName end as ADR_SUB_LOC_TOWN"
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
                   ,"ADR_BUILDING_PROP_NAME"
                   ,"coalesce(S_ResidentialAddressLine4,' ') as ADR_FLAT_UNIT"
                   ,"coalesce(S_ResidentialAddressLine2,'Not specified') as ADR_STREET_NO"
                   ,"coalesce(S_ResidentialAddressLine1,'Not specified') as ADR_STREET_NAME"
                   ,"coalesce(CASE \
                      when SSL_StudentStatusCode='DECD' then 'D' \
                      when SSL_StudentStatusCode is null then 'A' \
                      when COURSE_END_DATE<'1-JAN-2018' then 'A' \
                      when FND_SRC>0 or S_ResidentialRegion not in ('NSW','QLD','SA','VIC','WA','NT','ACT') then 'O' \
                      when S_ResidentialAddressLine1 like ('%correction%') \
                          or S_ResidentialAddressLine2 like ('%correction%') \
                          or S_ResidentialAddressLine3 like ('%correction%') \
                          or S_ResidentialAddressLine4 like ('%correction%') \
                          or S_ResidentialAddressLine5 like ('%correction%') then 'C'\
                      else 'E' \
                    END,' ') as SURVEY_CONTACT_STATUS"
                   ,"ROW_NUMBER() OVER (PARTITION BY CLIENT_IDENTIFER ORDER BY COURSE_END_DATE) AS RN"
                  ).filter(col("RN")==1).distinct()
  #PRADA-1718
  for c in df.columns:
    df=df.withColumn(c, NatSpecialCharacterReplace(c))
  df = GlobalSpecialCharacterReplacement(df)
  
  df=df.sort("CLIENT_IDENTIFER")
  df=df.withColumn("Output",concat( \
                                   rpad(coalesce(df.CLIENT_IDENTIFER,lit(' ')),10,' ')
                                  ,rpad(coalesce(df.NAME_FOR_ENCRYPTION,lit(' ')),60,' ') 
                                  ,rpad(coalesce(df.HIGH_SCHOOL_LVL_COMP,lit(' ')),2,' ') 
                                  ,rpad(coalesce(df.SEX,lit(' ')),1,' ') 
                                  ,rpad(coalesce(df.DATE_OF_BIRTH,lit(' ')),8,' ') 
                                  ,rpad(coalesce(df.POST_CODE,lit(' ')),4,' ') 
                                  ,rpad(coalesce(df.INDEGENIOUS_STATUS_ID,lit(' ')),1,' ') 
                                  ,rpad(coalesce(df.LANGUAGE_IDENTIFIER,lit(' ')),4,' ') 
                                  ,rpad(coalesce(df.LABOUR_FORCE_STATUS_ID,lit(' ')),2,' ') 
                                  ,rpad(coalesce(df.COUNTRY_IDENTIFIER,lit(' ')),4,' ') 
                                  ,rpad(coalesce(df.DISABILITY_FLG,lit(' ')),1,' ') 
                                  ,rpad(coalesce(df.PRIOR_ED_ACHIEVEMENT_FLG,lit(' ')),1,' ') 
                                  ,rpad(coalesce(df.AT_SCHOOL_FLAG,lit(' ')),1,' ') 
                                  ,rpad(coalesce(df.ADR_SUB_LOC_TOWN,lit(' ')),50,' ') 
                                  ,rpad(coalesce(df.UNIQUE_STUDENT_ID,lit(' ')),10,' ') 
                                  ,rpad(coalesce(df.STATE_ID,lit(' ')),2,' ') 
                                  ,rpad(coalesce(col("ADR_BUILDING_PROP_NAME"),lit(' ')),50,' ') 
                                  ,rpad(coalesce(df.ADR_FLAT_UNIT,lit(' ')),30,' ') 
                                  ,rpad(coalesce(df.ADR_STREET_NO,lit(' ')),15,' ') 
                                  ,rpad(coalesce(df.ADR_STREET_NAME,lit(' ')),70,' ') 
                                  ,rpad(coalesce(df.SURVEY_CONTACT_STATUS,lit(' ')),1,' ')
                                  )).dropDuplicates()
  return df

# COMMAND ----------


