# Databricks notebook source
def GetNAT085Avetmiss(dfNAT120Avetmiss,dfNAT130Avetmiss,dfAvetmissStudent):
  
  dfNAT120=dfNAT120Avetmiss.selectExpr("CLIENT_ID").distinct()
  dfNAT130=dfNAT130Avetmiss.selectExpr("CLIENT_ID").distinct()
  df=dfNAT120.union(dfNAT130).distinct()
  
  dfStudent=GeneralAliasDataFrameColumns(dfAvetmissStudent,"S_")
  dfStudent=dfStudent.withColumn("S_CLIENT_ID",coalesce(col("S_RegistrationNo"),col("S_PersonCode")))
  df=df.join(dfStudent,df.CLIENT_ID==dfStudent.S_CLIENT_ID)
  
  df=df.selectExpr("coalesce(cast(CLIENT_ID as int),' ') as CLIENT_ID"
                   ,"coalesce(Case When S_Gender = 'Female' AND COALESCE(S_Title, '') = '' then 'MS' \
                      When S_Gender = 'Male' AND COALESCE(S_Title, '') = '' then 'MR' \
                      ELSE S_Title end, ' ') as CLIENT_TITLE /*PRADA-1413*/"
                   ,"coalesce(S_Forename,' ') as CLIENT_FIRST_NAME"
                   ,"coalesce(S_Surname,' ') as CLIENT_LAST_NAME"
                   ,"coalesce(S_ResidentialAddressLine3,' ') as ADR_PROP_NAME"
                   ,"coalesce(S_ResidentialAddressLine4,' ') as ADR_FLAT_UNIT"
                   ,"coalesce(S_ResidentialAddressLine2,' ') as ADR_STREET_NO"
                   ,"coalesce(S_ResidentialAddressLine1,' ') as ADR_STREET_NAME"
                   ,"replace(replace(S_ResidentialAddressLine1,'.',''),'’','''') as ADR_POSTAL_DEL_BOX"
                   ,"coalesce(S_PostalAddressPostcodeName,' ') as ADR_POST_SUB_LOC_TOWN"
                   ,"coalesce(S_PostalAddressPostcode,' ') as POST_CODE"
                   ,"case \
                       when S_PostalAddressPostcode='OSPC' then '99' \
                       when S_PostalAddressRegion<>'99' then \
                         case S_AusState \
                          when 'NSW' THEN '01' \
                          when 'VIC' THEN '02' \
                          when 'QLD' THEN '03' \
                          when 'SA' THEN '04' \
                          when 'WA' THEN '05' \
                          when 'TAS' THEN '06' \
                          when 'NT' THEN '07' \
                          when 'ACT' THEN '08' \
                          else '99' \
                        end \
                      else '99' \
                    end as STATE"
                   ,"coalesce(S_HomePhone,' ') as TELEPHONE_HOME"
                   ,"coalesce(S_WorkPhone,' ') as TELEPHONE_WORK"
                   ,"coalesce(S_MobilePhone,' ') as TELEPHONE_MOBILE"
                   ,"coalesce(S_EmailAddress,' ') as EMAIL_ADR"
                   ,"coalesce(S_PostalAvetmissCountry,' ') as AVETMISS_COUNTRY_REF"
                  ).distinct()

  df=df.withColumn("ADR_POSTAL_DEL_BOX", 
                   when(upper(col("ADR_POSTAL_DEL_BOX")).like("PO BOX%")
                        |upper(col("ADR_POSTAL_DEL_BOX")).like("P O BOX%")
                        |upper(col("ADR_POSTAL_DEL_BOX")).like("POBOX%")
                        |upper(col("ADR_POSTAL_DEL_BOX")).like("LOCKED BAG%")
                        |upper(col("ADR_POSTAL_DEL_BOX")).like("GPO BOX%")
                        |upper(col("ADR_POSTAL_DEL_BOX")).like("PRIVATE BAG%")
                        |upper(col("ADR_POSTAL_DEL_BOX")).like("GPO LOCKED BAG%"),regexp_replace(col("ADR_STREET_NAME"),"’","'")).otherwise(" "))
  #PRADA-1412, PRADA-1607, PRADA-1181
  for c in df.columns:
    df=df.withColumn(c, NatSpecialCharacterReplace(c))
  df = GlobalSpecialCharacterReplacement(df)

  df=df.withColumn("Output",
                      concat 
                        ( 
                           rpad(df.CLIENT_ID,10,' ')
                          ,rpad(df.CLIENT_TITLE,4,' ') 
                          ,rpad(df.CLIENT_FIRST_NAME,40,' ') 
                          ,rpad(df.CLIENT_LAST_NAME,40,' ') 
                          ,rpad(df.ADR_PROP_NAME,50,' ') 
                          ,rpad(df.ADR_FLAT_UNIT,30,' ') 
                          ,rpad(df.ADR_STREET_NO,15,' ') 
                          ,rpad(df.ADR_STREET_NAME,70,' ') 
                          ,rpad(df.ADR_POSTAL_DEL_BOX,22,' ') 
                          ,rpad(df.ADR_POST_SUB_LOC_TOWN,50,' ') 
                          ,rpad(df.POST_CODE,4,' ') 
                          ,rpad(df.STATE,2,' ') 
                          ,rpad(df.TELEPHONE_HOME,20,' ') 
                          ,rpad(df.TELEPHONE_WORK,20,' ') 
                          ,rpad(df.TELEPHONE_MOBILE,20,' ') 
                          ,rpad(df.EMAIL_ADR,80,' ') 
                          ,rpad(df.AVETMISS_COUNTRY_REF,5,' ') 
                        ) 
                   ).dropDuplicates()
  
  return df

# COMMAND ----------


