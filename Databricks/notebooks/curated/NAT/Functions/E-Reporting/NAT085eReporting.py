# Databricks notebook source
def GetNAT085ERpt(dfBase, dfAvetmissStudent):
  #PRADA-1694
  df=dfBase.selectExpr("CLIENT_IDENTIFER").distinct()
  df=df.withColumnRenamed("CLIENT_IDENTIFER", "CLIENT_ID")
  
  dfStudent=GeneralAliasDataFrameColumns(dfAvetmissStudent,"S_")
  dfStudent=dfStudent.withColumn("S_CLIENT_ID", expr("CAST(COALESCE(S_RegistrationNo, S_PersonCode) AS INT)") )

  #JOIN
  df=df.join(dfStudent,df.CLIENT_ID==dfStudent.S_CLIENT_ID)
  df=df.withColumn("HasPostalAddress", expr("Case when S_PostalAddressLine1 is null and S_PostalAddressLine2 is null and S_PostalAddressLine3 is null and S_PostalAddressLine4 is null and S_PostalAddressPostcode is null and S_PostalAddressPostcodeName is null Then 'No' Else 'Yes' end"))
  df=df.selectExpr("coalesce(cast(CLIENT_ID as int),' ') as CLIENT_ID"
                   ,"coalesce(S_Title,' ') as CLIENT_TITLE"
                   ,"coalesce(S_Forename,' ') as CLIENT_FIRST_NAME"
                   ,"coalesce(S_Surname,' ') as CLIENT_LAST_NAME"
                   ,"Case when HasPostalAddress = 'Yes' Then coalesce(S_PostalAddressLine3,' ') else coalesce(S_ResidentialAddressLine3,'Not specified') end ADR_PROP_NAME"
                   ,"Case when HasPostalAddress = 'Yes' Then coalesce(S_PostalAddressLine4,' ') Else coalesce(S_ResidentialAddressLine4,'Not specified') end as ADR_FLAT_UNIT"
                   ,"Case when HasPostalAddress = 'Yes' Then coalesce(S_PostalAddressLine2,'Not specified') ELSE coalesce(S_ResidentialAddressLine2,'Not specified') end as ADR_STREET_NO"
                   ,"Case when HasPostalAddress = 'Yes' Then coalesce(S_PostalAddressLine1,'Not specified') Else coalesce(S_ResidentialAddressLine1,'Not specified') end as ADR_STREET_NAME"
                   ,"Case when HasPostalAddress = 'Yes' Then coalesce(S_PostalAddressLine1,' ') else coalesce(S_ResidentialAddressLine1,'Not specified') end as ADR_POSTAL_DEL_BOX"
                   ,"Case when HasPostalAddress = 'Yes' Then coalesce(S_PostalAddressPostcodeName,'Not specified') Else coalesce(S_HomePostcodeName,'Not specified') end as ADR_POST_SUB_LOC_TOWN"
                   ,"Case when HasPostalAddress = 'Yes' Then coalesce(S_PostalAddressPostcode,' ') Else  coalesce(S_HomePostcode,'Not specified') end as POST_CODE"
                   ,"CASE \
                       WHEN COALESCE(S_PostalAddressRegion, S_ResidentialRegion) = 'NSW' OR (S_HomePostCode Between 2000 AND 2999 AND HasPostalAddress = 'No') THEN '01' \
                       WHEN COALESCE(S_PostalAddressRegion, S_ResidentialRegion) = 'VIC' THEN '02' \
                       WHEN COALESCE(S_PostalAddressRegion, S_ResidentialRegion) = 'QLD' THEN '03'\
                       WHEN COALESCE(S_PostalAddressRegion, S_ResidentialRegion) = 'SA'  THEN '04' \
                       WHEN COALESCE(S_PostalAddressRegion, S_ResidentialRegion) = 'WA'  THEN '05' \
                       WHEN COALESCE(S_PostalAddressRegion, S_ResidentialRegion) = 'TAS' THEN '06' \
                       WHEN COALESCE(S_PostalAddressRegion, S_ResidentialRegion) = 'NT'  THEN '07' \
                       WHEN COALESCE(S_PostalAddressRegion, S_ResidentialRegion) = 'ACT' THEN '08'\
                       ELSE '99' \
                    END as STATE /*PRADA-1680, PRADA-1697*/"
                   ,"coalesce(S_HomePhone,' ') as TELEPHONE_HOME"
                   ,"coalesce(S_WorkPhone,' ') as TELEPHONE_WORK"
                   ,"coalesce(S_MobilePhone,' ') as TELEPHONE_MOBILE"
                   ,"coalesce(S_EmailAddress,' ') as EMAIL_ADR"
                   ,"' ' as EMAIL_ADR_ALT"
                   ,"HasPostalAddress as HasPostalAddress"
                  ).distinct()
  df=df.withColumn("ADR_POSTAL_DEL_BOX", 
                   when(upper(col("ADR_POSTAL_DEL_BOX")).like("PO BOX%")
                        |upper(col("ADR_POSTAL_DEL_BOX")).like("P O BOX%")
                        |upper(col("ADR_POSTAL_DEL_BOX")).like("POBOX%")
                        |upper(col("ADR_POSTAL_DEL_BOX")).like("LOCKED BAG%")
                        |upper(col("ADR_POSTAL_DEL_BOX")).like("GPO BOX%")
                        |upper(col("ADR_POSTAL_DEL_BOX")).like("PRIVATE BAG%")
                        |upper(col("ADR_POSTAL_DEL_BOX")).like("GPO LOCKED BAG%"),regexp_replace(col("ADR_STREET_NAME"),"’","'")).otherwise(""))
  #PRADA-1719
  for c in df.columns:
    df=df.withColumn(c, NatSpecialCharacterReplace(c))

  df=df.sort("CLIENT_ID")
  df=df.withColumn("Output",concat( \
                           rpad(coalesce(df.CLIENT_ID,lit(' ')),10,' ')
                          ,rpad(coalesce(df.CLIENT_TITLE,lit(' ')),4,' ') 
                          ,rpad(coalesce(df.CLIENT_FIRST_NAME,lit(' ')),40,' ') 
                          ,rpad(coalesce(df.CLIENT_LAST_NAME,lit(' ')),40,' ') 
                          ,rpad(coalesce(col("ADR_PROP_NAME"),lit(' ')),50,' ') 
                          ,rpad(coalesce(df.ADR_FLAT_UNIT,lit(' ')),30,' ') 
                          ,rpad(coalesce(df.ADR_STREET_NO,lit(' ')),15,' ') 
                          ,rpad(coalesce(df.ADR_STREET_NAME,lit(' ')),70,' ') 
                          ,rpad(coalesce(df.ADR_POSTAL_DEL_BOX,lit(' ')),22,' ') 
                          ,rpad(coalesce(df.ADR_POST_SUB_LOC_TOWN,lit(' ')),50,' ') 
                          ,rpad(coalesce(df.POST_CODE,lit(' ')),4,' ') 
                          ,rpad(coalesce(df.STATE,lit(' ')),2,' ') 
                          ,rpad(coalesce(df.TELEPHONE_HOME,lit(' ')),20,' ') 
                          ,rpad(coalesce(df.TELEPHONE_WORK,lit(' ')),20,' ') 
                          ,rpad(coalesce(df.TELEPHONE_MOBILE,lit(' ')),20,' ') 
                          ,rpad(coalesce(df.EMAIL_ADR,lit(' ')),80,' ') 
                          ,rpad(coalesce(df.EMAIL_ADR_ALT,lit(' ')),80,' ') 
                        )).dropDuplicates()
  return df

# COMMAND ----------


