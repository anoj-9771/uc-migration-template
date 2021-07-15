# Databricks notebook source
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.window import *
from pyspark.sql.types import *

# COMMAND ----------

def CleanseColumn(dataFrame, columnName):
  return dataFrame.withColumn(columnName, regexp_replace(col(columnName), "[’]", "'")) \
        .withColumn(columnName, regexp_replace(col(columnName), "[\"“”]", ""))

# COMMAND ----------

###########################################################################################################################  
# Function: GetAvetmissStudent
#  Returns a dataframe that for the Student Table
# Parameters:
#  dfPeople = Input DataFrame - People
#  dfStudentStatusLog = Input DataFrame - Status
#  dfAddresses = Input DataFrame - AdddressesDATE_OF_BIRTH
#  dfCountry = Input DataFrame -  Country
#  dfVisas = Input DataFrame - Visas
#  dfVisaHolderType = Input DataFrame - VIsa Holder Type
#  dfVisaSubClasses = Input DataFrame - Visa Sub Classes
#  dfEnglishLevel = Input DataFrame - English Level
#  dfLanguage = Input DataFrame - Language
#  dfResidentialStatus = Input DataFrame - Residential Status 
#  dfEmpStatus = Input DataFrame - Current Employment Status
#  dfDisabilityStatus = Input DataFrame - Disability Status
#  dfPeopleUSI = Input DataFrame - People USI
#  dfSEIFA = Input DataFrame - Socio-Economic Indexes for Areas
#  dfLGA = Input DataFrame -  Local Government Area
#  dfPeopleASR = Input Dataframe - Student Academic Record
# Returns:
#  A dataframe that has all attributes for Prada Student 
#############################################################################################################################
def GetAvetmissStudent(dfPeople, dfStudentStatusLog, dfStudentStatus, dfAddresses, dfCountry, dfVisas, dfVisaHolderType, dfVisaSubClasses, dfEnglishLevel, dfLanguage, dfResidentialStatus, dfEmpStatus, dfDisabilityStatus, dfPeopleUSI, dfSEIFA, dfLGA, dfPeopleASR, dfPriorEducationLocation, dfIndigenousStatus, startDate, endDate):
  #Alias Main People Table
  dfPeopleV = dfPeople.select(*(col(x).alias("PPL_"+ x) for x in dfPeople.columns))
  dfPeopleASRV = dfPeopleASR.select(*(col(x).alias("PASR_"+ x) for x in dfPeopleASR.columns))

  #Filter Addresses to Students Only
  dfAddresses = dfAddresses.filter((col("OWNER_TYPE") == "P"))

  #Addresses and Country Join RES -- Remove Dups
  dfFullAddressRES = dfAddresses.join(dfCountry, dfAddresses.COUNTRY_CODE == dfCountry.CountryCode, how="left")\
    .filter(col("ADDRESS_TYPE")=="RES")\
    .withColumn("row_number", expr("row_number() over(partition by PERSON_CODE order by coalesce(END_DATE, '9999-12-31') desc)")).filter(col("row_number") =="1").drop("row_number")\
    .selectExpr("OWNER_REF as RES_OWNER_REF", "OWNER_TYPE AS RES_OWNER_TYPE",  "ADDRESS_LINE_1 as RES_ADDRESS_LINE_1",
 "ADDRESS_LINE_2 as RES_ADDRESS_LINE_2", "ADDRESS_LINE_3 as RES_ADDRESS_LINE_3", "ADDRESS_LINE_4 as RES_ADDRESS_LINE_4", "ADDRESS_LINE_5 as RES_ADDRESS_LINE_5", "COUNTRY as RES_COUNTRY", "TOWN as RES_TOWN", "UK_POST_CODE_PT1 as RES_UK_POST_CODE_PT1", "REGION as RES_REGION", "COUNTRY_CODE as RES_USER_1", "greatest(trusted.oneebs_ebs_0165_addresses._DLTrustedZoneTimeStamp, reference.country._DLTrustedZoneTimeStamp) as RES__DLTrustedZoneTimeStamp")

  #Addresses and Country Join POS -- Remove Dups
  dfFullAddressPOS = dfAddresses.join(dfCountry, dfAddresses.COUNTRY_CODE == dfCountry.CountryCode, how="left")\
    .filter(col("ADDRESS_TYPE")=="POS")\
    .withColumn("row_number", expr("row_number() over(partition by PERSON_CODE order by coalesce(END_DATE, '9999-12-31') desc)")).filter(col("row_number") =="1").drop("row_number")\
    .selectExpr("OWNER_REF as POS_OWNER_REF", "OWNER_TYPE AS POS_OWNER_TYPE",  "ADDRESS_LINE_1 as POS_ADDRESS_LINE_1",
 "ADDRESS_LINE_2 as POS_ADDRESS_LINE_2", "ADDRESS_LINE_3 as POS_ADDRESS_LINE_3", "ADDRESS_LINE_4 as POS_ADDRESS_LINE_4", "ADDRESS_LINE_5 as POS_ADDRESS_LINE_5", "COUNTRY as POS_COUNTRY", "TOWN as POS_TOWN", "UK_POST_CODE_PT1 as POS_UK_POST_CODE_PT1", "REGION as POS_REGION", "COUNTRY_CODE as POS_USER_1", "TELEPHONE as POS_TELEPHONE", "greatest(trusted.oneebs_ebs_0165_addresses._DLTrustedZoneTimeStamp, reference.country._DLTrustedZoneTimeStamp) as POS__DLTrustedZoneTimeStamp")

  #Visas Holders View
  dfVisasV = dfVisas.join(dfVisaHolderType, lower(dfVisas.VISA_HOLDER_CODE) == lower(dfVisaHolderType.VisaHolderTypeCode), how="left") \
    .withColumn("row_number",row_number().over(Window.partitionBy("PERSON_CODE").orderBy(desc("CREATED_DATE")))).filter(col("row_number") =="1").drop("row_number")\
    .withColumn("VisaType", col("VISA_TYPE"))\
    .withColumn("VISA__DLTrustedZoneTimeStamp", expr("greatest(trusted.oneebs_ebs_0165_visas._DLTrustedZoneTimeStamp, reference.visa_holder_type._DLTrustedZoneTimeStamp)"))
  

  #Visa Sub Classes View
  dfVisasV = dfVisasV.join(dfVisaSubClasses, lower(dfVisasV.VISA_SUB_CLASS) == lower(dfVisaSubClasses.CODE), how="left")

  #Prior Education Location
  dfPriorEducationLocationV = dfPriorEducationLocation.selectExpr("PriorEducationLocationDescription", "PRIOREDUCATIONLOCATIONID as PelID_4", "PRIOREDUCATIONLOCATIONID as PelID_6", "PRIOREDUCATIONLOCATIONID as PelID_8", "PRIOREDUCATIONLOCATIONID as PelID_10", "PRIOREDUCATIONLOCATIONID as PelID_12", "PRIOREDUCATIONLOCATIONID as PelID_14",  "PRIOREDUCATIONLOCATIONID as PelID_16", "PRIOREDUCATIONLOCATIONID as PelID_18", "_DLTrustedZoneTimeStamp")

  ##############################################
  #######Start Main Table Join - People#########
  ##############################################
  #English Level Join
  df = dfPeopleV.join(dfEnglishLevel, dfPeopleV.PPL_F_LANG == dfEnglishLevel.EnglishLevelID, how="left")

  #Join with RES Address
  df = df.join(dfFullAddressRES, df.PPL_PERSON_CODE == dfFullAddressRES.RES_OWNER_REF, how="left")

  #Join with POS Addresses
  df = df.join(dfFullAddressPOS, df.PPL_PERSON_CODE == dfFullAddressPOS.POS_OWNER_REF, how="left")

  #Join Visas
  df = df.join(dfVisasV, df.PPL_PERSON_CODE == dfVisasV.PERSON_CODE, how="left")

  #Join Country
  df = df.join(dfCountry, df.PPL_FES_COUNTRY_OF_BIRTH == dfCountry.CountryCode, how="left")

  #Join Language
  df = df.join(dfLanguage, df.PPL_HOME_LANGUAGE == dfLanguage.LanguageID, how="left")

  #Residential Status
  df = df.join(dfResidentialStatus, df.PPL_RESIDENTIAL_STATUS == dfResidentialStatus.ResidentialStatusID, how="left")

  #Current Employment Status
  df = df.join(dfEmpStatus, df.PPL_CURREMPSTATUS == dfEmpStatus.EmploymentStatusID , how="left")

  #Disability Status
  df = df.join(dfDisabilityStatus, df.PPL_LEARN_DIFF == dfDisabilityStatus.DisabilityStatusCode , how="left")

  #Join Peple USI 
  df = df.join(dfPeopleUSI, df.PPL_PERSON_CODE == dfPeopleUSI.PERSON_CODE, how="left")
  
  #Join dfSEIFA
  df = df.join(dfSEIFA, ((trim(lower(df.RES_TOWN)) == trim(lower(dfSEIFA.Suburb))) & (df.RES_UK_POST_CODE_PT1 == dfSEIFA.Postcode)), how = "Left")
  
  #Join LGA
  dfLGA = GeneralAliasDataFrameColumns(dfLGA, "lga_")
  df = df.join(dfLGA, df.RES_UK_POST_CODE_PT1 == dfLGA.lga_Postcode, how="left")

  #Prior Education Location Joins PPL_FES_USER (4-18)
  df = df.join(dfPriorEducationLocationV.selectExpr("PriorEducationLocationDescription as QualificationDegreeLocation", "PelID_4", "_DLTrustedZoneTimeStamp as PelID_4_DLTrustedZoneTimeStamp"), df.PPL_FES_USER_4 == dfPriorEducationLocationV.PelID_4, how="left")
  df = df.join(dfPriorEducationLocationV.selectExpr("PriorEducationLocationDescription as QualificationAdvancedDiplomaLocation", "PelID_6", "_DLTrustedZoneTimeStamp as PelID_6_DLTrustedZoneTimeStamp"), df.PPL_FES_USER_6 == dfPriorEducationLocationV.PelID_6, how="left")
  df = df.join(dfPriorEducationLocationV.selectExpr("PriorEducationLocationDescription as QualificationAssociateDiplomaLocation", "PelID_8", "_DLTrustedZoneTimeStamp as PelID_8_DLTrustedZoneTimeStamp"), df.PPL_FES_USER_8 == dfPriorEducationLocationV.PelID_8, how="left")
  df = df.join(dfPriorEducationLocationV.selectExpr("PriorEducationLocationDescription as QualificationAdvancedCertificateLocation", "PelID_10", "_DLTrustedZoneTimeStamp as PelID_10_DLTrustedZoneTimeStamp"), df.PPL_FES_USER_10 == dfPriorEducationLocationV.PelID_10, how="left")
  df = df.join(dfPriorEducationLocationV.selectExpr("PriorEducationLocationDescription as QualificationTradeCertificateLocation", "PelID_12", "_DLTrustedZoneTimeStamp as PelID_12_DLTrustedZoneTimeStamp"), df.PPL_FES_USER_12 == dfPriorEducationLocationV.PelID_12, how="left")
  df = df.join(dfPriorEducationLocationV.selectExpr("PriorEducationLocationDescription as QualificationCertificateIILocation", "PelID_14", "_DLTrustedZoneTimeStamp as PelID_14_DLTrustedZoneTimeStamp"), df.PPL_FES_USER_14 == dfPriorEducationLocationV.PelID_14, how="left")
  df = df.join(dfPriorEducationLocationV.selectExpr("PriorEducationLocationDescription as QualificationCertificateILocation", "PelID_16", "_DLTrustedZoneTimeStamp as PelID_16_DLTrustedZoneTimeStamp"), df.PPL_FES_USER_16 == dfPriorEducationLocationV.PelID_16, how="left")
  df = df.join(dfPriorEducationLocationV.selectExpr("PriorEducationLocationDescription as QualificationOtherCertificateLocation", "PelID_18", "_DLTrustedZoneTimeStamp as PelID_18_DLTrustedZoneTimeStamp"), df.PPL_FES_USER_18 == dfPriorEducationLocationV.PelID_18, how="left")

  #Join Indigenous Behaviour
  df = df.join(dfIndigenousStatus, df.PPL_INDIGENOUS_STATUS == dfIndigenousStatus.IndigenousStatusID, how="left")
  
  #Join Student Academic Records
  df = df.join(dfPeopleASRV, df.PPL_PERSON_CODE == dfPeopleASRV.PASR_PERSON_CODE, how="left")
 
  #Calculated Columns
  df = df.withColumn("AboriginalGroup", \
                     when(col("PPL_INDIGENOUS_STATUS").isNull(), None)\
                     .when(col("PPL_INDIGENOUS_STATUS").isin([1,2,3]), 1)\
                     .otherwise(lit(0)))\
        .withColumn("AboriginalGroupDescription", \
                    when(col("PPL_INDIGENOUS_STATUS").isNull(), None)\
                    .when(col("PPL_INDIGENOUS_STATUS").isin([1,2,3]), "INDIGENOUS")\
                    .otherwise("NON INDIGENOUS"))\
        .withColumn("Age",  (round(datediff(lit(current_date()),col("PPL_DATE_OF_BIRTH")))/365).cast('integer'))\
        .withColumn("CompletedQualification", \
                  when((col("PASR_HAS_BACH_OR_HIGHER_DEGREE_008")=="Y") | (col("PASR_HAS_ADV_DIP_OR_ASSOC_DEG_410")=="Y")\
                       | (col("PASR_HAS_DIP_OR_ASSOC_DIP_420")=="Y") | (col("PASR_HAS_CERT_IV_OR_AD_CERT_TEC_511")=="Y")\
                       | (col("PASR_HAS_CERT_III_OR_TRADE_CERT_514")=="Y") | (col("PASR_HAS_OTHER_CERT_990")=="Y")\
                       | (col("PASR_HAS_CERT_II_521")=="Y") | (col("PASR_HAS_CERT_I_524")=="Y"), "Y")\
                  .otherwise(when((col("PASR_HAS_BACH_OR_HIGHER_DEGREE_008")=="N") & (col("PASR_HAS_ADV_DIP_OR_ASSOC_DEG_410")=="N")\
                                  & (col("PASR_HAS_DIP_OR_ASSOC_DIP_420")=="N") & (col("PASR_HAS_CERT_IV_OR_AD_CERT_TEC_511")=="N")\
                                  & (col("PASR_HAS_CERT_III_OR_TRADE_CERT_514")=="N") & (col("PASR_HAS_OTHER_CERT_990")=="N")\
                                  & (col("PASR_HAS_CERT_II_521")=="N") & (col("PASR_HAS_CERT_I_524")=="N"), "N")))\
        .withColumn("EnglishHelpFlag", \
                    when(col("PPL_FES_USER_1")=="Y", "Y")\
                    .otherwise("N"))\
        .withColumn("VisaProvided", \
                    when(col("VisaType").isNotNull(), "Yes - Visa Provided")\
                    .otherwise("No - Visa Blank"))\
        .withColumn("USIProvided", \
                    when(col("USI").isNotNull(), "Yes - USI Provided")\
                    .otherwise("No - USI Blank"))\
        .withColumn("ResidentialStatus", \
                    when(col("PPL_RESIDENTIAL_STATUS").isNotNull(), concat(col("PPL_RESIDENTIAL_STATUS"), lit(" "), col("ResidentialStatusDescription")))\
                    .otherwise(None))\
        .withColumn("StillAttendSchool", \
                    when(col("PPL_IS_AT_SCHOOL")=="Y", "Yes - Attends School")\
                    .when(col("PPL_IS_AT_SCHOOL")=="N", "No - Attends School")\
                    .otherwise(None))\
        .withColumn("HighestSchoolLevel", \
                    when(col("PPL_PRIOR_ATTAIN_LEVEL")=="02", "DID NOT GO TO SCHOOL")\
                    .when(col("PPL_PRIOR_ATTAIN_LEVEL")=="08", "YEAR 08 OR BELOW")\
                    .when(col("PPL_PRIOR_ATTAIN_LEVEL")=="09", "YEAR 09")\
                    .when(col("PPL_PRIOR_ATTAIN_LEVEL")=="10", "YEAR 10")\
                    .when(col("PPL_PRIOR_ATTAIN_LEVEL")=="11", "YEAR 11")\
                    .when(col("PPL_PRIOR_ATTAIN_LEVEL")=="12", "YEAR 12")\
                    .otherwise(None))\
        .withColumn("Gender", \
                    when(col("PPL_SEX")=="F", "Female")\
                    .when(col("PPL_SEX")=="M", "Male")\
                    .otherwise(None))\
        .withColumn("IndigenousFlag", \
                    when(col("PPL_INDIGENOUS_STATUS")==1, "Yes, Aboriginal")\
                    .when(col("PPL_INDIGENOUS_STATUS")==2, "Yes, Torres Strait Islander")\
                    .when(col("PPL_INDIGENOUS_STATUS")==3, "Yes, Aboriginal and Torres Strait Islanderr").otherwise("No"))\
        .withColumn("DisabilityNeedHelp", \
                    when(col("PPL_DIS_ACCESSED")=="Y","Yes - Help Dis")\
                    .otherwise("No - Help Dis"))\
        .withColumn("LanguageGroup", \
                    when(col("PPL_HOME_LANGUAGE").isNull(), None)\
                    .when(col("PPL_HOME_LANGUAGE").isin([1201, 9999]), 0)\
                    .otherwise(lit(1)))\
        .withColumn("LanguageGroupDescription", \
                    when(col("PPL_HOME_LANGUAGE").isNull(), None)\
                    .when(col("PPL_HOME_LANGUAGE") == 1201, "NON LBOTE")\
                    .when(col("PPL_HOME_LANGUAGE") == 9999, "NOT STATED")\
                    .otherwise("LBOTE"))\
        .withColumn("RegionalRemoteFlag", \
                    when(col("ARIA11B").isin(['INNER REGIONAL','OUTER REGIONAL' ,'REMOTE','VERY REMOTE']),  "REGIONAL/REMOTE")\
                    .otherwise(lit("NOT REGIONAL/REMOTE")))\
        .withColumn("HighestPreviousQualification", \
                    when(col("CompletedQualification") == 'Y',\
                       when(col("PASR_HAS_BACH_OR_HIGHER_DEGREE_008") == 'Y', "1. DEGREE OR HIGHER")\
                      .when(col("PASR_HAS_ADV_DIP_OR_ASSOC_DEG_410") == 'Y', "2. ADVANCED DIPLOMA")\
                      .when(col("PASR_HAS_DIP_OR_ASSOC_DIP_420") == 'Y', "3. DIPLOMA")\
                      .when(col("PASR_HAS_CERT_IV_OR_AD_CERT_TEC_511") == 'Y', "4. CERTIFICATE IV")\
                      .when(col("PASR_HAS_CERT_III_OR_TRADE_CERT_514") == 'Y', "5. CERTIFICATE III")\
                      .when(col("PASR_HAS_CERT_II_521") == 'Y', "6. CERTIFICATE II")\
                      .when(col("PASR_HAS_CERT_I_524") == 'Y', "7. CERTIFICATE I")\
                      .when(col("PASR_HAS_OTHER_CERT_990") == 'Y', "8. OTHER CERTIFICATE")\
                      .when((col("PASR_HAS_BACH_OR_HIGHER_DEGREE_008") == 'N')&(col("PASR_HAS_ADV_DIP_OR_ASSOC_DEG_410") == 'N')&(col("PASR_HAS_DIP_OR_ASSOC_DIP_420") == 'N')&(col("PASR_HAS_CERT_IV_OR_AD_CERT_TEC_511") == 'N')&(col("PASR_HAS_CERT_III_OR_TRADE_CERT_514") == 'N')&(col("PASR_HAS_CERT_II_521") == 'N')&(col("PASR_HAS_CERT_I_524") == 'N')&(col("PASR_HAS_OTHER_CERT_990") == 'N'), "0. NO PREVIOUS QUAL")\
                      .otherwise("9. NOT STATED"))\
                    .otherwise("0. NO PREVIOUS QUAL"))\
        .withColumn("HomePostcodeName",regexp_replace(trim(col("RES_TOWN")), "[!@#$%\\r\\n\\t]", ""))\
        .withColumn("HomePhone", regexp_replace(col("POS_TELEPHONE"), "[^0-9]+", ""))\
        .withColumn("WorkPhone", regexp_replace(col("PPL_FES_NOK_CONTACT_NO"), "[^0-9]+", ""))\
        .withColumn("MobilePhone", regexp_replace(col("PPL_MOBILE_PHONE_NUMBER"), "[^0-9]+", ""))\
        .withColumn("PostalAvetmissCountry", col("POS_USER_1"))\
        .withColumn("ResidentialAvetmissCountry", col("RES_USER_1"))

  df = df.withColumn("Age", (round(df["Age"], 0)).cast('integer'))

  #INCREMENTALS
  df = df.where(expr(f"(PPL__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}') \
        OR (reference.english_level._DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}') \
        OR (RES__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}') \
        OR (POS__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}') \
        OR (VISA__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}') \
        OR (reference.language._DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}') \
        OR (reference.country._DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}') \
        OR (reference.residential_status._DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}') \
        OR (reference.employment_status._DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}') \
        OR (reference.disability_status._DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}') \
        OR (trusted.oneebs_ebs_0165_people_usi._DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}') \
        OR (reference.seifa._DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}') \
        OR (lga__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}') \
        OR (PelID_4_DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}') \
        OR (PelID_6_DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}') \
        OR (PelID_8_DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}') \
        OR (PelID_10_DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}') \
        OR (PelID_12_DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}') \
        OR (PelID_14_DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}') \
        OR (PelID_16_DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}') \
        OR (PelID_18_DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}') \
        OR (reference.indigenous_status._DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}') \
        OR (PASR__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}') \
      "))

  #Final Select
  df = df.selectExpr(
    "CAST(PPL_PERSON_CODE AS BIGINT) as PersonCode"
    ,"PPL_REGISTRATION_NO as RegistrationNo"
    ,"USI"
    ,"AboriginalGroup"
    ,"AboriginalGroupDescription"
    ,"Age"
    ,"PPL_INDIGENOUS_STATUS as ATSICode" 
    ,"IndigenousStatusDescription as ATSICodeDescription"
    ,"PPL_OVERSEAS as InternationalFlag"
    ,"PPL_DATE_OF_BIRTH as DateOfBirth"
    ,"CompletedQualification"
    ,"PPL_FES_COUNTRY_OF_BIRTH as CountryOfBirth"
    ,"CountryName as CountryOfBirthDescription"
    ,"IndigenousFlag"
 	,"PPL_LEARN_DIFF as DisabilityFlag"
	,"DisabilityStatusDescription as DisabilityFlagDescription"
	,"DisabilityNeedHelp"
	,"PPL_DISABILITY as DisabilityTypeOrig"
	,"PPL_COLLEGE_EMAIL as EmailAddress"
	,"PPL_CURREMPSTATUS as EmploymentStatus"
	,"EmploymentStatusDescription"
	,"EnglishHelpFlag"
	,"PPL_F_LANG as EnglishLevel"
	,"EnglishLevelDescription"
	,"PPL_FORENAME as Forename"
	,"Gender"
	,"HighestSchoolLevel"
    ,"HomePhone"
    ,"WorkPhone"
    ,"MobilePhone"
	,"RES_UK_POST_CODE_PT1 as HomePostCode"
    ,"HomePostcodeName"
	,"LanguageGroup"
	,"LanguageGroupDescription"
	,"PPL_HOME_LANGUAGE as LanguageSpoken"
	,"LanguageDescription as LanguageSpokenDescription"
	,"PPL_MIDDLE_NAMES as MiddleNames"
	,"PPL_FES_USER_49 as MultiPriorEducationLocation"
	,"POS_ADDRESS_LINE_1 as PostalAddressLine1"
    ,"POS_ADDRESS_LINE_2 as PostalAddressLine2"
	,"POS_ADDRESS_LINE_3 as PostalAddressLine3"
	,"POS_ADDRESS_LINE_4 as PostalAddressLine4"
	,"POS_ADDRESS_LINE_5 as PostalAddressLine5"
	,"POS_COUNTRY as PostalAddressCountry"
	,"POS_UK_POST_CODE_PT1 as PostalAddressPostcode"
	,"POS_Town as PostalAddressPostcodeName"
	,"POS_REGION  as PostalAddressRegion"
    ,"PostalAvetmissCountry"
	,"PASR_HAS_CERT_IV_OR_AD_CERT_TEC_511 as QualificationAdvancedCertificate"
	,"QualificationAdvancedCertificateLocation"
	,"PASR_HAS_ADV_DIP_OR_ASSOC_DEG_410 as QualificationAdvancedDiploma"
	,"QualificationAdvancedDiplomaLocation"
	,"PASR_HAS_DIP_OR_ASSOC_DIP_420 as QualificationAssociateDiploma"
	,"QualificationAssociateDiplomaLocation"
	,"PASR_HAS_CERT_I_524 as QualificationCertificateI"
	,"QualificationCertificateILocation"
	,"PASR_HAS_CERT_II_521 as QualificationCertificateII"
	,"QualificationCertificateIILocation"
	,"PASR_HAS_BACH_OR_HIGHER_DEGREE_008 as QualificationDegree"
	,"QualificationDegreeLocation"
	,"PASR_HAS_OTHER_CERT_990 as QualificationOtherCertificate"
	,"QualificationOtherCertificateLocation"
	,"PASR_HAS_CERT_III_OR_TRADE_CERT_514 as QualificationTradeCertificate"
	,"QualificationTradeCertificateLocation"
	,"RES_ADDRESS_LINE_1 as ResidentialAddressLine1"
	,"RES_ADDRESS_LINE_2 as ResidentialAddressLine2"
	,"RES_ADDRESS_LINE_3 as ResidentialAddressLine3"
	,"RES_ADDRESS_LINE_4 as ResidentialAddressLine4"
	,"RES_ADDRESS_LINE_5 as ResidentialAddressLine5"
	,"RES_COUNTRY as ResidentialCountry"
    ,"RES_Region as ResidentialRegion"
    ,"ResidentialAvetmissCountry"
	,"StillAttendSchool"
	,"PPL_SURNAME as Surname"
	,"PPL_TITLE as Title"
	,"ResidentialStatusDescription as VisaClassName"
	,"PPL_RESIDENTIAL_STATUS as VisaClassNo"
	,"PPL_YEAR_LEFT as YearOfArrival"
	,"PPL_LAST_SCHOOL_YEAR as YearSchoolCompleted"
    ,"DATE_USI_VERIFIED as DateUSIVerified"
    ,"HAS_USI_BEEN_MANUALLY_VERIFIED as USIManuallyVerified"
	,"VisaType"
	,"VISA_SUB_CLASS as VisaSubClass"
	,"DESCRIPTION as VisaSubClassDescription"
	,"VISA_HOLDER_CODE as VisaHolderCode"
	,"VisaHolderTypeDescription as VisaHolderDescription"
    ,"IS_TEMPORARY_VISA_HOLDER as IsTemporaryVisaHolder"
	,"ISSUE_DATE as VisaIssueDate"
	,"EXPIRY_DATE as VisaExpiryDate"
    ,"ARIA11B"
    ,"HighestPreviousQualification"
    ,"RegionalRemoteFlag"
    ,"Decile as SEIFADecile"
    ,"Percentile as SEIFAPercentile"
    ,"CASE \
        WHEN lga_Postcode IS NOT NULL THEN lga_LGAName \
        WHEN ResidentialAvetmissCountry = '1101' AND RES_Region <> 'NSW' THEN '_INTERSTATE' \
        WHEN ResidentialAvetmissCountry IS NOT NULL AND ResidentialAvetmissCountry NOT IN ('0001', '0000', 'AN','1101') THEN '_OFF-SHORE/MIGRA' \
        ELSE '_UNKNOWN LGA' \
      END as LGA_Name"
    ,"ResidentialStatus"
    ,"USIProvided"
    ,"VisaProvided"
    ,"AusState"
    ,"SA4NAME"
    ,"SA2NAME"
    ,"PASR_SURVEY_CONTACT_STATUS as SurveyContactStatus"
  ).dropDuplicates()
  
  #Fix PRADA-1515, PRADA-1522, PRADA-1597
  columns = ["Forename" ,"Surname" ,"MiddleNames" ,"HomePostcodeName" ,"PostalAddressLine1" ,"PostalAddressLine2" ,"PostalAddressLine3" ,"PostalAddressLine4" ,"PostalAddressLine5" ,"PostalAddressPostcodeName" ,"ResidentialAddressLine1" ,"ResidentialAddressLine2" ,"ResidentialAddressLine3" ,"ResidentialAddressLine4" ,"ResidentialAddressLine5"]
  for c in columns:
    df=CleanseColumn(df, c)

  return df

# COMMAND ----------


