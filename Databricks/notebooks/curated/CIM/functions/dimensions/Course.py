# Databricks notebook source
def GetCIMCourse (unitInstancesDf, animalUseDf, qualificationTypeDf, occupationDf, industryDf, fieldOfEducationDf, productSubTypeDf, AQFLevelDf, startDate, endDate):
  #############################################
  # Function: GetCIMCourse
  # Description: Load Course Dimension
  # Returns:
  #  Dataframe of transformed Course records
  #############################################
  
  dfUnitInstancesDedup = unitInstancesDf.withColumn("RN", expr("row_number() over(partition by TRIM(FES_UNIT_INSTANCE_CODE) order by _transaction_date desc)")).filter(col("RN") == 1).drop("RN")
  
  dfUI = GeneralAliasDataFrameColumns(unitInstancesDf.where("UI_LEVEL = 'COURSE'"), "UI_")
  dfAnimalUse = GeneralAliasDataFrameColumns(animalUseDf, "AU_")
  dfQualificationType = GeneralAliasDataFrameColumns(qualificationTypeDf, "QT_")
  dfOccupation = GeneralAliasDataFrameColumns(occupationDf, "O_")
  dfIndustry = GeneralAliasDataFrameColumns(industryDf, "I_")
  dfFieldOfEducation = GeneralAliasDataFrameColumns(fieldOfEducationDf, "FOE_")
  dfProductSubType = GeneralAliasDataFrameColumns(productSubTypeDf, "PST_")
  dfAQFLevel = GeneralAliasDataFrameColumns(AQFLevelDf, "AQFL_")
  
  
  #Join Tables
  df = dfUI.join(dfAnimalUse, dfAnimalUse.AU_AnimalUseCode == dfUI.UI_FES_USER_5, how="left") \
      .join(dfQualificationType, dfQualificationType.QT_QualificationTypeID == dfUI.UI_AWARD_CATEGORY_CODE, how="left") \
      .join(dfOccupation, dfOccupation.O_OccupationCode == dfUI.UI_COURSE_OCCUPATION, how="left") \
      .join(dfIndustry, dfIndustry.I_IndustryCode == dfUI.UI_CENSUS_SUBJECT, how="left") \
      .join(dfFieldOfEducation, dfFieldOfEducation.FOE_FieldOfEducationID == dfUI.UI_NZSCED, how="left") \
      .join(dfProductSubType, dfProductSubType.PST_ProductSubTypeCode == dfUI.UI_UNIT_CATEGORY, how="left") \
      .join(dfAQFLevel, dfAQFLevel.AQFL_AQFLevelID == dfUI.UI_QUALIFICATION_LEVEL, how="left")
  
  #INCREMENTALS
  df = df.where(expr(f"UI__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
                    OR AU__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
                    OR QT__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
                    OR O__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
                    OR I__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
                    OR FOE__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
                    OR PST__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
                    OR AQFL__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
                    "))
  
  #Select
  df = df.selectExpr("CAST(UI_UI_ID AS BIGINT) AS CourseId"
    ,"UI_FES_UNIT_INSTANCE_CODE AS CourseCode"
    ,"UI_FES_LONG_DESCRIPTION AS CourseName"
    ,"UI_PROSP_USER_8 AS CourseNamePublished"
    ,"UI_FES_STATUS AS CourseStatus"
    ,"UI_FES_USER_5 AS AnimalUseCode"
    ,"AU_AnimalUseName AS AnimalUseName"
    ,"UI_COURSE_OCCUPATION AS OccupationCode"
    ,"O_OccupationName AS OccupationName"
    ,"UI_CENSUS_SUBJECT AS IndustryCode"
    ,"I_IndustryName AS IndustryName"
    ,"UI_AWARD_CATEGORY_CODE AS QualificationTypeCode"
    ,"QT_QualificationTypeName AS QualificationTypeName"
    ,"UI_NZSCED AS FieldOfEducationId"
    ,"FOE_FieldOfEducationName AS FieldOfEducationName"
    ,"FOE_FieldOfEducationDescription AS FieldOfEducationDescription"
    ,"UI_IS_FEE_HELP AS IsFEEHelp"
    ,"UI_IS_VET_FEE_HELP AS IsVETFEEHelp"
    ,"UI_IS_VOCATIONAL AS IsVocational"
    ,"UI_NATIONAL_COURSE_CODE as NationalCourseCode"
    ,"UI_MAXIMUM_HOURS as NominalHours"
    ,"UI_FES_USER_10 AS RecommendedUsage"
    ,"UI_SCOPE_APPROVED AS ScopeApproved"
    ,"UI_UNIT_CATEGORY as CourseCategory"
    ,"PST_ProductSubTypeName AS CourseCategoryName"
    ,"UI_UI_type as CourseType"
    ,"UI_ALTERNATIVE_NAME as Specialisation"
    ,"UI_FES_USER_4 as ReleaseNumber"
    ,"UI_QUALIFICATION_LEVEL as AQFLevelID"
    ,"AQFL_AQFLevelName AS AQFLevelName"
    ,"AQFL_AQFLevelDescription AS AQFLevelDescription"
    ,"UI_EFFECTIVE_START_DATE as StartDate"
    ,"UI_EFFECTIVE_END_DATE as EndDate"
  )

  return df
