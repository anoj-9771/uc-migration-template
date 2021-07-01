# Databricks notebook source
def GetAvetmissCourse (unitInstancesDf, uiLinksDf, animalUseDf, qualificationTypeDf, occupationDf, industryDf, resourceAllocationDf, fieldOfEducationDf, productSubTypeDf, avetCourseDf, startDate, endDate):
  #############################################
  # Function: GetAvetmissCourse
  # Description: Load Course Dimension
  # Returns:
  #  Dataframe of transformed Course records
  #############################################
  
  dfUnitInstancesDedup = unitInstancesDf.withColumn("RN", expr("row_number() over(partition by TRIM(FES_UNIT_INSTANCE_CODE) order by _transaction_date desc)")).filter(col("RN") == 1).drop("RN")
  
  dfUI = GeneralAliasDataFrameColumns(dfUnitInstancesDedup.where("CTYPE_CALENDAR_TYPE_CODE = 'COURSE'"), "UI_")
  dfTP = GeneralAliasDataFrameColumns(dfUnitInstancesDedup.where("CTYPE_CALENDAR_TYPE_CODE = 'TP'"), "TP_")
  dfUILinks = GeneralAliasDataFrameColumns(uiLinksDf.withColumn("RN", expr("ROW_NUMBER() OVER(PARTITION BY UI_CODE_TO ORDER BY UI_CODE_FROM)"))\
                                                    .filter(col("RN") == 1).drop("RN"), "UIL_")
  dfAnimalUse = GeneralAliasDataFrameColumns(animalUseDf, "AU_")
  dfQualificationType = GeneralAliasDataFrameColumns(qualificationTypeDf, "QT_")
  dfOccupation = GeneralAliasDataFrameColumns(occupationDf, "O_")
  dfIndustry = GeneralAliasDataFrameColumns(industryDf, "I_")
  dfResourceAllocation = GeneralAliasDataFrameColumns(resourceAllocationDf, "RA_")
  dfFieldOfEducation = GeneralAliasDataFrameColumns(fieldOfEducationDf, "FOE_")
  dfProductSubType = GeneralAliasDataFrameColumns(productSubTypeDf, "PST_")
  dfAvetCourse =  GeneralAliasDataFrameColumns(avetCourseDf, "AC_")
  
  #Get Training Packages beforehand
  dfUITP = GeneralAliasDataFrameColumns(dfUI.join(dfUILinks, (dfUI.UI_FES_UNIT_INSTANCE_CODE == dfUILinks.UIL_UI_CODE_TO) & (dfUI.UI_UNIT_CATEGORY == 'TPQ'))\
                                            .join(dfTP, (dfUILinks.UIL_UI_CODE_FROM == dfTP.TP_FES_UNIT_INSTANCE_CODE) & (dfTP.TP_CTYPE_CALENDAR_TYPE_CODE == 'TP')) \
                                            .selectExpr("UI_FES_UNIT_INSTANCE_CODE as CourseCode",
                                                        "UIL_UI_CODE_FROM  as TrainingPackageCode",
                                                        "TP_FES_LONG_DESCRIPTION as TrainingPackageName",
                                                        "greatest(UI__DLTrustedZoneTimeStamp, UIL__DLTrustedZoneTimeStamp, TP__DLTrustedZoneTimeStamp) as _DLTrustedZoneTimeStamp"), "UITP_")
  
  #Get CourseCodeConverted to join with AvetmissCourse to get AvetmissCourseCode 
  dfUI = dfUI.withColumn("UI_CourseCodeConverted",expr("\
    CASE \
      WHEN UI_NATIONAL_COURSE_CODE IS NOT NULL THEN UI_NATIONAL_COURSE_CODE \
      WHEN UI_CTYPE_CALENDAR_TYPE_CODE = 'COURSE' and LEFT(RIGHT(TRIM(UI_FES_UNIT_INSTANCE_CODE), 3), 1) = 'V' \
               AND LEFT(RIGHT(TRIM(UI_FES_UNIT_INSTANCE_CODE), 2), 1) IN ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9') THEN \
                      SUBSTRING(TRIM(UI_FES_UNIT_INSTANCE_CODE), 1, CHAR_LENGTH(TRIM(UI_FES_UNIT_INSTANCE_CODE))-3) \
      ELSE TRIM(UI_FES_UNIT_INSTANCE_CODE) \
    END"))
  
  #Join Tables
  df = dfUI.join(dfAnimalUse, dfAnimalUse.AU_AnimalUseCode == dfUI.UI_FES_USER_5, how="left") \
      .join(dfQualificationType, dfQualificationType.QT_QualificationTypeID == dfUI.UI_AWARD_CATEGORY_CODE, how="left") \
      .join(dfOccupation, dfOccupation.O_OccupationCode == dfUI.UI_COURSE_OCCUPATION, how="left") \
      .join(dfIndustry, dfIndustry.I_IndustryCode == dfUI.UI_CENSUS_SUBJECT, how="left") \
      .join(dfResourceAllocation, dfResourceAllocation.RA_ResourceAllocationModelCategoryID == dfUI.UI_RAM_CATEGORY, how="left") \
      .join(dfFieldOfEducation, dfFieldOfEducation.FOE_FieldOfEducationID == dfUI.UI_NZSCED, how="left") \
      .join(dfProductSubType, dfProductSubType.PST_ProductSubTypeCode == dfUI.UI_UNIT_CATEGORY, how="left") \
      .join(dfUITP, dfUITP.UITP_CourseCode == dfUI.UI_FES_UNIT_INSTANCE_CODE, how="left") \
      .join(dfAvetCourse, dfAvetCourse.AC_CourseCodeConverted == dfUI.UI_CourseCodeConverted, how="left")
  
  #INCREMENTALS
  df = df.where(expr(f"UI__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
                    OR AU__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
                    OR QT__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
                    OR O__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
                    OR I__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
                    OR RA__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
                    OR FOE__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
                    OR PST__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
                    OR UITP__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
                    OR AC__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
                    "))
  
  #PRADA-1658
  df = df.withColumn("UITP_TrainingPackageCode", expr("SPLIT(TRIM(UITP_TrainingPackageCode), '[.]')[0]"))
  
  #Select
  df = df.selectExpr("UI_UI_ID AS CourseId"
    ,"UI_FES_UNIT_INSTANCE_CODE AS CourseCode"
    ,"UI_FES_LONG_DESCRIPTION AS CourseName"
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
    ,"CASE WHEN UI_IS_FEE_HELP = 'Y' THEN 'Yes - FEE HELP' ELSE 'No - FEE HELP' END AS IsFEEHelp"
    ,"CASE WHEN UI_IS_VET_FEE_HELP = 'Y' THEN 'Yes - VET FEE HELP' ELSE 'No - VET FEE HELP' END AS IsVETFEEHelp"
    ,"CASE WHEN UI_IS_VOCATIONAL = 'Y' THEN 'Yes - Vocational' ELSE 'No - Vocational' END AS IsVocational"
    ,"UI_NATIONAL_COURSE_CODE as NationalCourseCode"
    ,"UI_MAXIMUM_HOURS as NominalHours"
    ,"UI_RAM_CATEGORY as ResourceAllocationModelCategoryCode"
    ,"RA_ResourceAllocationModelCategoryName AS ResourceAllocationModelCategoryName"
    ,"UI_FES_USER_10 AS RecommendedUsage"
    ,"UI_SCOPE_APPROVED AS ScopeApproved"
    ,"UI_UNIT_CATEGORY as CourseCategory"
    ,"PST_ProductSubTypeName AS CourseCategoryName"
    ,"UI_CourseCodeConverted AS CourseCodeConverted"
    ,"UITP_TrainingPackageCode as TrainingPackageCode"
    ,"UITP_TrainingPackageName as TrainingPackageName"
    ,"CASE \
       WHEN (UITP_TrainingPackageCode IS NOT NULL OR UI_UNIT_CATEGORY = 'TPQ') THEN LEFT(UI_NATIONAL_COURSE_CODE, 3)  \
       WHEN UI_UNIT_CATEGORY = 'AC' AND RIGHT(UI_NATIONAL_COURSE_CODE,2) = 'SA' THEN RIGHT(UI_NATIONAL_COURSE_CODE, 2) \
       WHEN UI_UNIT_CATEGORY = 'AC' THEN RIGHT(UI_NATIONAL_COURSE_CODE, 3) \
       ELSE NULL \
     END AS TrainingPackage3Digit"
    ,"CASE \
       WHEN (UITP_TrainingPackageCode IS NOT NULL OR UI_UNIT_CATEGORY = 'TPQ') THEN LEFT(UI_NATIONAL_COURSE_CODE, 4)  \
       ELSE NULL \
     END AS TrainingPackage4Digit"
    ,"CASE \
       WHEN (UITP_TrainingPackageCode IS NOT NULL OR ui_UNIT_CATEGORY = 'TPQ') THEN LEFT(ui_NATIONAL_COURSE_CODE, 6)  \
       ELSE NULL \
     END AS TrainingPackage6Digit"
    ,"UI_UI_type as UIType"
    ,"CASE WHEN AC_AvetmissCourseCode IS NOT NULL THEN AC_AvetmissCourseCode ELSE LEFT(UI_CourseCodeConverted,10) END AS AvetmissCourseCode"
  )

  return df
