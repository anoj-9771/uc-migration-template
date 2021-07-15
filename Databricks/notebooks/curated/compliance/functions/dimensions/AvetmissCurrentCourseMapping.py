# Databricks notebook source
############################################################################################################################  
# Function: GetAvetmissCurrentCourseMapping
#  Returns a dataframe that for the Current Course Mapping
# Parameters:
#  dfUnitInstances = Input DataFrame - EBS Unit Instances
#  dfSkillsList = Input DataFrame - Reference Skills List
#  dfCourseReplacement = Input DataFrame - Reference Course Replacement
# Returns:
#  A dataframe that has all attributes for the Current Course Mapping
##############################################################################################################################
def GetAvetmissCurrentCourseMapping(dfUnitInstances, dfSkillsList, dfCourseReplacement, dfAvetmissCourse, startDate, endDate):
  dfAvetmissCourse = GeneralAliasDataFrameColumns(dfAvetmissCourse.selectExpr("CourseCode", "AvetmissCourseCode"), "C_")

  df = dfUnitInstances
  df = df.where(expr(f"CTYPE_CALENDAR_TYPE_CODE = 'COURSE' \
                     AND FES_STATUS NOT IN ('DRAFT', 'PENDING') \
                     AND FES_LONG_DESCRIPTION IS NOT NULL \
                     AND LOWER(FES_LONG_DESCRIPTION) not like '%do not use%' \
                     AND (YEAR(EFFECTIVE_END_DATE) >= 2014 OR EFFECTIVE_END_DATE IS NULL) \
                     AND _DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
                     ")) \
    .withColumn("OldCourseCode", expr("COALESCE(NATIONAL_COURSE_CODE, FES_UNIT_INSTANCE_CODE)")) \
    .join(dfAvetmissCourse, expr("C_CourseCode == FES_UNIT_INSTANCE_CODE"), how="left") \
    .withColumn("CourseCode", expr("COALESCE(C_AvetmissCourseCode, NATIONAL_COURSE_CODE, FES_UNIT_INSTANCE_CODE)")) \
    .withColumn("ROW_NUMBER", expr("RANK() OVER (PARTITION BY CourseCode ORDER BY EFFECTIVE_START_DATE DESC, UI_ID DESC)")) \
    .where(expr("ROW_NUMBER = 1")) \
    .selectExpr( \
      "CourseCode" \
      ,"NATIONAL_COURSE_CODE" \
      ,"FES_UNIT_INSTANCE_CODE" \
      ,"FES_LONG_DESCRIPTION CourseName" \
  )

  #SKILLS LIST PIVOT TABLE
  pivotCols = [2015+x for x in range(26)]
  dfSkillsList = dfSkillsList.where(expr(f"_DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}'")).groupBy("NationalCourseCode").pivot("SkillsListYear", pivotCols).max("SkillsListYear")

  #ALIAS TABLES
  dfCourseReplacement = GeneralAliasDataFrameColumns(dfCourseReplacement.where(expr(f"_DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}'")), "CR_")
  dfBaseReplacement = GeneralAliasDataFrameColumns(df, "BASE_")
  
  #JOIN TABLES
  df = df.join(dfCourseReplacement, dfCourseReplacement.CR_NationalCourseCode == df.CourseCode, how="left") \
    .join(dfBaseReplacement, dfBaseReplacement.BASE_CourseCode == dfCourseReplacement.CR_ReplacementNationalCourseCode, how="left") \
    .join(dfSkillsList, dfSkillsList.NationalCourseCode == df.CourseCode, how="left") \
  
  df = df.withColumn("CurrentCourseCode", expr("COALESCE(BASE_CourseCode, CourseCode)")) \
    .withColumn("CurrentCourseName", expr("COALESCE(BASE_CourseName, CourseName)")) \
    .withColumn("CurrentCourseOnSL", expr("CASE WHEN NationalCourseCode IS NOT NULL THEN 'Y' ELSE 'N' END")) \
    .withColumn("OldCourseCode", expr("CourseCode")) \
    .withColumn("OldCourseName", expr("CourseName")) \
    .withColumn("VersionOnSkillsList", expr("CASE WHEN CurrentCourseCode = OldCourseCode THEN 'Current Course' ELSE 'Superseded' END"))

  returnColumns = ["CurrentCourseCode", "CurrentCourseName", "CurrentCourseOnSL", "OldCourseCode", "OldCourseName", "VersionOnSkillsList"]
  for col in pivotCols: 
    df = df.withColumn(str(col), when(df[str(col)] > 0, "Y").otherwise("N"))
    returnColumns.append(str(col))
    
  return df.select(returnColumns)

# COMMAND ----------


