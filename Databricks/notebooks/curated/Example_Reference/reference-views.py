# Databricks notebook source
# MAGIC %run ../../includes/include-all-util

# COMMAND ----------

#REFERENCE
USER_DEFINED_VIEWS_LIST = [
  {"name" : "funding_source", 
   "sql" : "SELECT \
  A.Ref_1 AS FundingSourceCode \
  ,A.Ref_2 AS FundingSourceName \
  ,A.Ref_3 AS FundingSourceDescription \
  ,B.Ref_10 AS FundingSourceNationalID \
  ,C.Ref_10 AS SpecificFundingCodeID \
  ,A.Ref_6 AS Active \
  ,A._DLRawZoneTimeStamp _DLRawZoneTimeStamp \
  ,A._DLTrustedZoneTimeStamp _DLTrustedZoneTimeStamp \
  ,A._DLTrustedZoneTimeStamp _DLCuratedZoneTimeStamp \
  ,A._RecordStart _RecordStart \
  ,A._RecordEnd _RecordEnd \
  ,A._RecordDeleted _RecordDeleted \
  ,A._RecordCurrent _RecordCurrent \
  FROM trusted.reference_master_reference_data A \
  LEFT JOIN trusted.reference_master_reference_data B ON B.Ref_8 = A.Ref_1 AND B.TABLE_NAME = 'VERIFIER_PROPERTIES' AND B.Ref_9 = 'DEC_FUNDING_SOURCE_NATIONAL_2015' \
  LEFT JOIN trusted.reference_master_reference_data C ON C.Ref_8 = A.Ref_1 AND C.TABLE_NAME = 'VERIFIER_PROPERTIES' AND C.Ref_9 = 'DEC_SPECIFIC_FUNDING_ID_2015' \
  WHERE \
  A.TABLE_NAME = 'VERIFIERS' \
  AND A.Ref_7 = 'FUNDING'"},
  {"name" : "waiver_types", 
   "sql" : "select waiver_code as WaiverTypeCode \
		, SHORT_DESCRIPTION as WaiverTypeName \
		, LONG_DESCRIPTION as WaiverTypeDescription \
		, DISABILITY_DEPENDENCIES as DisabilityDependencies \
		, EXCLUSIVE_WAIVER as ExclusiveWaiver \
		, EXCLUSIVE_FEE as ExclusiveFee \
		, FEE_EXEMPTION as FeeExemption \
		, IS_ONCE_PER_CALENDAR_YEAR as IsOncePerCalendarYear \
		, IS_FEE_HELP_APPLICABLE as IsFeeHelpApplicable \
		, IS_VET_FEE_HELP_APPLICABLE as IsVetFeeHelpApplicable \
		, Created_date as CreatedDate \
		, UPDATED_DATE as UpdatedDate \
from trusted.OneEBS_EBS_0165_WAIVER_TYPES \
WHERE _RecordCurrent = 1"},
  {"name" : "student_progress", 
   "sql" : "WITH pt AS \
( SELECT DISTINCT Type_code, Description FROM trusted.OneEBS_EBS_0165_PROGRESS_TYPES WHERE _recordcurrent = 1 ) \
SELECT TYPE_NAME AS ProgressCode,CONCAT(TYPE_NAME, ' - ' , FES_LONG_DESCRIPTION) AS ProgressName, Pt.DESCRIPTION AS ProgressType \
FROM trusted.OneEBS_EBS_0165_PROGRESS_CODES pc \
LEFT JOIN pt ON pc.TYPE_CODE = pt.TYPE_CODE WHERE pc._RecordCurrent = 1 AND pt.description <> 'Transfered'  "}
]

# COMMAND ----------

def CreateViews(sql, entity):
  if DeltaTableExists(f"reference.{entity}"):
    return
  print(sql)
  spark.sql(sql)
  friendlyName = GeneralToPascalCase(entity)
  sql = sql.replace(" REPLACE ", " ALTER ") \
            .replace("trusted.", "edw.") \
            .replace(f".{entity}", f".{friendlyName}") \
            .replace("ORDER BY 1", "")
  print(sql)
  _ExecuteSqlQuery(sql)

# COMMAND ----------

############################################################################################################################
# Function: CreateReferenceViews
#  CREATES VIEWS FROM TRUSTED INTO REFERNCE DATABSE AND SQL SERVER
# Parameters: 
#  None
# Returns:
#  None
#############################################################################################################################
def CreateReferenceViews():
  tableList = spark.sql("SHOW TABLES FROM trusted LIKE 'reference_*'")
  
  for t in tableList.rdd.collect():
    #if(t.tableName != "reference_master_reference_data"): continue
    table = t.tableName
    entity = table[10:]
    friendlyName = GeneralToPascalCase(entity)
    CreateViews(f"CREATE OR REPLACE VIEW reference.{entity} AS SELECT DISTINCT *, _DLTrustedZoneTimeStamp _DLCuratedZoneTimeStamp  FROM trusted.{table}", entity)

# COMMAND ----------

# DBTITLE 0,Materalise Master Reference Data
###########################################################################################################################
# Function: CreateMDSViews
#  USES verifiers_view_list AND master_reference_data FILES TO EXPLODE INTO INDIVIDUAL REFERENCE VIEWS
# Parameters: 
#  None
# Returns:
#  None
#############################################################################################################################
def CreateMDSViews():
  sqlSelectTemplate = "\
  SELECT \
  {columns}\
  ,V.Ref_6 Active \
  ,V._DLRawZoneTimeStamp _DLRawZoneTimeStamp \
  ,V._DLTrustedZoneTimeStamp _DLTrustedZoneTimeStamp \
  ,V._DLTrustedZoneTimeStamp _DLCuratedZoneTimeStamp \
  ,V._RecordStart _RecordStart \
  ,V._RecordEnd _RecordEnd \
  ,V._RecordDeleted _RecordDeleted \
  ,V._RecordCurrent _RecordCurrent \
  FROM trusted.reference_master_reference_data V \
  {joinProperties} \
  WHERE V.Ref_7 ='{rvDomain}' \
  AND V.TABLE_NAME = 'VERIFIERS' \
  ORDER BY 1"

  columnTemplate = "V.Ref_1 {lowValue}"
  viewList = "trusted.reference_verifiers_view_list"
  spark.sql(f"REFRESH TABLE {viewList}")
  tableList = spark.table(f"{viewList}")
  
  for row in tableList.rdd.collect():
    #if(row.friendly_name != "IndigenousStatus"): continue
    columns = columnTemplate.format(lowValue=row.low_value)
    columns += ", V.Ref_13 {abbreviation}".format(abbreviation=row.abbreviation) if row.abbreviation else ""
    columns += ", V.Ref_2 {fesShortDescription}".format(fesShortDescription=row.fes_short_description) if row.fes_short_description else ""

    if row.fes_long_description:
      for s in row.fes_long_description.split("|"):
        columns += ", V.Ref_3 {fesLongDescription}".format(fesLongDescription=s)

    columns += ", P.Ref_10 {propertyValue}".format(propertyValue=row.property_value) if row.property_value else ""
    joinProperties = "LEFT JOIN trusted.reference_master_reference_data P ON P.Ref_7 = '{rvDomain}' AND P.TABLE_NAME = 'VERIFIER_PROPERTIES' AND P.Ref_8 = V.Ref_1 AND P.Ref_9 IN ('{propertyNames}')".format(rvDomain=row.rv_domain, propertyNames=row.property_names.replace("|", "', '")) if row.property_names else ""
    selectSql = sqlSelectTemplate.format(viewName=row.internal_name, rvDomain=row.rv_domain, columns=columns, joinProperties=joinProperties)
    entity = row.internal_name

    CreateViews(f"CREATE OR REPLACE VIEW reference.{entity} AS {selectSql}", entity)

# COMMAND ----------

###########################################################################################################################
# Function: CreateUserDefinedViews
#  LOOPS THROUGH CREATION OF USER VIEWS
# Parameters: 
#  None
# Returns:
#  None
#############################################################################################################################
def CreateUserDefinedViews():
  for i in USER_DEFINED_VIEWS_LIST:
    entity = i["name"]
    sql = i["sql"]
    CreateViews(f"CREATE OR REPLACE VIEW reference.{entity} AS {sql}", entity)

# COMMAND ----------

def Main():
  CreateReferenceViews()
  CreateMDSViews()
  CreateUserDefinedViews()


# COMMAND ----------

Main()
