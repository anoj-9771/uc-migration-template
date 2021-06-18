# Databricks notebook source
# MAGIC %run ../../../includes/include-all-util

# COMMAND ----------

#CONSTANTS
COMPLIANCE_CONST_STAGE_DATABASE = "stage"
COMPLIANCE_CONST_CURATED_DATABASE = "compliance"
COMPLIANCE_CONST_DATALAKE_FOLDER = "compliance"
COMPLIANCE_CONST_SQL_SCHEMA_STAGE = "stage"
COMPLIANCE_CONST_SQL_SCHEMA_TARGET = "compliance"

# COMMAND ----------

###########################################################################################################################
# Function: AvetmissMergeSCD
#  HANDLES DEFAULT VAULES BEFORE CALLING MergeSCD
# Parameters: 
#  dataframe - DataFrame - Dataframe to persist
#  targetTable - String - In form of entity_name to automattically PascalCase when landing to SQL Server
#  businessKey - String - Business Key to set on merge sql
# Returns:
#  NULL
#############################################################################################################################
def AvetmissMergeSCD(dataFrame, targetTable, businessKey):
  dataLakeMount = DataLakeGetMountPoint(ADS_CONTAINER_CURATED)
  curatedPath = "{dataLakeMount}/{folder}/{tableName}".format(dataLakeMount=dataLakeMount, folder=COMPLIANCE_CONST_DATALAKE_FOLDER, tableName=targetTable)
  stageTableName = "{db}.{table}".format(db=COMPLIANCE_CONST_STAGE_DATABASE, table=targetTable)
  deltaTableName = "{db}.{table}".format(db=COMPLIANCE_CONST_CURATED_DATABASE, table=targetTable)
  sqlTableName = "{table}".format(table=GeneralToPascalCase(targetTable))
  sqlStageTable = "{schema}.{table}".format(schema=COMPLIANCE_CONST_SQL_SCHEMA_STAGE, table=sqlTableName)
  sqlTargetTable = "{schema}.{table}".format(schema=COMPLIANCE_CONST_SQL_SCHEMA_TARGET, table=sqlTableName)
  
  MergeSCD(dataFrame=dataFrame
           ,businessKey=businessKey
           ,dlStageTableFqn=stageTableName
           ,dlTargetTableFqn=deltaTableName
           ,dataLakePath = curatedPath
           ,sqlStageTableFqn=sqlStageTable
           ,sqlTargetFqn=sqlTargetTable)

# COMMAND ----------

###########################################################################################################################
# Function: AvetmissAppend
#  HANDLES DEFAULT VAULES BEFORE CALLING AvetmissAppend
# Parameters: 
#  dataframe - DataFrame - Dataframe to persist
#  targetTable - String - In form of entity_name to automattically PascalCase when landing to SQL Server
#  businessKey - String - Business Key to overwrite on
# Returns:
#  NULL
#############################################################################################################################
def AvetmissAppend(dataFrame, targetTable, businessKey):
  dataLakeMount = DataLakeGetMountPoint(ADS_CONTAINER_CURATED)
  curatedPath = "{dataLakeMount}/{folder}/{tableName}".format(dataLakeMount=dataLakeMount, folder=COMPLIANCE_CONST_DATALAKE_FOLDER, tableName=targetTable)
  stageTableName = "{db}.{table}".format(db=COMPLIANCE_CONST_STAGE_DATABASE, table=targetTable)
  deltaTableName = "{db}.{table}".format(db=COMPLIANCE_CONST_CURATED_DATABASE, table=targetTable)
  sqlTableName = "{table}".format(table=GeneralToPascalCase(targetTable))
  sqlStageTable = "{schema}.{table}".format(schema=COMPLIANCE_CONST_SQL_SCHEMA_STAGE, table=sqlTableName)
  sqlTargetTable = "{schema}.{table}".format(schema=COMPLIANCE_CONST_SQL_SCHEMA_TARGET, table=sqlTableName)
  
  _AppendDeltaAndSql(dataFrame=dataFrame
           ,businessKey=businessKey
           ,dlStageTableFqn=stageTableName
           ,dlTargetTableFqn=deltaTableName
           ,dataLakePath = curatedPath
           ,sqlStageTableFqn=sqlStageTable
           ,sqlTargetFqn=sqlTargetTable)

# COMMAND ----------


