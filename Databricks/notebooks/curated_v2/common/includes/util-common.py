# Databricks notebook source
# MAGIC %run ../../../includes/include-all-util

# COMMAND ----------

#CONSTANTS
CIM_CONST_STAGE_DATABASE = "stage"
CIM_CONST_CURATED_DATABASE = "common"
CIM_CONST_DATALAKE_FOLDER = "common"
CIM_CONST_SQL_SCHEMA_STAGE = "stage"
CIM_CONST_SQL_SCHEMA_TARGET = "common"

# COMMAND ----------

###########################################################################################################################
# Function: CIMMergeSCD
#  HANDLES DEFAULT VAULES BEFORE CALLING MergeSCD
# Parameters: 
#  dataframe - DataFrame - Dataframe to persist
#  targetTable - String - In form of entity_name to automattically PascalCase when landing to SQL Server
#  businessKey - String - Business Key to set on merge sql
# Returns:
#  NULL
#############################################################################################################################
def CIMMergeSCD(dataFrame, targetTable, businessKey):
  dataLakeMount = DataLakeGetMountPoint(ADS_CONTAINER_CURATED)
  curatedPath = "{dataLakeMount}/{folder}/{tableName}".format(dataLakeMount=dataLakeMount, folder=CIM_CONST_DATALAKE_FOLDER, tableName=targetTable)
  stageTableName = "{db}.{table}".format(db=CIM_CONST_STAGE_DATABASE, table=targetTable)
  deltaTableName = "{db}.{table}".format(db=CIM_CONST_CURATED_DATABASE, table=targetTable)
  sqlTableName = "{table}".format(table=GeneralToPascalCase(targetTable))
  sqlStageTable = "{schema}.{table}".format(schema=CIM_CONST_SQL_SCHEMA_STAGE, table=sqlTableName)
  sqlTargetTable = "{schema}.{table}".format(schema=CIM_CONST_SQL_SCHEMA_TARGET, table=sqlTableName)
  
  MergeSCD(dataFrame=dataFrame
           ,businessKey=businessKey
           ,dlStageTableFqn=stageTableName
           ,dlTargetTableFqn=deltaTableName
           ,dataLakePath = curatedPath
           ,sqlStageTableFqn=sqlStageTable
           ,sqlTargetFqn=sqlTargetTable)

# COMMAND ----------

###########################################################################################################################
# Function: CIMAppend
#  HANDLES DEFAULT VAULES BEFORE CALLING CIMAppend
# Parameters: 
#  dataframe - DataFrame - Dataframe to persist
#  targetTable - String - In form of entity_name to automattically PascalCase when landing to SQL Server
#  businessKey - String - Business Key to overwrite on
# Returns:
#  NULL
#############################################################################################################################
def CIMAppend(dataFrame, targetTable, businessKey):
  dataLakeMount = DataLakeGetMountPoint(ADS_CONTAINER_CURATED)
  curatedPath = "{dataLakeMount}/{folder}/{tableName}".format(dataLakeMount=dataLakeMount, folder=CIM_CONST_DATALAKE_FOLDER, tableName=targetTable)
  stageTableName = "{db}.{table}".format(db=CIM_CONST_STAGE_DATABASE, table=targetTable)
  deltaTableName = "{db}.{table}".format(db=CIM_CONST_CURATED_DATABASE, table=targetTable)
  sqlTableName = "{table}".format(table=GeneralToPascalCase(targetTable))
  sqlStageTable = "{schema}.{table}".format(schema=CIM_CONST_SQL_SCHEMA_STAGE, table=sqlTableName)
  sqlTargetTable = "{schema}.{table}".format(schema=CIM_CONST_SQL_SCHEMA_TARGET, table=sqlTableName)
  
  _AppendDeltaAndSql(dataFrame=dataFrame
           ,businessKey=businessKey
           ,dlStageTableFqn=stageTableName
           ,dlTargetTableFqn=deltaTableName
           ,dataLakePath = curatedPath
           ,sqlStageTableFqn=sqlStageTable
           ,sqlTargetFqn=sqlTargetTable)

# COMMAND ----------


