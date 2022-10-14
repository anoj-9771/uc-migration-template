# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import *

# COMMAND ----------

def SapPreprocessCleansed(sourceDataFrame,businessKey,sourceRecordDeletion,sourceQuery,watermarkColumn=None):
    businessKeyList = list(businessKey.split(","))
    if sourceQuery[0:3].lower() in ('crm','isu'):
        sortOrderList = ["extract_datetime","di_sequence_number"] if watermarkColumn else ["extract_datetime"]
        whereClause = "di_operation_type != 'X' AND di_operation_type != 'D'"
    else:
        sortOrderList = ["delta_ts"]
        whereClause = "is_deleted != 'Y'"
        
    #FILTER DELETED RECORDS FROM SOURCE    
    sourceDataFrame = sourceDataFrame.where(whereClause) if sourceRecordDeletion.lower() == "true" else sourceDataFrame
        
    #PICK THE LATEST IMAGE OF AN INCOMING RECORD
    partitionSpec = Window.partitionBy([col(x) for x in businessKeyList]).orderBy([col(x).desc() for x in sortOrderList])
    sourceDataFrame = sourceDataFrame.withColumn("rowNumber",row_number().over(partitionSpec)) \
                                     .where("rowNumber == 1")
    sourceDataFrame = sourceDataFrame.withColumn("_RecordCurrent",lit('1')).withColumn("_RecordDeleted",lit('0')).drop("rowNumber")
    return sourceDataFrame

# COMMAND ----------

def SapPostprocessCleansed(sourceDataFrame,businessKey,sourceRecordDeletion,sourceQuery,watermarkColumn=None):
    businessKeyList = list(businessKey.split(","))
    #PICK DELETED RECORDS FROM SOURCE
    if sourceQuery[0:3].lower() in ('crm','isu'):
        sortOrderList = ["extract_datetime","di_sequence_number"] if watermarkColumn else ["extract_datetime"]
        whereClause = "rowNumber == 1 AND (di_operation_type == 'X' OR di_operation_type == 'D')"
    else:
        sortOrderList = ["delta_ts"]
        whereClause = "rowNumber == 1 AND (is_deleted =='Y')"
#         partitionSpec = Window.partitionBy([col(x) for x in businessKeyList]).orderBy([col(x).desc() for x in sortOrderList])    
#         sourceDataFrame = sourceDataFrame.withColumn("rowNumber",row_number().over(partitionSpec)) \
#                                          .where("rowNumber == 1 AND (di_operation_type == 'X' OR di_operation_type == 'D')" )  
    partitionSpec = Window.partitionBy([col(x) for x in businessKeyList]).orderBy([col(x).desc() for x in sortOrderList])    
    sourceDataFrame = sourceDataFrame.withColumn("rowNumber",row_number().over(partitionSpec)) \
                                         .where(whereClause)    
    sourceDataFrame = sourceDataFrame.withColumn("_RecordCurrent",lit('0')).withColumn("_RecordDeleted",lit('1')).drop("rowNumber")
    return sourceDataFrame
