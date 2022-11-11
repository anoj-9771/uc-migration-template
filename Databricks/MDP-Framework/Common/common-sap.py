# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import *

# COMMAND ----------

def SapCleansedPreprocess(sourceDataFrame,businessKey,sourceRecordDeletion,watermarkColumn=None):
    businessKeyList = list(businessKey.split(","))
    sortOrderList = ["extract_datetime","di_sequence_number"] if watermarkColumn else ["extract_datetime"]
    #FILTER DELETED RECORDS FROM SOURCE
    if sourceRecordDeletion.lower() == "true":
        sourceDataFrame = sourceDataFrame.where("di_operation_type != 'X' AND di_operation_type != 'D'")
        
    #PICK THE LATEST IMAGE OF AN INCOMING RECORD
    partitionSpec = Window.partitionBy([col(x) for x in businessKeyList]).orderBy([col(x).desc() for x in sortOrderList])
    sourceDataFrame = sourceDataFrame.withColumn("rowNumber",row_number().over(partitionSpec)) \
                                     .where("rowNumber == 1")
    sourceDataFrame = sourceDataFrame.withColumn("_RecordCurrent",lit('1')).withColumn("_RecordDeleted",lit('0')).drop("rowNumber").drop("extract_datetime")
    return sourceDataFrame

# COMMAND ----------

def SapCleansedPostprocess(sourceDataFrame,businessKey,sourceRecordDeletion,watermarkColumn=None):
    businessKeyList = list(businessKey.split(","))
    sortOrderList = ["extract_datetime","di_sequence_number"] if watermarkColumn else ["extract_datetime"]
    #PICK DELETED RECORDS FROM SOURCE
    partitionSpec = Window.partitionBy([col(x) for x in businessKeyList]).orderBy([col(x).desc() for x in sortOrderList])
    sourceDataFrame = sourceDataFrame.withColumn("rowNumber",row_number().over(partitionSpec)) \
                                     .where("rowNumber == 1 AND (di_operation_type == 'X' OR di_operation_type == 'D')" )
    sourceDataFrame = sourceDataFrame.withColumn("_RecordCurrent",lit('0')).withColumn("_RecordDeleted",lit('1')).drop("rowNumber").drop("extract_datetime")
    return sourceDataFrame
