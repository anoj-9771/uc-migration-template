# Databricks notebook source
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.window import *
from pyspark.sql.types import *
import pyspark.sql.functions as f

# COMMAND ----------

def GetNAT010Avetmiss(dfNAT120Avetmiss, dfAvetmissLocation):
  ###########################################################################################################################
  # Function: GetNAT010Avetmiss
  # Parameters: 
  # dfNAT120Avetmiss = NAT Avetmiss 120
  # dfAvetmissLocation = Avetmiss Location
  # Returns:
  #  Dataframe for all NAT 010 Avetmiss
  ############################################################################################################################# 
  df = dfAvetmissLocation.join(dfNAT120Avetmiss, dfAvetmissLocation.LocationCode == dfNAT120Avetmiss.TRAINING_ORG_DEL_LOC_ID, how="left").filter(col("TRAINING_ORG_DEL_LOC_ID").isNotNull())
  df=df.filter(col("InstituteId")=='0165')
  df = df.withColumn("Training_Org_Id", rpad(col("RTOCode"), 10, ' ')) \
        .withColumn("Training_Org_Name",rpad(lit("Technical and Further Education Commission"), 100,' ')) \
        .withColumn("Training_Org_Type_Id", rpad(lit("31"), 2,' ')) \
        .withColumn("AddressLine1", rpad(coalesce(col("InstituteAddressLine1"),lit('')), 50, ' ')) \
        .withColumn("AddressLine2", rpad(coalesce(col("InstituteAddressLine2"),lit('')), 50, ' ')) \
        .withColumn("Suburb", rpad(coalesce(col("InstituteSuburb"),lit('')), 50, ' ')) \
        .withColumn("PostCode", rpad(coalesce(col("InstitutePostcode"),lit('')), 4, ' ')) \
        .withColumn("State", rpad(lit("01"), 2, ' '))

  df = df.selectExpr("Training_Org_Id"
                     ,"Training_Org_Name"
                     ,"Training_Org_Type_Id"
                     ,"AddressLine1"
                     ,"AddressLine2"
                     ,"Suburb"
                     ,"PostCode"
                     ,"State"
                     
                    )
  
  df = df.withColumn("Output", (concat(*df.columns))).dropDuplicates()
  
  
  
  return df

# COMMAND ----------


