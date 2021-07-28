# Databricks notebook source
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.window import *
from pyspark.sql.types import *

# COMMAND ----------

def GetNAT020Avetmiss(dfAvetmissLocation, dfNAT120):
  ###########################################################################################################################
  # Function: GetNAT020Avetmiss
  # Parameters: 
  # dfAvetmissLocation = Avetmiss Location
  # dfNAT120 = NAT120 Avetmiss File
  # Returns:
  #  Dataframe for all NAT 020 AVETEMISS
  #############################################################################################################################  
  
  dfCampusList=dfNAT120.selectExpr("TRAINING_ORG_DEL_LOC_ID").distinct()
  dfLocV=dfAvetmissLocation.select(*(col(x).alias("Loc_"+ x) for x in dfAvetmissLocation.columns))
  
      
  df=dfCampusList.join(dfLocV,dfCampusList.TRAINING_ORG_DEL_LOC_ID==dfLocV.Loc_LocationCode,how="inner")

  df=df.withColumn("TRAINING_ORG_IDENTIFIER",lit('90003'))
  df=df.withColumn("STATE_ID",lit('01'))
  df=df.withColumn("COUNTRY_ID",lit('1101'))
  df=df.withColumn("Post_Code",rpad(col("Loc_Postcode"),4,"0"))
  df=df.withColumn("Output", \
                    concat(rpad(coalesce(col("TRAINING_ORG_IDENTIFIER"),lit('')),10,' '), \
                           rpad(coalesce(col("TRAINING_ORG_DEL_LOC_ID"),lit('')),10, ' '), \
                           rpad(coalesce(col("Loc_LocationDescription"),lit('')),100,' '), \
                           rpad(coalesce(col("Post_Code"),lit('')),4,' '), \
                           rpad(coalesce(col("STATE_ID"),lit('')),2,' '), \
                           rpad(coalesce(col("Loc_InstituteSuburb"),lit('')),50,' '), \
                           rpad(coalesce(col("COUNTRY_ID"),lit('')),4,' ')
                   ))
  df=df.selectExpr("TRAINING_ORG_IDENTIFIER"
                   ,"TRAINING_ORG_DEL_LOC_ID"
                   ,"Loc_LocationDescription as TRAINING_ORG_DEL_LOC_NAME"
                   ,"Post_Code"
                   ,"STATE_ID"
                   ,"Loc_InstituteSuburb as ADR_SUB_LOC_TOWN"
                   ,"COUNTRY_ID"
                   ,"Output"
                  ).dropDuplicates()
  return df
