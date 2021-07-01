# Databricks notebook source
def GetNAT010ERpt(dfAvetmissLocation, dfNAT120ERpt, rtoCode):
  ###########################################################################################################################
  # Function: GetNAT010ERpt
  # Parameters: 
  # dfAvetmissLocation = Avetmiss Location
  # dfNAT120ERpt = NAT E-Reporting 120
  # rtoCode = RTO Code - To Return Institute
  # Returns:
  #  Dataframe for all NAT 010 E-Reporting
  #############################################################################################################################
  df = dfAvetmissLocation.join(dfNAT120ERpt, dfAvetmissLocation.LocationCode == dfNAT120ERpt.SLOC_LOCATION_CODE, how="left").filter(col("SLOC_LOCATION_CODE").isNotNull())
  if rtoCode is None:
    df = df.filter(col("InstituteId")=='0165').withColumn("Training_Org_Id", rpad(lit('90003'),10,' '))
  else:
    df = df.withColumn("Training_Org_Id", rpad(coalesce(col("RTOCode"), lit(' ')),10,' '))
  #PRADA-1523
  df = df.withColumn("Training_Org_Name",rpad(expr("CASE WHEN TRIM(Training_Org_Id)='90003' THEN 'Technical and Further Education Commission' ELSE CONCAT('TAFE NSW ', COALESCE(InstituteDescription, ' ')) END"), 100,' ')) \
        .withColumn("EmptyColumn", rpad(lit(""), 158, ' '))
  df=df.limit(1)

  df = df.select("Training_Org_Id", "Training_Org_Name", "EmptyColumn")

  df = df.withColumn("Output", (concat(*df.columns))).dropDuplicates()
  
  return df

# COMMAND ----------


