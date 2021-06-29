# Databricks notebook source
def GetNAT020ERpt(dfAvetmissLocation, rtoCode, dfNAT120):
  ###########################################################################################################################
  # Function: GetNAT020ERpt
  # Parameters: 
  # dfAvetmissLocation = Avetmiss Location
  # dfNAT120 = NAT120 E-Reporting File
  # rtoCode = RTO Code
  # Returns:
  #  Dataframe for all NAT 020 ERPT
  #############################################################################################################################  
  if rtoCode is None:
      rtoCode=90003
  
  dfCampusList=dfNAT120.selectExpr("SLOC_LOCATION_CODE").distinct()
  dfLocV=dfAvetmissLocation.select(*(col(x).alias("Loc_"+ x) for x in dfAvetmissLocation.columns))
  
  df=dfCampusList.join(dfLocV,dfCampusList.SLOC_LOCATION_CODE==dfLocV.Loc_LocationCode,how="inner")

  df=df.withColumn("TRAINING_ORG_IDENTIFIER",lit(rtoCode))
  df=df.withColumn("STATE_ID",lit('01'))
  df=df.withColumn("COUNTRY_ID",lit('1101'))
  df=df.withColumn("Post_Code",rpad(col("Loc_LocationPostCode"),4,"0"))
  df=df.withColumn("Output", \
                    concat(rpad(coalesce(col("TRAINING_ORG_IDENTIFIER"),lit(' ')),10,' ') , \
                           rpad(coalesce(col("SLOC_LOCATION_CODE"),lit(' ')),10, ' '), \
                           rpad(coalesce(col("Loc_LocationDescription"),lit(' ')),100,' '), \
                           rpad(coalesce(col("Post_Code"),lit(' ')),4,' '), \
                           rpad(coalesce(col("STATE_ID"),lit(' ')),2,' '), \
                           rpad(coalesce(col("Loc_LocationSuburb"),lit(' ')),50,' '), \
                           rpad(coalesce(col("COUNTRY_ID"),lit(' ')),4,' ')
                   ))
  df=df.selectExpr("TRAINING_ORG_IDENTIFIER"
                   ,"SLOC_LOCATION_CODE"
                   ,"Loc_LocationDescription as TRAINING_ORG_DEL_LOC_NAME"
                   ,"Post_Code"
                   ,"STATE_ID"
                   ,"Loc_InstituteSuburb as ADR_SUB_LOC_TOWN"
                   ,"COUNTRY_ID"
                   ,"Output"
                  ).dropDuplicates()
  df=df.sort("SLOC_LOCATION_CODE")
  
  return df

# COMMAND ----------

