# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import *

#Apply Window Functions
from pyspark.sql.window import Window


# COMMAND ----------

def GetAvetmissPrimaryLearner(dfPeople, dfAddresses, dfPeopleUnits, dfPeopleUsi, dfFeesList, dfUIO, dfUI, dfAttainments, startDate, endDate):
  ###########################################################################################################################
  # Function: GetAvetmissPrimaryLearner
  # Description: Load Primary Learner Dimension
  # Parameters: 
  # dfPeople = Input DataFrame - People
  # dfAddresses = Input DataFrame - AdddressesDATE_OF_BIRTH
  # dfPeopleUnits = Input DataFrame - People Units
  # dfPeopleUSI = Input DataFrame - People USI
  # dfFeesList = Input DataFrame - Fees Details
  # dfUIO = Input DataFrame - Unit Links Occurrences 
  # dfUI = Input DataFrame - Unit Instances (Coures Enrolments) 
  # 
  # Returns:
  #  Dataframe of transformed Course
  #############################################################################################################################  
  
  LogEtl("Starting the Primary Learner")

  #Get the ScoreCard values to be applied as weight on different measures
  ScoreCard = _GetScorecard()

  #dfPeople = dfPeople.where("PERSON_CODE = 100234 or PERSON_CODE = 100235")

  #Add Prefix to columns so it is easier to identify when there are similar column names
  dfPeopleUnits = dfPeopleUnits.select(*(col(x).alias("pu_"+ x) for x in dfPeopleUnits.columns))
  dfFeesList = dfFeesList.select(*(col(x).alias("fl_"+ x) for x in dfFeesList.columns))
  dfPeople = dfPeople.select(*(col(x).alias("p_"+ x) for x in dfPeople.columns))
  dfAddresses = dfAddresses.select(*(col(x).alias("a_"+ x) for x in dfAddresses.columns))
  dfPeopleUsi = dfPeopleUsi.select(*(col(x).alias("pusi_"+ x) for x in dfPeopleUsi.columns))
  dfUIO = dfUIO.select(*(col(x).alias("uio_"+ x) for x in dfUIO.columns))
  dfUI = dfUI.select(*(col(x).alias("ui_"+ x) for x in dfUI.columns))
  dfAttainments = dfAttainments.select(*(col(x).alias("at_"+ x) for x in dfAttainments.columns))
  
  LogEtl("Column renames completed")

  #Get the current datetime. This is needed to filter the Address table
  from datetime import datetime
  curr_date_time = str(datetime.now())

  #Apply Initial Filter conditions as per our requirements
  dfPeopleUnits = dfPeopleUnits.where("pu_UNIT_TYPE == 'R' AND pu_PROGRESS_CODE <> '3.1 WD'")
  dfUI = dfUI.where("ui_UI_LEVEL = 'COURSE' AND ui_UNIT_CATEGORY <> 'BOS'")
  dfAddresses = dfAddresses.where("a_ADDRESS_TYPE = 'RES' AND a_OWNER_TYPE = 'P' AND (a_END_DATE is null or a_END_DATE > '" + curr_date_time + "')")
  dfAddresses = dfAddresses.withColumn("Address_Rank", dense_rank().over(Window.partitionBy("a_PERSON_CODE").orderBy(desc("a_START_DATE"), desc("a_END_DATE"))))
  dfAddresses = dfAddresses.filter(col("Address_Rank") == 1)
  
  #The below lines converts the PersonCode to remove any decimal as the original field was double/float
  #First convert to Integer to remove decimal values and then convert to String
  dfPeople = dfPeople.withColumn("p_PERSON_CODE", dfPeople["p_PERSON_CODE"].cast(IntegerType()))
  dfPeople = dfPeople.withColumn("p_PERSON_CODE", dfPeople["p_PERSON_CODE"].cast(StringType()))

  #Get the base datafram for People. The dataframe contains all basic details related to Person
  df_People_Base = dfPeople \
    .join(dfPeopleUsi, dfPeople.p_PERSON_CODE == dfPeopleUsi.pusi_PERSON_CODE, how = "left") \
    .join(dfAddresses, dfPeople.p_PERSON_CODE == dfAddresses.a_PERSON_CODE, how = "left") \
    .select ("p_SURNAME", "p_DATE_OF_BIRTH", "p_PERSON_CODE"
            ,"p_REGISTRATION_NO", "p_ENTITLEMENT_ID", "p_PERSONAL_EMAIL", "p_MOBILE_PHONE_NUMBER", "p_UPDATED_DATE", "p_PREVIOUS_FORENAME", "p_FORENAME"
            ,"pusi_USI"
            ,"a_ADDRESS_LINE_1" , "a_END_DATE", "a_PERSON_CODE")

  #As emails are case insensitive, convert all emails to lower case for matching and preserve the original version for reporting
  #The Mobile phones can have spaces in between
  df_People_Base = df_People_Base \
    .withColumn("p_SURNAME_Original", col("p_SURNAME")) \
    .withColumn("pusi_USI_Original", col("pusi_USI")) \
    .withColumn("p_PERSONAL_EMAIL_Original", col("p_PERSONAL_EMAIL")) \
    .withColumn("p_MOBILE_PHONE_NUMBER_Original", col("p_MOBILE_PHONE_NUMBER"))
  
  df_People_Base = df_People_Base \
    .withColumn("p_PERSONAL_EMAIL", trim(lower(col("p_PERSONAL_EMAIL")))) \
    .withColumn("p_MOBILE_PHONE_NUMBER", trim(regexp_replace(col("p_MOBILE_PHONE_NUMBER"), " ", ""))) \
    .withColumn("p_SURNAME", trim(lower(regexp_replace(col("p_SURNAME"), " ", "")))) \
    .withColumn("pusi_USI", trim(regexp_replace(col("pusi_USI"), " ", ""))) \
    .withColumn("p_REGISTRATION_NO", trim(col("p_REGISTRATION_NO"))) \
    .withColumn("p_ENTITLEMENT_ID", trim(col("p_ENTITLEMENT_ID")))
  
  
  #Apply all Aggregations for People
  df_People_Main = _GetPeopleAggregations(df_People_Base, dfPeople, dfPeopleUnits, dfUIO, dfUI, dfFeesList, dfAttainments, ScoreCard)

  #Filter out the records where there is no matching group or other exceptions e.g. Surname = DO NOT USE
  df_People_Main = df_People_Main.where("Base_Count > 1 and (USI_Count > 1 OR Mobile_Count > 1 OR Email_Count> 1 OR Entitlement_Count > 1)  \
    AND (UPPER(p_SURNAME) NOT IN ('_', '.', 'NO NAME', '-', '(DO NOT USE)') AND UPPER(p_SURNAME) NOT LIKE '%DUPLICATE%')")
  
  #Apply Scoring on Aggregations Columns 
  df_People_Score = _ApplyScoring(df_People_Main, ScoreCard)
  
  df_People_Score_MatchGroup = _ApplyMatching(df_People_Score)
  
  #Apply Window Function to get the MatchGroupID
  #df_People_Score_MatchGroup = df_People_Score.withColumn("MatchGroupID", dense_rank().over(Window.partitionBy().orderBy("p_SURNAME", "p_DATE_OF_BIRTH", "Base_Count")))
  #Apply Window Function to get the Score Rank. Give priority to highest total score. And if there is a tie, take the latest Update from People
  #df_People_Score = df_People_Score.withColumn("Score_Rank", dense_rank().over(Window.partitionBy("MatchGroupID").orderBy(desc("TotalScore"), desc("p_UPDATED_DATE"))))
  df_People_Score_MatchGroup = df_People_Score_MatchGroup.withColumn("Score_Rank", dense_rank().over(Window.partitionBy("MatchGroupID").orderBy(desc("TotalScore"), desc("p_UPDATED_DATE"), desc("p_PERSON_CODE"))))

  #Filter for the highest score only and pick up the Primary Learner Code and Registration Number for Rank 1 in each group
  df_Primary_Learner = df_People_Score_MatchGroup.where("Score_Rank = 1").select(
      col("MatchGroupID").alias("Primary_MatchGroupID")
    , col("p_PERSON_CODE").alias("PrimaryPersonCode")
    , col("p_REGISTRATION_NO").alias("PrimaryRegistrationNo"))
  
  #Join with the Primary Learner table to find the Primary Learner Code for each Match Group
#  df_People_Final = df_People_Score.join(df_Primary_Learner, (df_People_Score.MatchGroupID == df_Primary_Learner.Primary_MatchGroupID) & (df_People_Score.p_PERSON_CODE == df_Primary_Learner.PrimaryPersonCode), how = "left") 
  df_People_Final = df_People_Score_MatchGroup.join(df_Primary_Learner, (df_People_Score_MatchGroup.MatchGroupID == df_Primary_Learner.Primary_MatchGroupID), how = "left") 

  #Apply the final logic
  df_People_Final = df_People_Final \
            .withColumn("PrimaryLearner", expr("COALESCE(PrimaryRegistrationNo, PrimaryPersonCode)")) \
            .withColumn("PrimaryRecordFlag", expr("case when PrimaryPersonCode = p_PERSON_CODE then 'Yes' else 'No' END")) \
            .withColumn("RegistrationNoValue", col("p_REGISTRATION_NO")) \
            .withColumn("FirstName", col("p_FORENAME")) \

  #Rename the Columns
  df_People_Final = df_People_Final \
            .withColumnRenamed("p_SURNAME_Original", "Surname") \
            .withColumnRenamed("p_DATE_OF_BIRTH", "DateOfBirth") \
            .withColumnRenamed("p_PERSON_CODE", "PersonCode") \
            .withColumnRenamed("p_REGISTRATION_NO", "RegistrationNo") \
            .withColumnRenamed("pusi_USI_Original", "USIValue") \
            .withColumnRenamed("p_MOBILE_PHONE_NUMBER_Original", "MobilePhoneValue") \
            .withColumnRenamed("p_ENTITLEMENT_ID", "EntitlementValue") \
            .withColumnRenamed("p_PERSONAL_EMAIL_Original", "PersonalEmailValue") \
            .withColumnRenamed("p_PREVIOUS_FORENAME", "PreviousForenameValue") \
            .withColumnRenamed("a_ADDRESS_LINE_1", "AddressLine1Value") \

  #Select the columns required for data load
  df_Final = df_People_Final.select("MatchGroupID"
                                  , "Surname"
                                  , "FirstName"
                                  , "DateOfBirth"
                                  , "PersonCode"
                                  , "RegistrationNo"
                                  , "PrimaryPersonCode"
                                  , "PrimaryRegistrationNo"
                                  , "PrimaryLearner"
                                  , "PrimaryRecordFlag"
                                  , "USIValue"
                                  , "USIScore"
                                  , "MobilePhoneValue"
                                  , "MobilePhoneScore"
                                  , "EntitlementValue"
                                  , "EntitlementScore"
                                  , "PersonalEmailValue"
                                  , "PersonalEmailScore"
                                  , "RegistrationNoValue"
                                  , "MisingRegistrationNoScore"
                                  , "EnrolmentCount"
                                  , "EnrolmentScore"
                                  , "FeeRecordCountByEnrolment"
                                  , "FeeRecordCountByScore"
                                  , "ProgressCourseCount"
                                  , "ProgressCourseScore"
                                  , "PreviousForenameValue"
                                  , "PreviousForenameScore"
                                  , "AddressLine1Value"
                                  , "DoNotMigrateScore"
                                  , "CompletedThisYearCount"
                                  , "CompletedThisYearScore"
                                  , "TotalScore"
                                 )
  
  LogEtl("Finishing the Primary Learner")

  return df_Final


# COMMAND ----------

def _GetScorecard():
  
  #Get the Scorecard values. Currently the values are fixed here.
  #Going forward, this should come from a CSV file to make it more customisable
  ScoreCard = {
    "w_PREVIOUS_FORENAME_MIGRATE" : 9999,
    "w_PREVIOUS_FORENAME_DO_NOTMIGRATE_ADDRESS_LINE_1_DO_NOT" : -9999,
    "w_USI" : 15,
    "w_MOBILE_PHONE_NUMBER" : 1,
    "w_ENTITLEMENT_ID" : 1,
    "w_PERSONAL_EMAIL" : 1,
    "w_AWARD_CODE_NON_WITHDRAWN_ENROLMENT" : 1,
    "w_AWARD_CODE_FEE_EXISTS" : 1,
    "w_AWARD_CODE_ACTIVE_ENROLMENT" : 20,
    "w_MISSING_REGISTRATION" : -20000,
    "w_ENROLMENT" : 3,
    "w_FEES" : 1,
    "w_PROGRESS" : 100,
    "w_COMPLETED_COURSE_THIS_YEAR" : 90
  }
  
  return ScoreCard

# COMMAND ----------

def _GetEnrolmentAggregation(dfPeople, dfPeopleUnits, dfUIO, dfUI, EnrolmentScore):

  LogEtl("Applying Enrolment Aggregation")

  #Get the dataframe for Award related details. The main table here is Unit Instance Occurence
  dfUIO = dfPeople \
    .join(dfPeopleUnits, dfPeople.p_PERSON_CODE == dfPeopleUnits.pu_PERSON_CODE, how = "inner") \
    .join(dfUIO, dfPeopleUnits.pu_UIO_ID == dfUIO.uio_UIO_ID, how = "inner") \
    .join(dfUI, dfUIO.uio_FES_UINS_INSTANCE_CODE == dfUI.ui_FES_UNIT_INSTANCE_CODE, how = "inner") \
    .select ("p_SURNAME", "p_DATE_OF_BIRTH", "p_PERSON_CODE"
            ,"pu_UIO_ID", "pu_ID", "pu_PERSON_CODE", "pu_PROGRESS_CODE"
            ,"uio_UIO_ID", "uio_FES_UINS_INSTANCE_CODE", "uio_AWARD_CODE"
            ,"ui_FES_UNIT_INSTANCE_CODE")

  
  #CREATE AGGREGATIONS

  #Get the Total Enrolment Count 
  dfUIOAgg = dfUIO \
    .groupBy("p_PERSON_CODE") \
    .count() \
    .withColumnRenamed("count", "EnrolmentCount") \
    .withColumnRenamed("p_PERSON_CODE", "UIO_AGG_PERSON_CODE") \

  #Apply weights to the enrolment related counts
  dfUIOAgg = dfUIOAgg.withColumn("EnrolmentScore", col("EnrolmentCount") * EnrolmentScore)
  
  return dfUIOAgg


# COMMAND ----------

def _GetFeesAggregation(dfPeople, dfPeopleUnits, dfFeesList, dfUIO, FeeScore):
  LogEtl("Applying Fees Aggregation")

  #Get the dataframe for fees
  dfFees = dfPeople \
    .join(dfPeopleUnits, dfPeople.p_PERSON_CODE == dfPeopleUnits.pu_PERSON_CODE, how = "inner") \
    .join(dfFeesList, (dfPeopleUnits.pu_PERSON_CODE == dfFeesList.fl_FES_PERSON_CODE) & (dfPeopleUnits.pu_ID == dfFeesList.fl_RUL_CODE), how = "inner") \
    .join(dfUIO, dfPeopleUnits.pu_UIO_ID == dfUIO.uio_UIO_ID, how = "inner") \
    .select ("p_SURNAME", "p_DATE_OF_BIRTH", "p_PERSON_CODE"
            ,"pu_UIO_ID", "pu_ID", "pu_PERSON_CODE", "pu_PROGRESS_CODE"
            ,"fl_RUL_CODE", "fl_FES_PERSON_CODE"
            ,"uio_UIO_ID", "uio_AWARD_CODE")

  #CREATE AGGREGATIONS

  #Get the Total Number of Enrolments where we have the Fee Records
  dfFeesAgg = dfFees \
    .groupBy("p_PERSON_CODE") \
    .agg(countDistinct("uio_AWARD_CODE")) \
    .withColumnRenamed("count(uio_AWARD_CODE)", "FeeRecordCountByEnrolment") \
    .withColumnRenamed("p_PERSON_CODE", "FEES_AGG_PERSON_CODE") \

  #Apply weights to the enrolment related counts
  dfFeesAgg = dfFeesAgg.withColumn("FeeRecordCountByScore", col("FeeRecordCountByEnrolment") * FeeScore)
  
  return dfFeesAgg


# COMMAND ----------

def _GetProgressAggregation(dfPeople, dfPeopleUnits, dfUIO, dfUI, ProgressScore):
  
  LogEtl("Applying Progress Aggregation")
  
  from datetime import datetime
  curr_year = str(datetime.now().year)
  #dfPeopleUnits = dfPeopleUnits.where ("YEAR(pu_PROGRESS_DATE) = {}".format(year))
  
  #Get the dataframe for Award related details. The main table here is Unit Instance Occurence
  dfUIO = dfPeople \
    .join(dfPeopleUnits, dfPeople.p_PERSON_CODE == dfPeopleUnits.pu_PERSON_CODE, how = "inner") \
    .join(dfUIO, dfPeopleUnits.pu_UIO_ID == dfUIO.uio_UIO_ID, how = "inner") \
    .join(dfUI, dfUIO.uio_FES_UINS_INSTANCE_CODE == dfUI.ui_FES_UNIT_INSTANCE_CODE, how = "inner") \
    .select ("p_SURNAME", "p_DATE_OF_BIRTH", "p_PERSON_CODE"
            ,"pu_UIO_ID", "pu_ID", "pu_PERSON_CODE", "pu_PROGRESS_CODE"
            ,"uio_UIO_ID", "uio_FES_UINS_INSTANCE_CODE", "uio_AWARD_CODE"
            ,"ui_FES_UNIT_INSTANCE_CODE")

  #CREATE AGGREGATIONS

  #Get total Active / In Progress Enrolments
  dfProgressAgg = dfUIO \
    .where("pu_PROGRESS_CODE == '1.1 ACTIVE' AND YEAR(pu_PROGRESS_DATE) = " + curr_year) \
    .groupBy("p_PERSON_CODE") \
    .count() \
    .withColumnRenamed("count", "ProgressCourseCount") \
    .withColumnRenamed("p_PERSON_CODE", "PROGRESS_AGG_PERSON_CODE") \

  #Apply weights to the enrolment related counts
  dfProgressAgg = dfProgressAgg.withColumn("ProgressCourseScore", col("ProgressCourseCount") * ProgressScore)
  
  return dfProgressAgg

# COMMAND ----------

def _GetCompletedCourseAggregation(dfPeopleUnits, dfUIO, dfUI, dfAttainments, CompletedCourseScore):
  
  LogEtl("Applying Completed Course Aggregation")
  
  from datetime import datetime
  curr_year = str(datetime.now().year)
  
  #Get the dataframe for Completed Course details. 
  dfCompletedCourse = dfPeopleUnits \
    .join(dfUIO, dfPeopleUnits.pu_UIO_ID == dfUIO.uio_UIO_ID, how = "inner") \
    .join(dfUI, dfUIO.uio_FES_UINS_INSTANCE_CODE == dfUI.ui_FES_UNIT_INSTANCE_CODE, how = "inner") \
    .join(dfAttainments, dfPeopleUnits.pu_ID == dfAttainments.at_PEOPLE_UNITS_ID , how = "inner") \
    .select ("pu_UIO_ID", "pu_ID", "pu_PERSON_CODE", "pu_PROGRESS_CODE"
            ,"uio_UIO_ID", "uio_FES_UINS_INSTANCE_CODE", "uio_AWARD_CODE"
            ,"ui_FES_UNIT_INSTANCE_CODE"
            ,"at_DATE_AWARDED")

  #CREATE AGGREGATIONS

  #Get total Active / In Progress Enrolments
  dfCompletedCourseAgg = dfCompletedCourse \
    .where("pu_PROGRESS_CODE == '4.1 COMPL' AND YEAR(at_DATE_AWARDED) >= 2019 ") \
    .groupBy("pu_PERSON_CODE") \
    .agg(countDistinct("uio_AWARD_CODE")) \
    .withColumnRenamed("count(uio_AWARD_CODE)", "CompletedThisYearCount") \
    .withColumnRenamed("pu_PERSON_CODE", "COMPLETEDCOURSE_AGG_PERSON_CODE") \

  #Apply weights to the enrolment related counts
  dfCompletedCourseAgg = dfCompletedCourseAgg.withColumn("CompletedThisYearScore", col("CompletedThisYearCount") * CompletedCourseScore)
  
  return dfCompletedCourseAgg

# COMMAND ----------

def _GetPeopleAggregations(df_People_Base, dfPeople, dfPeopleUnits, dfUIO, dfUI, dfFeesList, dfAttainments, ScoreCard):
  
  LogEtl("Applying People Aggregation")
  #Get Aggregations for People related measurse

  #Get Aggregations for Enrolments
  df_P_UIO_AGG = _GetEnrolmentAggregation(dfPeople, dfPeopleUnits, dfUIO, dfUI, ScoreCard["w_ENROLMENT"])

  #Get Aggregations for Fees
  df_P_FEES_AGG = _GetFeesAggregation(dfPeople, dfPeopleUnits, dfFeesList, dfUIO, ScoreCard["w_FEES"])

  #Get Aggregations for Progress
  df_P_PROGRESS_AGG = _GetProgressAggregation(dfPeople, dfPeopleUnits, dfUIO, dfUI, ScoreCard["w_PROGRESS"])

  #Get Aggregations for Progress
  df_P_COMPLETED_COURSE_AGG = _GetCompletedCourseAggregation(dfPeopleUnits, dfUIO, dfUI, dfAttainments, ScoreCard["w_COMPLETED_COURSE_THIS_YEAR"])
  LogEtl ("Completed Aggregations")

  #Get Total number of records for a combination of Surname and DOB
  df_Base_Count = df_People_Base \
    .groupBy("p_SURNAME", "p_DATE_OF_BIRTH") \
    .count() \
    .filter(col("count") > 1) \
    .withColumnRenamed("p_SURNAME", "Base_SURNAME") \
    .withColumnRenamed("p_DATE_OF_BIRTH", "Base_DATE_OF_BIRTH") \
    .withColumnRenamed("count", "Base_Count") 

  #Get total USI for each Surname and DOB combination
  df_USI_Count = df_People_Base \
    .where(df_People_Base.pusi_USI.isNotNull()) \
    .groupBy("p_SURNAME", "p_DATE_OF_BIRTH", "pusi_USI") \
    .count() \
    .filter(col("count") > 1) \
    .withColumnRenamed("p_SURNAME", "USI_SURNAME") \
    .withColumnRenamed("p_DATE_OF_BIRTH", "USI_DATE_OF_BIRTH") \
    .withColumnRenamed("pusi_USI", "USI_USI") \
    .withColumnRenamed("count", "USI_Count") \

  #Get Total Number of Mobile records for each Surname and DOB combination
  df_Mobile_Count = df_People_Base \
    .where(df_People_Base.p_MOBILE_PHONE_NUMBER.isNotNull()) \
    .groupBy("p_SURNAME", "p_DATE_OF_BIRTH", "p_MOBILE_PHONE_NUMBER") \
    .count() \
    .filter(col("count") > 1) \
    .withColumnRenamed("p_SURNAME", "Mobile_SURNAME") \
    .withColumnRenamed("p_DATE_OF_BIRTH", "Mobile_DATE_OF_BIRTH") \
    .withColumnRenamed("p_MOBILE_PHONE_NUMBER", "Mobile_MOBILE_PHONE_NUMBER") \
    .withColumnRenamed("count", "Mobile_Count") \

  #Get Total Number of Email records for each Surname and DOB combination
  df_Email_Count = df_People_Base \
    .where(df_People_Base.p_PERSONAL_EMAIL.isNotNull()) \
    .groupBy("p_SURNAME", "p_DATE_OF_BIRTH", "p_PERSONAL_EMAIL") \
    .count() \
    .filter(col("count") > 1) \
    .withColumnRenamed("p_SURNAME", "Email_SURNAME") \
    .withColumnRenamed("p_DATE_OF_BIRTH", "Email_DATE_OF_BIRTH") \
    .withColumnRenamed("p_PERSONAL_EMAIL", "Email_PERSONAL_EMAIL") \
    .withColumnRenamed("count", "Email_Count") \

  #Get Total Number of Entitlement records for each Surname and DOB combination
  df_Entitlement_Count = df_People_Base \
    .where(df_People_Base.p_ENTITLEMENT_ID.isNotNull()) \
    .groupBy("p_SURNAME", "p_DATE_OF_BIRTH", "p_ENTITLEMENT_ID") \
    .count() \
    .filter(col("count") > 1) \
    .withColumnRenamed("p_SURNAME", "Entitlement_SURNAME") \
    .withColumnRenamed("p_DATE_OF_BIRTH", "Entitlement_DATE_OF_BIRTH") \
    .withColumnRenamed("p_ENTITLEMENT_ID", "Entitlement_ENTITLEMENT_ID") \
    .withColumnRenamed("count", "Entitlement_Count") \
  
  #Join all the aggregations to the base table of Suraname and DOB
  df_People_Main = df_People_Base \
    .join(df_Base_Count, ((df_People_Base.p_SURNAME == df_Base_Count.Base_SURNAME) & (df_People_Base.p_DATE_OF_BIRTH == df_Base_Count.Base_DATE_OF_BIRTH)), how = "left") \
    .drop(df_Base_Count.Base_SURNAME) \
    .drop(df_Base_Count.Base_DATE_OF_BIRTH) \
    .join(df_USI_Count, ((df_People_Base.p_SURNAME == df_USI_Count.USI_SURNAME) & (df_People_Base.p_DATE_OF_BIRTH == df_USI_Count.USI_DATE_OF_BIRTH) & (df_People_Base.pusi_USI == df_USI_Count.USI_USI)), how = "left") \
    .drop(df_USI_Count.USI_SURNAME) \
    .drop(df_USI_Count.USI_DATE_OF_BIRTH) \
    .drop(df_USI_Count.USI_USI) \
    .join(df_Mobile_Count, ((df_People_Base.p_SURNAME == df_Mobile_Count.Mobile_SURNAME) \
                            & (df_People_Base.p_DATE_OF_BIRTH == df_Mobile_Count.Mobile_DATE_OF_BIRTH) \
                            & (df_People_Base.p_MOBILE_PHONE_NUMBER == df_Mobile_Count.Mobile_MOBILE_PHONE_NUMBER)), how = "left") \
    .drop(df_Mobile_Count.Mobile_SURNAME) \
    .drop(df_Mobile_Count.Mobile_DATE_OF_BIRTH) \
    .drop(df_Mobile_Count.Mobile_MOBILE_PHONE_NUMBER) \
    .join(df_Email_Count, ((df_People_Base.p_SURNAME == df_Email_Count.Email_SURNAME) \
                           & (df_People_Base.p_DATE_OF_BIRTH == df_Email_Count.Email_DATE_OF_BIRTH) \
                           & (df_People_Base.p_PERSONAL_EMAIL == df_Email_Count.Email_PERSONAL_EMAIL)), how = "left") \
    .drop(df_Email_Count.Email_SURNAME) \
    .drop(df_Email_Count.Email_DATE_OF_BIRTH) \
    .drop(df_Email_Count.Email_PERSONAL_EMAIL) \
    .join(df_Entitlement_Count, ((df_People_Base.p_SURNAME == df_Entitlement_Count.Entitlement_SURNAME) \
                           & (df_People_Base.p_DATE_OF_BIRTH == df_Entitlement_Count.Entitlement_DATE_OF_BIRTH) \
                           & (df_People_Base.p_ENTITLEMENT_ID == df_Entitlement_Count.Entitlement_ENTITLEMENT_ID)), how = "left") \
    .drop(df_Entitlement_Count.Entitlement_SURNAME) \
    .drop(df_Entitlement_Count.Entitlement_DATE_OF_BIRTH) \
    .drop(df_Entitlement_Count.Entitlement_ENTITLEMENT_ID) \
    .join(df_P_UIO_AGG, df_People_Base.p_PERSON_CODE == df_P_UIO_AGG.UIO_AGG_PERSON_CODE, how = "left") \
    .join(df_P_FEES_AGG, df_People_Base.p_PERSON_CODE == df_P_FEES_AGG.FEES_AGG_PERSON_CODE, how = "left") \
    .join(df_P_PROGRESS_AGG, df_People_Base.p_PERSON_CODE == df_P_PROGRESS_AGG.PROGRESS_AGG_PERSON_CODE, how = "left") \
    .join(df_P_COMPLETED_COURSE_AGG, df_People_Base.p_PERSON_CODE == df_P_COMPLETED_COURSE_AGG.COMPLETEDCOURSE_AGG_PERSON_CODE, how = "left") \
    .orderBy(df_People_Base.p_SURNAME, df_People_Base.p_DATE_OF_BIRTH)

  df_People_Main = df_People_Main \
    .withColumn("MatchGroupBase", dense_rank().over(Window.partitionBy().orderBy("p_SURNAME", "p_DATE_OF_BIRTH"))) \
    .withColumn("MatchGroupBase", concat(lit("B-"), col("MatchGroupBase")))
  
  return df_People_Main
  

# COMMAND ----------

def _ApplyScoring(df, ScoreCard):
  
  LogEtl("Applying Scorings to Dataframe")

  #Apply Scoring to all the measures
  df_People_Score = df \
    .withColumn("PreviousForenameScore", when(df.p_PREVIOUS_FORENAME == "MIGRATE", ScoreCard["w_PREVIOUS_FORENAME_MIGRATE"]) \
        .otherwise(0)) \
    .withColumn("DoNotMigrateScore", expr ("CASE WHEN a_ADDRESS_LINE_1 LIKE '%DO NOT%' or p_PREVIOUS_FORENAME = 'DO NOT MIGRATE' then " + str(ScoreCard["w_PREVIOUS_FORENAME_DO_NOTMIGRATE_ADDRESS_LINE_1_DO_NOT"]) + " ELSE 0 END")) \
    .withColumn("USIScore", expr ("CASE WHEN pusi_USI is not null or pusi_USI = '' then " + str(ScoreCard["w_USI"]) + " ELSE 0 END")) \
    .withColumn("MobilePhoneScore", expr ("CASE WHEN p_MOBILE_PHONE_NUMBER is not null or p_MOBILE_PHONE_NUMBER = '' then " + str(ScoreCard["w_MOBILE_PHONE_NUMBER"]) + " ELSE 0 END")) \
    .withColumn("EntitlementScore", expr ("CASE WHEN p_ENTITLEMENT_ID is not null or p_ENTITLEMENT_ID = '' then " + str(ScoreCard["w_ENTITLEMENT_ID"]) + " ELSE 0 END")) \
    .withColumn("PersonalEmailScore", expr ("CASE WHEN p_PERSONAL_EMAIL is not null or p_PERSONAL_EMAIL = '' then " + str(ScoreCard["w_PERSONAL_EMAIL"]) + " ELSE 0 END")) \
    .withColumn("MisingRegistrationNoScore", expr ("CASE WHEN p_REGISTRATION_NO is null OR p_REGISTRATION_NO= '' then " + str(ScoreCard["w_MISSING_REGISTRATION"]) + " ELSE 0 END")) \
    .withColumn("EnrolmentCount", expr ("CASE WHEN EnrolmentCount is null then 0 ELSE EnrolmentCount END")) \
    .withColumn("EnrolmentScore", expr ("CASE WHEN EnrolmentScore is null then 0 ELSE EnrolmentScore END")) \
    .withColumn("FeeRecordCountByEnrolment", expr ("CASE WHEN FeeRecordCountByEnrolment is null then 0 ELSE FeeRecordCountByEnrolment END")) \
    .withColumn("FeeRecordCountByScore", expr ("CASE WHEN FeeRecordCountByScore is null then 0 ELSE FeeRecordCountByScore END")) \
    .withColumn("ProgressCourseCount", expr ("CASE WHEN ProgressCourseCount is null then 0 ELSE ProgressCourseCount END")) \
    .withColumn("ProgressCourseScore", expr ("CASE WHEN ProgressCourseScore is null then 0 ELSE ProgressCourseScore END")) \
    .withColumn("CompletedThisYearCount", expr ("CASE WHEN CompletedThisYearCount is null then 0 ELSE CompletedThisYearCount END")) \
    .withColumn("CompletedThisYearScore", expr ("CASE WHEN CompletedThisYearScore is null then 0 ELSE CompletedThisYearScore END")) \

  #Replace Nulls with 0. Nulls can generate because of LEFT JOIN
  df_People_Score.fillna( { 'EnrolmentCount':0, 'FeeRecordCountByEnrolment':0, 'ProgressCourseCount':0 } )

  #Calculate Total Score based on all the scores
  df_People_Score = df_People_Score.withColumn ("TotalScore", 
                                     df_People_Score.PreviousForenameScore \
                                   + df_People_Score.DoNotMigrateScore \
                                   + df_People_Score.USIScore \
                                   + df_People_Score.MobilePhoneScore \
                                   + df_People_Score.EntitlementScore \
                                   + df_People_Score.PersonalEmailScore \
                                   + df_People_Score.MisingRegistrationNoScore \
                                   + df_People_Score.EnrolmentScore \
                                   + df_People_Score.FeeRecordCountByScore \
                                   + df_People_Score.ProgressCourseScore \
                                   + df_People_Score.CompletedThisYearScore \
                                 )
  
  
  return df_People_Score

# COMMAND ----------

def _ApplyMatching(df_People_Score):
  
  LogEtl("Applying Matching Groups to Dataframe")
  
  #Step 1
  #Find all matching groups where either Email, Mobile, USI or Entitlement has a match 
  
  #Email match
  df_Email = df_People_Score \
    .where("Email_Count > 0") \
    .withColumn("MatchGroupEmail", dense_rank().over(Window.partitionBy().orderBy("p_SURNAME", "p_DATE_OF_BIRTH", "p_PERSONAL_EMAIL"))) \
    .select("p_PERSON_CODE", "p_SURNAME", "p_DATE_OF_BIRTH", "MatchGroupEmail") \
    .withColumn("MatchGroupEmail", concat(lit("E-"), col("MatchGroupEmail"))) \
    .withColumn("MatchGroupEmailFlag", lit(1))

  #Mobile match
  df_Mobile = df_People_Score \
    .where("Mobile_Count > 0") \
    .withColumn("MatchGroupMobile", dense_rank().over(Window.partitionBy().orderBy("p_SURNAME", "p_DATE_OF_BIRTH", "p_MOBILE_PHONE_NUMBER"))) \
    .select("p_PERSON_CODE", "p_SURNAME", "p_DATE_OF_BIRTH", "MatchGroupMobile") \
    .withColumn("MatchGroupMobile", concat(lit("M-"), col("MatchGroupMobile"))) \
    .withColumn("MatchGroupMobileFlag", lit(1))

  #USI Match
  df_USI = df_People_Score \
    .where("USI_Count > 0") \
    .withColumn("MatchGroupUSI", dense_rank().over(Window.partitionBy().orderBy("p_SURNAME", "p_DATE_OF_BIRTH", "pusi_USI"))) \
    .select("p_PERSON_CODE", "p_SURNAME", "p_DATE_OF_BIRTH", "MatchGroupUSI") \
    .withColumn("MatchGroupUSI", concat(lit("U-"), col("MatchGroupUSI"))) \
    .withColumn("MatchGroupUSIFlag", lit(1))

  #Entitilement Match
  df_Entitlement = df_People_Score \
    .where("Entitlement_Count > 0") \
    .withColumn("MatchGroupEntitlement", dense_rank().over(Window.partitionBy().orderBy("p_SURNAME", "p_DATE_OF_BIRTH", "p_ENTITLEMENT_ID"))) \
    .select("p_PERSON_CODE", "p_SURNAME", "p_DATE_OF_BIRTH", "MatchGroupEntitlement") \
    .withColumn("MatchGroupEntitlement", concat(lit("U-"), col("MatchGroupEntitlement"))) \
    .withColumn("MatchGroupEntitlementFlag", lit(1))
  
  #Join all of them together into a single dataframe
  df_People_Score = df_People_Score \
    .join(df_Email, ["p_PERSON_CODE", "p_SURNAME", "p_DATE_OF_BIRTH"], how = "left") \
    .join(df_Mobile, ["p_PERSON_CODE", "p_SURNAME", "p_DATE_OF_BIRTH"], how = "left") \
    .join(df_USI, ["p_PERSON_CODE", "p_SURNAME", "p_DATE_OF_BIRTH"], how = "left") \
    .join(df_Entitlement, ["p_PERSON_CODE", "p_SURNAME", "p_DATE_OF_BIRTH"], how = "left")
  
  
  #Replace NULL with 0. Thus we have 1 where there are matching records and 0 where none by each group
  df_People_Score = df_People_Score \
    .withColumn("MatchGroupEmailFlag", coalesce(col("MatchGroupEmailFlag"), lit(0))) \
    .withColumn("MatchGroupMobileFlag", coalesce(col("MatchGroupMobileFlag"), lit(0))) \
    .withColumn("MatchGroupUSIFlag", coalesce(col("MatchGroupUSIFlag"), lit(0))) \
    .withColumn("MatchGroupEntitlementFlag", coalesce(col("MatchGroupEntitlementFlag"), lit(0))) 
  
  #Sum all the match group together to get a match weight
  df_People_Score = df_People_Score \
    .withColumn("MatchFlagTotal", col("MatchGroupEmailFlag") + col("MatchGroupMobileFlag") + col("MatchGroupUSIFlag") + col("MatchGroupEntitlementFlag"))

  #Apply the wegith to each of the match groups
  df_People_Score = df_People_Score \
    .withColumn("MatchGroupEmailScore", col("MatchFlagTotal") * col("MatchGroupEmailFlag")) \
    .withColumn("MatchGroupMobileScore", col("MatchFlagTotal") * col("MatchGroupMobileFlag")) \
    .withColumn("MatchGroupUSIScore", col("MatchFlagTotal") * col("MatchGroupUSIFlag")) \
    .withColumn("MatchGroupEntitlementScore", col("MatchFlagTotal") * col("MatchGroupEntitlementFlag")) \
  
  #Step 2
  #Use the weight to get the weighting score for each group.
  #If the Total Match records in group is same as total weight score than there are no cross group match
  #Else there could be cross group matches. e.g. 2 rows match over email whereas 1 from this group and 1 from other group match over phone. They will all be clubbed togeher
  
  #Email weighting
  df_People_Score_Email = df_People_Score \
    .groupBy("MatchGroupEmail") \
    .agg(sum("MatchGroupEmailScore").alias("MatchGroupEmailScoreTotal"), count("MatchGroupEmail").alias("MatchGroupEmailCount")) \
    .select("MatchGroupEmail", "MatchGroupEmailScoreTotal", "MatchGroupEmailCount")
  
  #Mobile weighting
  df_People_Score_Mobile = df_People_Score \
    .groupBy("MatchGroupMobile") \
    .agg(sum("MatchGroupMobileScore").alias("MatchGroupMobileScoreTotal"), count("MatchGroupMobile").alias("MatchGroupMobileCount")) \
    .select("MatchGroupMobile", "MatchGroupMobileScoreTotal", "MatchGroupMobileCount")
  
  #USI weighting
  df_People_Score_USI = df_People_Score \
    .groupBy("MatchGroupUSI") \
    .agg(sum("MatchGroupUSIScore").alias("MatchGroupUSIScoreTotal"), count("MatchGroupUSI").alias("MatchGroupUSICount")) \
    .select("MatchGroupUSI", "MatchGroupUSIScoreTotal", "MatchGroupUSICount")
  
  #Entitlement weighting
  df_People_Score_Entitlement = df_People_Score \
    .groupBy("MatchGroupEntitlement") \
    .agg(sum("MatchGroupEntitlementScore").alias("MatchGroupEntitlementScoreTotal"), count("MatchGroupEntitlement").alias("MatchGroupEntitlementCount")) \
    .select("MatchGroupEntitlement", "MatchGroupEntitlementScoreTotal", "MatchGroupEntitlementCount")
  
  #Join all weighting together
  df_People_Score = df_People_Score \
    .join(df_People_Score_Email, ["MatchGroupEmail"], how = "left") \
    .join(df_People_Score_Mobile, ["MatchGroupMobile"], how = "left") \
    .join(df_People_Score_USI, ["MatchGroupUSI"], how = "left") \
    .join(df_People_Score_Entitlement, ["MatchGroupEntitlement"], how = "left")
  
  #Step 3
  #Apply final logic. 
  #If the Weighted score is greater than total number of records than they have cross group matches, use Base to club them together
  #Else see if the match is on either of the group and use the ID from there
  
  df_People_Score = df_People_Score \
    .withColumn("MatchGroupID", expr("CASE \
      WHEN MatchGroupEmailScoreTotal > MatchGroupEmailCount THEN MatchGroupBase \
      WHEN MatchGroupMobileScoreTotal > MatchGroupMobileCount THEN MatchGroupBase \
      WHEN MatchGroupUSIScoreTotal > MatchGroupUSICount THEN MatchGroupBase \
      WHEN MatchGroupEntitlementScoreTotal > MatchGroupEntitlementCount THEN MatchGroupBase \
      ELSE COALESCE(MatchGroupEmail, MatchGroupMobile, MatchGroupUSI, MatchGroupEntitlement) END"))
  #df_People_Score = df_People_Score.withColumn("MatchGroupID", coalesce(col("MatchGroupIDEmail"), col("MatchGroupIDMobile"), col("MatchGroupIDUSI"), col("MatchGroupIDEntitlement")))
  
  return df_People_Score


# COMMAND ----------

def TestPrimaryLearner():

  dfPeopleUnits = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_people_units")
  dfFeesList = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_fees_list")
  dfPeople = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_people")
  dfAddresses = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_addresses")
  dfPeopleUsi = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_people_usi")
  dfUIO = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_unit_instance_occurrences")
  dfUI = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_unit_instances")
  dfAttainments = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_attainments")
  
  start_date = "2000-01-01" 
  end_date = "9999-12-31"

  
  df = GetAvetmissPrimaryLearner(dfPeople, dfAddresses, dfPeopleUnits, dfPeopleUsi, dfFeesList, dfUIO, dfUI, dfAttainments, start_date, end_date)
  return df


#df = TestPrimaryLearner()
#display(df.where ("CompletedThisYearScore > 3"))
