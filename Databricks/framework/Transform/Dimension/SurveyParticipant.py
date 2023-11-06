# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

# CleanSelf()

# COMMAND ----------

from pyspark.sql.functions import lit, col, when, row_number
from pyspark.sql.window import Window


def add_missing_columns(df, required_columns):
    for col_name in required_columns:
        if col_name not in df.schema.fieldNames():
            df = df.withColumn(col_name, lit(None))
    return df.select(required_columns).filter(col("recipientEmail").isNotNull())


table_name = ["qualtrics.billpaidsuccessfullyresponses", "qualtrics.businessConnectServiceRequestCloseResponses", "qualtrics.complaintsComplaintClosedResponses", "qualtrics.contactcentreinteractionmeasurementsurveyResponses", "qualtrics.customercareResponses", "qualtrics.developerapplicationreceivedResponses",
              "qualtrics.p4sonlinefeedbackResponses", "qualtrics.s73surveyResponses", "qualtrics.waterfixpostinteractionfeedbackResponses",
              "qualtrics.websitegoliveResponses", "qualtrics.wscs73experiencesurveyResponses", "qualtrics.feedbacktabgoliveResponses"]

required_columns = ["recipientEmail", "recipientFirstName", "recipientLastName", "customerFirstName", "customerLastName", "companyName", "ageGroup", "recordedDate"]

union_df = None

for table in table_name:
    df = GetTable(f"{getEnv()}cleansed.{table}")
    df = add_missing_columns(df, required_columns) 
    
    if union_df is None:
        union_df = df
    else:
        union_df = union_df.unionByName(df)
        

finaldf = (union_df.withColumn("sourceSystemCode", lit('Qualtrics').cast("string")) 
                  .withColumn("BusinessKey", concat_ws('|', union_df.recipientEmail, 
                                                            when((union_df.recipientFirstName).isNull(), lit('')).otherwise(union_df.recipientFirstName),
                                                            when((union_df.recipientLastName).isNull(), lit('')).otherwise(union_df.recipientLastName),
                                                            union_df.recordedDate)) 
                  .withColumn("sourceBusinessKey", concat_ws('|', union_df.recipientEmail, 
                                                            when((union_df.recipientFirstName).isNull(), lit('')).otherwise(union_df.recipientFirstName),
                                                            when((union_df.recipientLastName).isNull(), lit('')).otherwise(union_df.recipientLastName))) 
                  .dropDuplicates()) 


windowSpec = Window.partitionBy("sourceBusinessKey").orderBy(col("recordedDate").desc())
finaldf = (finaldf.withColumn("row_num", row_number().over(windowSpec)) 
                 .withColumn("lagValidDate", lag("recordedDate").over(windowSpec)- expr("Interval 1 milliseconds")) 
                 .withColumn("sourceRecordCurrent", when(col("row_num") == 1, 1).otherwise(0)))

# if not(TableExists(_.Destination)):
#     finaldf = finaldf.unionByName(spark.createDataFrame([dummyRecord(finaldf.schema)], finaldf.schema))  

# COMMAND ----------

def Transform():
    global df_final
    df_final = finaldf

    # ------------- TRANSFORMS ------------- # 
    _.Transforms = [
        f"BusinessKey {BK}"
        ,"recipientEmail surveyParticipantEmailRecipientId"
        ,"recipientFirstName surveyParticipantEmailRecipientFirstName"
        ,"recipientLastName surveyParticipantEmailRecipientSurname"
        ,"customerFirstName surveyParticipantCustomerFirstName"
        ,"customerLastName surveyParticipantCustomerSurname" 
        ,"companyName surveyParticipantCompanyName"
        ,"ageGroup surveyParticipantAgeGroupIndicator"
        ,"sourceBusinessKey sourceBusinessKey"
        ,"RecordedDate surveyRecordedTimestamp"
        ,"sourceRecordCurrent" 
        ,"RecordedDate sourceValidFromTimestamp"
        ,"CASE WHEN lagValidDate is NULL THEN CAST('9999-12-31' AS TIMESTAMP) ELSE lagValidDate END sourceValidToTimestamp"
        ,"sourceSystemCode sourceSystemCode"
    ]
    
    df_final = df_final.selectExpr(
        _.Transforms
    )

    # ------------- SAVE ------------------- #
    #df_final.display()
    #CleanSelf()
    Save(df_final)     
    #DisplaySelf()
Transform()

# COMMAND ----------

# MAGIC %sql
# MAGIC select surveyParticipantSK, count(1) from ppd_curated.dim.surveyParticipant group by all having count(1)>1

# COMMAND ----------


