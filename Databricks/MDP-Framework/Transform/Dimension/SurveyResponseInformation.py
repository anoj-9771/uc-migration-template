# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

# CleanSelf()

# COMMAND ----------

from pyspark.sql.functions import lit

def add_missing_columns(df, required_columns):
    for col_name in required_columns:
        if col_name not in df.schema.fieldNames():
            df = df.withColumn(col_name, lit(None))
    return df.select(required_columns)


table_name = ["qualtrics.billpaidsuccessfullyresponses", "qualtrics.businessConnectServiceRequestCloseResponses", "qualtrics.complaintsComplaintClosedResponses", "qualtrics.contactcentreinteractionmeasurementsurveyResponses", "qualtrics.customercareResponses", "qualtrics.developerapplicationreceivedResponses",
              "qualtrics.p4sonlinefeedbackResponses", "qualtrics.s73surveyResponses", "qualtrics.waterfixpostinteractionfeedbackResponses",
              "qualtrics.websitegoliveResponses", "qualtrics.wscs73experiencesurveyResponses", "qualtrics.feedbacktabgoliveResponses"]

required_columns = ["surveyID", "recordId", "startDate", "endDate", "finished", "status", "recordedDate"]

qualtricsDF = None

for table in table_name:
    df = GetTable(f"{getEnv()}cleansed.{table}")
    df = add_missing_columns(df, required_columns)
    df = ( df.select(col("surveyID").alias("surveyId")
                     ,col("recordId").alias("surveyResponseId")
                     ,col("startDate").alias("surveyResponseStartTimestamp")
                     ,col("endDate").alias("surveyResponseEndTimestamp")
                     ,col("finished").alias("surveyFinishedIndicator")
                     ,col("status").alias("surveyResponseStatusIndicator")
                     ,col("recordedDate").alias("surveyResponseRecordedTimestamp")
                     ,lit('Qualtrics').cast("string").alias("sourceSystemCode")

    )) 
    
    if qualtricsDF is None:
        qualtricsDF = df
    else:
        qualtricsDF = qualtricsDF.unionByName(df)                

# COMMAND ----------

###################CRM Response #####################################
crmDF = spark.sql(f""" Select distinct R.surveyID surveyId,
                                      SV.surveyValuesGUID as surveyResponseId,
                                    CAST(NULL as timestamp) as surveyResponseStartTimestamp,
                                    CAST(NULL as timestamp) as surveyResponseEndTimestamp,
                                    CAST(NULL as string)    as surveyFinishedIndicator,
                                    CAST(NULL as string)    as surveyResponseStatusIndicator,
                                    CAST(NULL as timestamp) as surveyResponseRecordedTimestamp,
                                    'CRM' as sourceSystemCode
                                    FROM  {getEnv()}cleansed.crm.0crm_srv_req_inci_h I  
                                    INNER JOIN {getEnv()}cleansed.crm.crmd_link L on I.serviceRequestGUID = L.hiGUID and setobjecttype = 58
                                    INNER JOIN {getEnv()}cleansed.crm.crmd_survey S on S.setGUID = L.setGUID
                                    INNER JOIN (Select * , split_part(surveyValueKeyAttribute, '/',1) as questionID from {getEnv()}cleansed.crm.crm_svy_db_sv SV1 where surveyValuesVersion = (Select max(surveyValuesVersion) 
                                    from {getEnv()}cleansed.crm.crm_svy_db_sv where surveyValuesGUID = SV1.surveyValuesGUID )) SV on SV.surveyValuesGUID = S.surveyValuesGuid
                                    INNER JOIN {getEnv()}cleansed.crm.crm_svy_re_quest R ON R.questionID = SV.questionID and SV.surveyValuesVersion = R.surveyVersion  
                                    INNER JOIN {getEnv()}cleansed.crm.crm_svy_db_s SDB on R.surveyID = SDB.surveyID and R.surveyVersion = SDB.surveyVersion 
                                    INNER JOIN {getEnv()}cleansed.crm.0svy_qstnnr_text Q on  Q.questionnaireId = R.questionnaire
                                    INNER JOIN {getEnv()}cleansed.crm.0svy_quest_text QT on QT.questionnaireId = Q.questionnaireId AND QT.surveyQuestionId = R.questionId 
                                    WHERE R.surveyID != 'Z_BILLASSIST_SURVEY'  """)



finaldf = qualtricsDF.unionByName(crmDF)

if not(TableExists(_.Destination)):
    finaldf = finaldf.unionByName(spark.createDataFrame([dummyRecord(finaldf.schema)], finaldf.schema)) 

# COMMAND ----------

def Transform():
    global df_final
    df_final = finaldf

    # ------------- TRANSFORMS ------------- # 
    _.Transforms = [f"sourceSystemCode||'|'||surveyResponseId {BK}" ,'*']
    
    df_final = df_final.selectExpr(
        _.Transforms
    )

    # ------------- SAVE ------------------- #
    #df_final.display()
    Save(df_final)
    #DisplaySelf()
Transform()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from ppd_curated.dim.surveyResponseInformation group by all having count(1)>1
