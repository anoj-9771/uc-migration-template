# Databricks notebook source
# MAGIC %md 
# MAGIC Vno| Date      | Who         |Purpose
# MAGIC ---|:---------:|:-----------:|:--------:
# MAGIC 1  |11/04/2023 |Mag          |Initial

# COMMAND ----------

# MAGIC %run ../../Common/common-transform 

# COMMAND ---------- 

# MAGIC %run ../../Common/common-helpers 
# COMMAND ---------- 


# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

from pyspark.sql.functions import lit

def add_missing_columns(df, required_columns):
    for col_name in required_columns:
        if col_name not in df.schema.fieldNames():
            df = df.withColumn(col_name, lit(None))
    return df.select(required_columns)


table_name = ["qualtrics_billpaidsuccessfullyresponses", "qualtrics_businessConnectServiceRequestCloseResponses", "qualtrics_complaintsComplaintClosedResponses", "qualtrics_contactcentreinteractionmeasurementsurveyResponses", "qualtrics_customercareResponses", "qualtrics_developerapplicationreceivedResponses",
              "qualtrics_p4sonlinefeedbackResponses", "qualtrics_s73surveyResponses", "qualtrics_waterfixpostinteractionfeedbackResponses",
              "qualtrics_websitegoliveResponses", "qualtrics_wscs73experiencesurveyResponses", "qualtrics_feedbacktabgoliveResponses"]

required_columns = ["surveyID", "recordId", "startDate", "endDate", "finished", "status", "recordedDate"]

qualtricsDF = None

for table in table_name:
    df = GetTable(f"{get_table_namespace(f'{SOURCE}', f'{table}')}")
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
                                    FROM  cleansed.crm_0crm_srv_req_inci_h I  
                                    INNER JOIN cleansed.crm_crmd_link L on I.serviceRequestGUID = L.hiGUID and setobjecttype = 58
                                    INNER JOIN cleansed.crm_crmd_survey S on S.setGUID = L.setGUID
                                    INNER JOIN (Select * , split_part(surveyValueKeyAttribute, '/',1) as questionID from cleansed.crm_crm_svy_db_sv SV1 where surveyValuesVersion = (Select max(surveyValuesVersion) from cleansed.crm_crm_svy_db_sv where surveyValuesGUID = SV1.surveyValuesGUID )) SV on SV.surveyValuesGUID = S.surveyValuesGuid
                                    INNER JOIN cleansed.crm_crm_svy_re_quest R ON R.questionID = SV.questionID and SV.surveyValuesVersion = R.surveyVersion  
                                    INNER JOIN cleansed.crm_crm_svy_db_s SDB on R.surveyID = SDB.surveyID and R.surveyVersion = SDB.surveyVersion 
                                    INNER JOIN cleansed.crm_0svy_qstnnr_text Q on  Q.questionnaireId = R.questionnaire
                                    INNER JOIN cleansed.crm_0svy_quest_text QT on QT.questionnaireId = Q.questionnaireId AND QT.surveyQuestionId = R.questionId 
                                    WHERE R.surveyID != 'Z_BILLASSIST_SURVEY'  """)



finaldf = qualtricsDF.unionByName(crmDF)

# COMMAND ----------

def Transform():
    global df_final
    df_final = finaldf

    # ------------- TRANSFORMS ------------- # 
    _.Transforms = [f"sourceSystemCode||'|'||surveyId||'|'||surveyResponseId {BK}" ,'*']
    
    df_final = df_final.selectExpr(
        _.Transforms
    )

    # ------------- SAVE ------------------- #
    #df_final.display()
    #CleanSelf()
    Save(df_final)
    #DisplaySelf()
Transform()
