# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

# CleanSelf()

# COMMAND ----------

surveyQualtrics = spark.sql(f"""
       SELECT aa.id  as surveyId, 
       aa.name as surveyName,
       'Qualtrics'||'|'||aa.id||'|' as businessKey, 
       CASE WHEN DestinationTableName LIKE '%BillPaidSuccessfullyQuestions%' THEN 'Survey to determine the customer experience of Bill payment'
            WHEN DestinationTableName LIKE '%BusinessConnectServiceRequestCloseQuestions%' THEN 'Survey to determine the customer experience about Business Requests'
            WHEN DestinationTableName LIKE '%ComplaintsComplaintClosedQuestions%' THEN 'Survey to determine customer complaints about dealing with Sydney Water'
            WHEN DestinationTableName LIKE '%ContactCentreInteractionMeasurementSurveyQuestions%' THEN 'Survey to measure customer experience with Sydney Water Contact Centre'
            WHEN DestinationTableName LIKE '%CustomerCareQuestions%' THEN 'Survey to measure the experience of dealing with Customer Care services at Sydney Water'
            WHEN DestinationTableName LIKE '%FeedbackTabGoLiveQuestions%' THEN 'Survey to measure the customer experience with Sydney Water website'
            WHEN DestinationTableName LIKE '%DeveloperApplicationReceivedQuestions%' THEN 'Survey to measure how Sydney Water can improve the quality of services for Developer Applications'
            WHEN DestinationTableName LIKE '%P4SOnlineFeedbackQuestions%' THEN 'Survey to measure how Sydney Water can improve its services to protect Personal Information and Privacy of customers'
            WHEN DestinationTableName LIKE '%S73SurveyQuestions%' THEN 'Survey to measure how Sydney Water can improve the quality of services for s73 Developer Applications'
            WHEN DestinationTableName LIKE '%WaterFixPostInteractionFeedbackQuestions%' THEN 'Survey to measure the success of Water efficiency programs'
            WHEN DestinationTableName LIKE '%WebsitegoliveQuestions%' THEN 'Survey to measure the overall customer experience of using Sydney Water website'
            WHEN DestinationTableName LIKE '%WSCS73ExperienceSurveyQuestions%' THEN 'Survey to measure how Sydney Water can improve the quality of services for wscs73 Developer Applications'
            ELSE NULL END as surveyDescription,
            'Qualtrics' as sourceSystemCode,
            CAST(NULL as TIMESTAMP) as createdDate,
            NULL as createdBy,
            NULL as surveyVersion,
            CAST(NULL AS TIMESTAMP) as surveyStartDate,
            CAST(NULL AS TIMESTAMP) as surveyEndDate,
            CAST(NULL as TIMESTAMP) as sourceValidFromDateTime,
            CAST(NULL as TIMESTAMP) as sourceValidToDateTime,
            CAST(NULL as INTEGER) as sourceRecordCurrent,
            'Qualtrics'||'|'||aa.id||'|' as sourceBusinessKey            
        FROM (SELECT r.* FROM ( SELECT explode(result.elements) r FROM {getEnv()}raw.qualtrics.surveys )) aa
                               ,controldb.dbo_extractLoadManifest bb 
        WHERE SystemCode = 'Qualtricsref'
          AND  aa.id = concat('SV_', regexp_extract(bb.SourceQuery, "/SV_(.*?)/", 1)) 
        """)


surveyCRM = spark.sql(f""" 
                     WITH maintab as
                     (Select * , dateadd(MILLISECOND ,-1,LEAD(sourceValidFromDateTime ) OVER(partition by surveyId order by sourceValidFromDateTime )) sourceValidToDateTime
                     from (Select distinct s.surveyID as surveyId,  questionnaireLong as surveyName,  s.createdBy createdByUserId ,  q.surveyVersion, 
                                  creationDateAt as sourceValidFromDateTime 
                                 from {get_env()}cleansed.crm.crm_svy_re_quest q
                                 INNER JOIN {get_env()}cleansed.crm.crm_svy_db_s s on q.surveyID = s.surveyID and q.surveyVersion = s.surveyVersion
                                  LEFT JOIN {get_env()}cleansed.crm.0svy_qstnnr_text S on q.questionnaire = S.questionnaireId                           
                                  ))
                       SELECT surveyId
                             ,surveyName
                             ,'CRM'||'|'||surveyId||'|'||surveyVersion as businessKey
                             ,'CRM'||'|'||surveyId||'|'||surveyVersion as sourceBusinessKey
                             ,NULL as surveyDescription
                             ,'CRM' as sourceSystemCode
                             ,sourceValidFromDateTime as createdDate
                             ,createdByUserId
                             ,surveyVersion
                             ,CAST(NULL AS TIMESTAMP) as surveyStartDate
                             ,CAST(NULL AS TIMESTAMP) as surveyEndDate
                             ,sourceValidFromDateTime
                             ,CASE WHEN sourceValidToDateTime IS NOT NULL THEN sourceValidToDateTime ELSE CAST('9999-12-31' as TIMESTAMP) END sourceValidToDateTime
                             ,CASE WHEN sourceValidToDateTime IS NOT NULL THEN '0' ELSE '1' END sourceRecordCurrent FROM maintab """)


createdByDF= spark.sql(f"""Select userid, concat_ws(' ',givenNames,surname) as createdBy 
                              from {get_env()}cleansed.aurion.employee_details """).drop_duplicates()

#remove duplicates USERID assignments has aurion data has duplicates #
windowSpec = Window.partitionBy("userid").orderBy(col("createdBy").desc())
createdByDF = (createdByDF.withColumn("row_num", row_number().over(windowSpec)) 
                         .filter(col("row_num") == 1).drop("row_num"))

##################################################
surveyCRM = surveyCRM.join(createdByDF, surveyCRM["createdByUserId"] == createdByDF["userid"], how='left').drop("userid", "createdByUserId")



# COMMAND ----------

try:
    existing_data = spark.sql(f"""select * from {get_table_namespace(f'{DEFAULT_TARGET}', f'{TableName}')} where sourceSystemCode = 'Qualtrics'""").filter("_recordCurrent == 1").filter("sourceRecordCurrent == 1")
    
    surveyQualtrics_new = surveyQualtrics.join(existing_data.select("surveyId","surveyName"),["surveyId","surveyName"],"left_anti")
except Exception as e:
    print(e)
    surveyQualtrics_new = surveyQualtrics

# COMMAND ----------

#surveyCRM.display()
survey = surveyQualtrics_new.unionByName(surveyCRM)

if not(TableExists(_.Destination)):
    survey = survey.unionByName(spark.createDataFrame([dummyRecord(survey.schema)], survey.schema))  

# COMMAND ----------

def Transform():
    global df    

    # ------------- TRANSFORMS ------------- # 
    _.Transforms = [
        f"businessKey {BK}"
        ,"sourceSystemCode" 
        ,"surveyId"
        ,"surveyName"
        ,"surveyDescription"
        ,"surveyStartDate surveyStartTimestamp"
        ,"surveyEndDate surveyEndTimestamp" 
        ,"createdDate surveyCreatedTimestamp" 
        ,"createdBy surveyCreatedByName"
        ,"CAST(surveyVersion as string) surveyVersionNumber"
        ,"sourceValidFromDatetime sourceValidFromTimestamp" 
        ,"sourceValidToDatetime sourceValidToTimestamp"
        ,"sourceRecordCurrent"
        ,"sourceBusinessKey"

    ]
    
    df = survey.selectExpr(
        _.Transforms
    )

    # ------------- SAVE ------------------- #
    #df.display()
    SaveDefaultSource(df)
    #DisplaySelf()
Transform()

# COMMAND ----------

# MAGIC %sql
# MAGIC select surveySK,count(1) from ppd_curated.dim.survey group by all having count(1)> 1
