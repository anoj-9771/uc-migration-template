# Databricks notebook source
# MAGIC %md 
# MAGIC Vno| Date      | Who         |Purpose
# MAGIC ---|:---------:|:-----------:|:--------:
# MAGIC 1  |04/05/2023 |Mag          |Initial

# COMMAND ----------

# MAGIC %run ../../Common/common-transform

# COMMAND ----------

SurveyDim = spark.sql(f"""
       SELECT aa.id  as surveyId, 
       aa.name as surveyName,
       'Qualtrics'||'|'||aa.id||'|'||current_timestamp() as businessKey, 
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
            NULL as sourceValidFromDateTime,
            NULL as sourceValidToDateTime,
            NULL as sourceRecordCurrent,
            'Qualtrics'||'|'||aa.id||'|'||current_timestamp() as sourceBusinessKey,
            'batch'  as createdBy,          
            'Qualtrics' as surveySourceSystem
        FROM (SELECT r.* FROM ( SELECT explode(result.elements) r FROM raw.qualtrics_surveys )) aa
                               ,controldb.dbo_extractLoadManifest bb 
        WHERE SystemCode = 'Qualtricsref'
          AND  aa.id = concat('SV_', regexp_extract(bb.SourceQuery, "/SV_(.*?)/", 1)) 
        """)

SurveyCRM = spark.sql(F"""Select * , CASE WHEN ISNULL(dateadd(MILLISECOND ,-1,LEAD(validFromDate) OVER(partition by surveyId order by validFromDate ))) THEN CAST('9999-12-31' as TIMESTAMP) 
                               ELSE dateadd(MILLISECOND ,-1,LEAD(validFromDate) OVER(partition by surveyID order by validFromDate )) END  as validToDate 
                            from (Select distinct s.surveyID,  questionnaireLong as SurveyName,  s.createdBy,  q.surveyVersion , creationDateAt as validFromDate 
                            FROM {SOURCE}.crm_crm_svy_re_quest q
                            INNER JOIN {SOURCE}.crm_crm_svy_db_s s on q.surveyID = s.surveyID and q.surveyVersion = s.surveyVersion
                            LEFT JOIN {SOURCE}.crm_0svy_qstnnr_text S on q.questionnaire = S.questionnaireId) A """)

# COMMAND ----------

# Select surveyID as surveyId, 
#        surveyName, 
#        validFromDate as sourceValidFromDateTime,
#        CASE WHEN ISNULL(dateadd(MILLISECOND ,-1,LEAD(validFromDate) OVER(partition by surveyId order by validFromDate ))) THEN CAST('9999-12-31' as TIMESTAMP) 
#                                ELSE dateadd(MILLISECOND ,-1,LEAD(validFromDate) OVER(partition by surveyID order by validFromDate )) END  as sourceValidToDateTime,
#        NULL createdBy--(Select * from view_name where createdBy = userid and rownum = 1) createdBy
# from (
# Select distinct s.surveyID,  questionnaireLong as surveyName,  s.createdBy ,  q.surveyVersion , creationDateAt as validFromDate 
# from cleansed.crm_crm_svy_re_quest q
# Inner JOIN cleansed.crm_crm_svy_db_s s on q.surveyID = s.surveyID and q.surveyVersion = s.surveyVersion
# LEFT Join cleansed.crm_0svy_qstnnr_text S on q.questionnaire = S.questionnaireId)


# "Select * , CASE WHEN ISNULL(dateadd(MILLISECOND ,-1,LEAD(validFromDate) OVER(partition by surveyId order by validFromDate ))) THEN CAST('9999-12-31' as TIMESTAMP) 
#                                ELSE dateadd(MILLISECOND ,-1,LEAD(validFromDate) OVER(partition by surveyID order by validFromDate )) END  as validToDate 
# from (
# Select distinct s.surveyID,  questionnaireLong as SurveyName,  s.createdBy,  q.surveyVersion , creationDateAt as validFromDate 
# from crm_crm_svy_re_quest q
# Inner JOIN crm_crm_svy_db_s s on q.surveyID = s.surveyID and q.surveyVersion = s.surveyVersion
# LEFT Join crm_0svy_qstnnr_text S on q.questionnaire = S.questionnaireId
# ) A"

# COMMAND ----------



# COMMAND ----------

def Transform():
    global df    

    # ------------- TRANSFORMS ------------- # 
    _.Transforms = [
        f"businessKey {BK}"
        ,"surveyName surveyName"
        ,"surveyDescription surveyDescription"
        ,"current_timestamp() as surveyStartDate"
        ,f"CAST(NULL AS timestamp) as surveyEndDate"
        ,"surveySourceSystem surveySourceSystem" 
        ,"current_timestamp() as createdDate"
        ,"createdBy createdBy"
        ,"sourceValidFromDateTime" 
        ,"sourceValidToDateTime"
        ,"sourceRecordCurrent"
        ,"sourceBusinessKey"

    ]
    
    df = SurveyDim.selectExpr(
        _.Transforms
    )

    # ------------- SAVE ------------------- #
    #df.display()
    #CleanSelf()
    SaveDefaultSource(df)
    #DisplaySelf()
Transform()
