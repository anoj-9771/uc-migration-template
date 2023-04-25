# Databricks notebook source
# MAGIC %md 
# MAGIC Vno| Date      | Who         |Purpose
# MAGIC ---|:---------:|:-----------:|:--------:
# MAGIC 1  |05/04/2023 |Mag          |Initial

# COMMAND ----------

# MAGIC %run ../../Common/common-transform

# COMMAND ----------

#variables #
createdBy  =  'batch'

SurveyDim = spark.sql(f"""
       SELECT aa.id  as surveyId, 
       aa.name as surveyName, 
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
            'Qualtrics' as surveySourceSystem
        FROM (SELECT r.* FROM ( SELECT explode(result.elements) r FROM raw.qualtrics_surveys )) aa
                               ,controldb.dbo_extractLoadManifest bb 
        WHERE SystemCode = 'Qualtricsref'
          AND  aa.id = concat('SV_', regexp_extract(bb.SourceQuery, "/SV_(.*?)/", 1)) 
        """)

# COMMAND ----------

def Transform():
    global df    

    # ------------- TRANSFORMS ------------- # 
    _.Transforms = [
        f"surveySourceSystem||'|'||surveyId {BK}"
        ,"surveyName surveyName"
        ,"surveyDescription surveyDescription"
        ,"current_timestamp() as surveyStartDate"
        ,f"CAST(NULL AS timestamp) as surveyEndDate"
        ,"surveySourceSystem surveySourceSystem" 
        ,"current_timestamp() as createdDate"
        ,f"'{createdBy}' createdBy"
    ]
    
    df = SurveyDim.selectExpr(
        _.Transforms
    )

    # ------------- SAVE ------------------- #
    #df.display()
    #CleanSelf()
    Save(df)
    #DisplaySelf()
Transform()
