# Databricks notebook source
# MAGIC %run ../common-controldb

# COMMAND ----------

df_main = spark.sql("""
WITH _Base AS 
(
    SELECT 'Questions' Type, 'Qualtricsref' SystemCode, 'Qualtrics' SourceSchema, 'qualtrics-auth-clientId' SourceKeyVaultSecret, 'qualtrics-load' SourceHandler, 'raw-load' RawHandler, 'cleansed-load-qualtrics' CleansedHandler, 'json' RawFileExtension, '' WatermarkColumn
    ,'{ "CleansedQuery" : "SELECT r.* FROM ( SELECT explode(result.elements) r FROM {tableFqn} ) Q" }' ExtendedProperties
    ,'https://syd1.qualtrics.com/API/v3/survey-definitions/$SURVEY_ID$/questions' SourceQuery
    UNION SELECT 'Responses' Type, 'Qualtricsdata' SystemCode, 'Qualtrics' SourceSchema, 'qualtrics-auth-clientId' SourceKeyVaultSecret, 'qualtrics-responses-load' SourceHandler, 'raw-load' RawHandler, 'cleansed-load-qualtrics' CleansedHandler, 'json' RawFileExtension, '' WatermarkColumn
    ,'{ "CleansedQuery" : "SELECT r.values.* FROM ( SELECT explode(responses) r FROM {tableFqn}) R" }' ExtendedProperties
    ,'https://syd1.qualtrics.com/API/v3/surveys/$SURVEY_ID$/export-responses' SourceQuery
),
_Surveys AS (
    SELECT 'SV_5nBg9k8g82wKVZs' SurveyId, 'BillPaidSuccessfully' TableName
    UNION SELECT 'SV_89cmPdrG4sKNBvU' SurveyId, 'BusinessConnectServiceRequestClose' TableName
    UNION SELECT 'SV_41p0ItsUQf81zZb' SurveyId, 'ComplaintsComplaintClosed' TableName
    UNION SELECT 'SV_ey9P9D7WIOOBA7c' SurveyId, 'ContactCentreInteractionMeasurementSurvey' TableName
    UNION SELECT 'SV_cFPFq1Pdw1PDsOO' SurveyId, 'CustomerCare' TableName
    UNION SELECT 'SV_bkGwmzYr5TJGFpk' SurveyId, 'DAFTestSurvey' TableName
    UNION SELECT 'SV_6mqWO3tTVB0FcNf' SurveyId, 'DeveloperApplicationReceived' TableName
    UNION SELECT 'SV_9GrGuXTjnyQqqpM' SurveyId, 'FeedbackTabGoLive' TableName
    UNION SELECT 'SV_2rdgSuw7pwGJZ9c' SurveyId, 'P4SOnlineFeedback' TableName
    UNION SELECT 'SV_0OK7lzXf0QalVR3' SurveyId, 'S73Survey' TableName
    UNION SELECT 'SV_agz0ozaXwOjnJyJ' SurveyId, 'WaterFixPostInteractionFeedback' TableName
    UNION SELECT 'SV_6SxFZRoY4nOAoDA' SurveyId, 'Websitegolive' TableName
    UNION SELECT 'SV_6VAdcfAD8inWnXL' SurveyId, 'WSCS73ExperienceSurvey' TableName
)
SELECT 'Qualtricsref' SystemCode, 'Qualtrics' SourceSchema, 'Surveys' SourceTableName, 'https://syd1.qualtrics.com/API/v3/surveys' SourceQuery, 'qualtrics-auth-clientId' SourceKeyVaultSecret, 'qualtrics-load' SourceHandler, 'json' RawFileExtension, '' ExtendedProperties, 'raw-load' RawHandler, 'cleansed-load-qualtrics' CleansedHandler, '' WatermarkColumn
UNION
SELECT 
SystemCode
,SourceSchema
,TableName || Type SourceTableName
,REPLACE(SourceQuery, '$SURVEY_ID$', SurveyId) SourceQuery
,SourceKeyVaultSecret
,SourceHandler
,RawFileExtension
,ExtendedProperties
,RawHandler
,CleansedHandler
,WatermarkColumn
FROM _Base B
JOIN _Surveys S ON 1=1
""")

# COMMAND ----------

#Redefine AddIngestion for
# 1. to add NULLIF to handle null columns
# 2. to populate cleansedHandler based on config
def AddIngestion(df, clean = False):
    df = AddSystemCode(df)
    if clean:
        CleanConfig()
        
#     for i in df.rdd.collect():
        
#         #cleansedHandler = i.CleansedHandler 
#         cleansedHandler = 'NULL'
        
#         ExecuteStatement(f"""
#         DECLARE @RC int
#         DECLARE @SystemCode varchar(max) = '{i.SystemCode}'
#         DECLARE @Schema varchar(max) = '{i.SystemCode}'
#         DECLARE @Table varchar(max) = '{i.SourceTableName}'
#         DECLARE @Query varchar(max) = '{i.SourceQuery}'
#         DECLARE @WatermarkColumn varchar(max) = NULL
#         DECLARE @SourceHandler varchar(max) = '{i.SourceHandler}'
#         DECLARE @RawFileExtension varchar(max) = '{i.RawFileExtension}'
#         DECLARE @KeyVaultSecret varchar(max) = '{i.SourceKeyVaultSecret}'
#         DECLARE @ExtendedProperties varchar(max) = '{i.ExtendedProperties}'
#         DECLARE @RawHandler varchar(max) = '{i.RawHandler}'
#         DECLARE @CleansedHandler varchar(max) = {cleansedHandler}

#         EXECUTE @RC = [dbo].[AddIngestion] 
#            @SystemCode
#           ,@Schema
#           ,@Table
#           ,@Query
#           ,@WatermarkColumn
#           ,@SourceHandler
#           ,@RawFileExtension
#           ,@KeyVaultSecret
#           ,@ExtendedProperties
#           ,@RawHandler
#           ,@RawHandler
#         """)
    for i in df.rdd.collect():
        ExecuteStatement(f"""    
        DECLARE @RC int
        DECLARE @SystemCode varchar(max) = NULLIF('{i.SystemCode}','')
        DECLARE @Schema varchar(max) = NULLIF('{i.SourceSchema}','')
        DECLARE @Table varchar(max) = NULLIF('{i.SourceTableName}','')
        DECLARE @Query varchar(max) = NULLIF('{i.SourceQuery}','')
        DECLARE @WatermarkColumn varchar(max) = NULL
        DECLARE @SourceHandler varchar(max) = NULLIF('{i.SourceHandler}','')
        DECLARE @RawFileExtension varchar(max) = NULLIF('{i.RawFileExtension}','')
        DECLARE @KeyVaultSecret varchar(max) = NULLIF('{i.SourceKeyVaultSecret}','')
        DECLARE @ExtendedProperties varchar(max) = NULLIF('{i.ExtendedProperties}','')
        DECLARE @RawHandler varchar(max) = NULLIF('{i.RawHandler}','') 
        DECLARE @CleansedHandler varchar(max) = NULLIF('{i.CleansedHandler}','')    

        EXECUTE @RC = [dbo].[AddIngestion] 
           @SystemCode
          ,@Schema
          ,@Table
          ,@Query
          ,@WatermarkColumn
          ,@SourceHandler
          ,@RawFileExtension
          ,@KeyVaultSecret
          ,@ExtendedProperties
          ,@RawHandler
          ,@CleansedHandler
        """)        

# COMMAND ----------

def ConfigureManifest(dataFrameConfig, whereClause=None):
    # ------------- CONSTRUCT QUERY ----------------- #
    df = dataFrameConfig.where(whereClause)
    # ------------- DISPLAY ----------------- #
#     ShowQuery(df)

    # ------------- SAVE ----------------- #
    AddIngestion(df, clean=True)
    
    # ------------- ShowConfig ----------------- #
    ShowConfig()

for system_code in ['Qualtricsref','Qualtricsdata']:
    SYSTEM_CODE = system_code
    ConfigureManifest(df_main, whereClause=f"SystemCode = '{SYSTEM_CODE}'")
    
#ADD BUSINESS KEY
ExecuteStatement("""
update dbo.extractLoadManifest set
businessKeyColumn = 'recordId'
where systemCode in ('Qualtricsref','Qualtricsdata')
and sourceTableName like '%responses'
""")      

# COMMAND ----------

#ADD RECORD INTO CONFIG TABLE TO MASK COLUMNS IN CLEANSED-LOAD-QUALTRICS
#Manually run this for environments (dev,test,preprod) that needs masking. 
# ExecuteStatement("""
# merge into dbo.config as target using(
#     select
#         keyGroup = 'maskColumns'
#         ,[key] = 'Qualtrics'
#         ,value = 1
# ) as source on
#     target.keyGroup = source.keyGroup
#     and target.[key] = source.[key]
# when not matched then insert(
#     keyGroup
#     ,[key]
#     ,value
#     ,createdDTS
# )
# values(
#     source.keyGroup
#     ,source.[key]
#     ,source.value
#     ,CONVERT(DATETIME, CONVERT(DATETIMEOFFSET, GETDATE()) AT TIME ZONE 'AUS Eastern Standard Time')
# )
# when matched then update
#     set value = source.value
# ;
# """)
