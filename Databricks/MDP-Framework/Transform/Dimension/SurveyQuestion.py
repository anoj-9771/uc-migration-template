# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

DimSurvey  = (GetTable(f"{getEnv()}curated.dim.Survey").select("surveySK", "surveyName"))

Survey1  = (GetTable(f"{getEnv()}cleansed.qualtrics.billpaidsuccessfullyquestions")
                .withColumn("sourceSystem", lit("Qualtrics")) 
                .select("surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "answers", "choices", "surveyName", "sourceSystem"))

Survey2  = (GetTable(f"{getEnv()}cleansed.qualtrics.businessConnectServiceRequestCloseQuestions")
                .withColumn("sourceSystem", lit("Qualtrics")) 
                .select("surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "answers", "choices", "surveyName", "sourceSystem"))

Survey3  = (GetTable(f"{getEnv()}cleansed.qualtrics.complaintsComplaintClosedQuestions")  
                .withColumn("sourceSystem", lit("Qualtrics")) 
                .select("surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "answers", "choices", "surveyName", "sourceSystem"))

Survey4  = (GetTable(f"{getEnv()}cleansed.qualtrics.contactcentreinteractionmeasurementsurveyquestions")
                .withColumn("sourceSystem", lit("Qualtrics")) 
                .select("surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "answers", "choices", "surveyName", "sourceSystem"))

Survey5  = (GetTable(f"{getEnv()}cleansed.qualtrics.customercarequestions")
                .withColumn("sourceSystem", lit("Qualtrics")) 
                .select("surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "answers", "choices", "surveyName", "sourceSystem"))

Survey6  = (GetTable(f"{getEnv()}cleansed.qualtrics.developerapplicationreceivedquestions")
                .withColumn("sourceSystem", lit("Qualtrics")) 
                .select("surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "answers", "choices", "surveyName", "sourceSystem"))

Survey7  = (GetTable(f"{getEnv()}cleansed.qualtrics.p4sonlinefeedbackquestions")
                .withColumn("sourceSystem", lit("Qualtrics")) 
                .select("surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "answers", "choices", "surveyName", "sourceSystem"))

Survey8  = (GetTable(f"{getEnv()}cleansed.qualtrics.s73surveyquestions")
                .withColumn("sourceSystem", lit("Qualtrics")) 
                .select("surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "answers", "choices", "surveyName", "sourceSystem"))

Survey9 =  (GetTable(f"{getEnv()}cleansed.qualtrics.waterfixpostinteractionfeedbackquestions")
                .withColumn("sourceSystem", lit("Qualtrics")) 
                .select("surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "answers", "choices", "surveyName", "sourceSystem"))

Survey10 = (GetTable(f"{getEnv()}cleansed.qualtrics.websitegolivequestions")
                .withColumn("sourceSystem", lit("Qualtrics")) 
                .withColumn("answers", lit(None).cast("string")) 
                .select("surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "answers", "choices", "surveyName", "sourceSystem"))

Survey11 = (GetTable(f"{getEnv()}cleansed.qualtrics.wscs73experiencesurveyquestions")
                .withColumn("sourceSystem", lit("Qualtrics")) 
                .withColumn("answers", lit(None).cast("string")) 
                .select("surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "answers", "choices", "surveyName", "sourceSystem"))

Survey12  = (GetTable(f"{getEnv()}cleansed.qualtrics.feedbacktabgolivequestions")
                 .withColumn("sourceSystem", lit("Qualtrics")) 
                 .withColumn("answers", lit(None).cast("string")) 
                 .select("surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "answers", "choices", "surveyName", "sourceSystem"))

# COMMAND ----------

from pyspark.sql.functions import col, udf, from_json, to_json, explode_outer, when, lit
from pyspark.sql.types import  ArrayType, MapType, StringType, StructType, StructField

def choices_to_array(row):
    if row is None:
        return []
    
    if isinstance(row, dict):
        items = row.items()
    else:
        items = row.asDict().items()
    
    result = []
    for questionPartId, questionPartText in items:
        if questionPartText is not None:
            result.append((int(questionPartId), questionPartText.Display))
    return result


choices_to_array_udf = udf(choices_to_array, ArrayType(StructType([StructField("questionPartId", StringType()), StructField("questionPartText", StringType())])))

Choiceschema = MapType(
    StringType(),
    StructType([
       StructField("Display", StringType())
    ])
)




Surveys = [Survey1, Survey2, Survey3, Survey4, Survey5, Survey6, Survey7, Survey8, Survey9, Survey10, Survey11, Survey12]

union_df = None

for df in Surveys:
    
    answers_schema = None
    choices_schema = None
    answers_is_struct = None
    choices_is_struct = None
    
    
    for field in df.schema.fields:
        if field.name == "answers":
            answers_schema = field.dataType.jsonValue()
            answers_dType = field.dataType
            answers_is_struct = isinstance(answers_dType, StructType)            
        if field.name == "choices":
            choices_schema = field.dataType.jsonValue()
            choices_dType = field.dataType
            choices_is_struct = isinstance(choices_dType, StructType)
            
        
            
    if answers_is_struct and choices_is_struct:
        stringconvertedDF = (df.withColumn("choices_array", choices_to_array_udf(col("choices"))) 
                              .withColumn("answers", to_json(col("answers"), answers_schema)) 
                              .withColumn("choices", to_json(col("choices"), choices_schema)) 
                              .withColumn("split", when(col("QuestionType") == "Matrix", col("choices_array")).otherwise(None)))        
    elif choices_is_struct or answers_is_struct:
        if choices_is_struct:
            stringconvertedDF = (df.withColumn("choices_array", choices_to_array_udf(col("choices"))) 
                                  .withColumn("choices", to_json(col("choices"), choices_schema)) 
                                  .withColumn("split", when(col("QuestionType") == "Matrix", col("choices_array")).otherwise(None)))           
        elif answers_is_struct:
            stringconvertedDF = (df.withColumn("answers", to_json(col("answers"), answers_schema)) 
                                  .withColumn("ChoiceStruct", from_json(col("choices"), Choiceschema)) 
                                  .withColumn("choices_array", choices_to_array_udf(col("ChoiceStruct"))) 
                                  .withColumn("split", when(col("QuestionType") == "Matrix", col("choices_array")).otherwise(None)))
    else:
        stringconvertedDF = (df.withColumn("ChoiceStruct", from_json(col("choices"), Choiceschema)) 
                              .withColumn("choices_array", choices_to_array_udf(col("ChoiceStruct"))) 
                              .withColumn("split", when(col("QuestionType") == "Matrix", col("choices_array")).otherwise(None)))
            
          
    
    joinDF = stringconvertedDF.join(DimSurvey, stringconvertedDF["surveyName"] == DimSurvey["surveyName"], "inner").drop("surveyName")                               
    flattened_df = (joinDF.select("*", explode_outer("split").alias("kv")) 
                        .select("surveySK", "surveyId", "questionid", "questionText", "questionDescription", "QuestionType", "kv.questionPartId", "kv.questionPartText", "answers", "choices", "sourceSystem") )
                        
                       
    flattened_df = flattened_df.withColumn("questionPartText", regexp_replace(col("questionPartText"), '<strong>|</strong>', ''))
    
    if union_df is None:
        union_df = flattened_df
    else:
        union_df = union_df.union(flattened_df)

#########Added CRM Survey #############################
union_df = union_df.withColumn("surveyVersion", lit(None).cast("string"))

dimBuss = GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'dimSurvey')}")
crmQues = GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_crm_svy_re_quest')}")

split_col = split(col("sourceBusinessKey"), r"\|")
dimBuss = dimBuss.withColumn("surveyID", split_col.getItem(1))
#dimBuss.display()
crmQuestion = (dimBuss.join(crmQues, (dimBuss["surveyID"] == crmQues["surveyID"]) & (dimBuss["surveyVersionNumber"] == crmQues["surveyVersion"])) 
                     .select(dimBuss["surveySK"], crmQues["surveyID"], crmQues["questionId"], crmQues["surveyVersion"].alias("surveyVersion"), 
                             crmQues["longDescription"].alias("questionText"), crmQues["longDescription"].alias("questionDescription"))) 


crmQuestion =   (crmQuestion.withColumn("questionType", lit(None).cast("string")) 
                           .withColumn("questionPartId", lit(None).cast("string")) 
                           .withColumn("questionPartText", lit(None).cast("string")) 
                           .withColumn("answers", lit(None).cast("string")) 
                           .withColumn("choices", lit(None).cast("string")) 
                           .withColumn("surveyVersion", col("surveyVersion")) 
                           .withColumn("sourceSystem", lit('CRM').cast("string")) 
                           .select("surveySK", "surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "questionPartId", "questionPartText", "answers", "choices", "surveyVersion", "sourceSystem"))


union_df = union_df.unionByName(crmQuestion)

if not(TableExists(_.Destination)):
    union_df = union_df.unionByName(spark.createDataFrame([dummyRecord(union_df.schema)], union_df.schema)) 

# COMMAND ----------

def Transform():
    global df_final
    df_final = union_df

    # ------------- TRANSFORMS ------------- # 
    _.Transforms = [
        f"sourceSystem||'|'||surveyID||'|'||CASE WHEN sourceSystem = 'CRM' THEN surveyVersion ELSE '' END||'|'||questionId||'|'||CASE WHEN questionPartId IS NULL THEN '' ELSE questionPartId END  {BK}"
        ,"surveySK surveyFK"
        ,"surveyID surveyID"
        ,"surveyVersion surveyVersionNumber"
        ,"questionId surveyQuestionId"
        ,"questionText surveyQuestionText"
        ,"questionDescription surveyQuestionDescription" 
        ,"questionType surveyQuestionTypeName"
        ,"questionPartId surveyQuestionPartId"
        ,"questionPartText surveyQuestionPartText"
        ,"answers surveyAnswersText"
        ,"choices surveyChoicesText"
        ,"sourceSystem sourceSystemCode"
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
