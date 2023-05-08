# Databricks notebook source
# MAGIC %md 
# MAGIC Vno| Date      | Who         |Purpose
# MAGIC ---|:---------:|:-----------:|:--------:
# MAGIC 1  |04/05/2023 |Mag          |Initial

# COMMAND ----------

# MAGIC %run ../../Common/common-transform

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

DimSurvey  = GetTable(f"{DEFAULT_TARGET}.dimSurvey").select("surveySK", "surveyName") 

Survey1  = GetTable(f"{SOURCE}.qualtrics_billpaidsuccessfullyquestions").withColumn("sourceSystem", lit("Qualtrics")) \
                .select("surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "answers", "choices", "surveyName", "sourceSystem")

Survey2  = GetTable(f"{SOURCE}.qualtrics_businessConnectServiceRequestCloseQuestions").withColumn("sourceSystem", lit("Qualtrics")) \
                .select("surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "answers", "choices", "surveyName", "sourceSystem")

Survey3  = GetTable(f"{SOURCE}.qualtrics_complaintsComplaintClosedQuestions").withColumn("sourceSystem", lit("Qualtrics")) \
                .select("surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "answers", "choices", "surveyName", "sourceSystem")

Survey4  = GetTable(f"{SOURCE}.qualtrics_contactcentreinteractionmeasurementsurveyquestions").withColumn("sourceSystem", lit("Qualtrics")) \
                .select("surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "answers", "choices", "surveyName", "sourceSystem")

Survey5  = GetTable(f"{SOURCE}.qualtrics_customercarequestions").withColumn("sourceSystem", lit("Qualtrics")) \
                .select("surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "answers", "choices", "surveyName", "sourceSystem")

Survey6  = GetTable(f"{SOURCE}.qualtrics_developerapplicationreceivedquestions").withColumn("sourceSystem", lit("Qualtrics")) \
                .select("surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "answers", "choices", "surveyName", "sourceSystem")

Survey7  = GetTable(f"{SOURCE}.qualtrics_p4sonlinefeedbackquestions").withColumn("sourceSystem", lit("Qualtrics")) \
                .select("surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "answers", "choices", "surveyName", "sourceSystem")

Survey8  = GetTable(f"{SOURCE}.qualtrics_s73surveyquestions").withColumn("sourceSystem", lit("Qualtrics")) \
                .select("surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "answers", "choices", "surveyName", "sourceSystem")

Survey9 =  GetTable(f"{SOURCE}.qualtrics_waterfixpostinteractionfeedbackquestions").withColumn("sourceSystem", lit("Qualtrics")) \
                .select("surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "answers", "choices", "surveyName", "sourceSystem")

Survey10 = GetTable(f"{SOURCE}.qualtrics_websitegolivequestions").withColumn("sourceSystem", lit("Qualtrics")) \
                .withColumn("answers", lit(None).cast("string")) \
                .select("surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "answers", "choices", "surveyName", "sourceSystem")

Survey11 = GetTable(f"{SOURCE}.qualtrics_wscs73experiencesurveyquestions").withColumn("sourceSystem", lit("Qualtrics")) \
                .withColumn("answers", lit(None).cast("string")) \
                .select("surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "answers", "choices", "surveyName", "sourceSystem")

Survey12  = GetTable(f"{SOURCE}.qualtrics_feedbacktabgolivequestions").withColumn("sourceSystem", lit("Qualtrics"))  \
                 .withColumn("answers", lit(None).cast("string")) \
                 .select("surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "answers", "choices", "surveyName", "sourceSystem")


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
        stringconvertedDF = df.withColumn("choices_array", choices_to_array_udf(col("choices"))) \
                              .withColumn("answers", to_json(col("answers"), answers_schema)) \
                              .withColumn("choices", to_json(col("choices"), choices_schema)) \
                              .withColumn("split", when(col("QuestionType") == "Matrix", col("choices_array")).otherwise(None))        
    elif choices_is_struct or answers_is_struct:
        if choices_is_struct:
            stringconvertedDF = df.withColumn("choices_array", choices_to_array_udf(col("choices"))) \
                                  .withColumn("choices", to_json(col("choices"), choices_schema)) \
                                  .withColumn("split", when(col("QuestionType") == "Matrix", col("choices_array")).otherwise(None))           
        elif answers_is_struct:
            stringconvertedDF = df.withColumn("answers", to_json(col("answers"), answers_schema)) \
                                  .withColumn("ChoiceStruct", from_json(col("choices"), Choiceschema)) \
                                  .withColumn("choices_array", choices_to_array_udf(col("ChoiceStruct"))) \
                                  .withColumn("split", when(col("QuestionType") == "Matrix", col("choices_array")).otherwise(None))
    else:
        stringconvertedDF = df.withColumn("ChoiceStruct", from_json(col("choices"), Choiceschema)) \
                              .withColumn("choices_array", choices_to_array_udf(col("ChoiceStruct"))) \
                              .withColumn("split", when(col("QuestionType") == "Matrix", col("choices_array")).otherwise(None))
            
          
    
    joinDF = stringconvertedDF.join(DimSurvey, stringconvertedDF["surveyName"] == DimSurvey["surveyName"], "inner").drop("surveyName")
    flattened_df = joinDF.select("*", explode_outer("split").alias("kv")) \
                        .select("surveySK", "surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "kv.questionPartId", "kv.questionPartText", "answers", "choices", "sourceSystem") 
                        
                       
    
    if union_df is None:
        union_df = flattened_df
    else:
        union_df = union_df.union(flattened_df)

#########Added CRM Survey #############################
union_df = union_df.withColumn("surveyVersion", lit(None).cast("string"))

dimBuss = GetTable(f"{DEFAULT_TARGET}.dimSurvey")
crmQues = GetTable(f"{SOURCE}.crm_crm_svy_re_quest")

split_col = split(col("sourceBusinessKey"), r"\|")
dimBuss = dimBuss.withColumn("surveyID", split_col.getItem(1))
#dimBuss.display()
crmQuestion = dimBuss.join(crmQues, (dimBuss["surveyID"] == crmQues["surveyID"]) & (dimBuss["surveyVersion"] == crmQues["surveyVersion"])) \
                     .select(dimBuss["surveySK"], crmQues["surveyID"], crmQues["questionId"], crmQues["surveyVersion"].alias("surveyVersion"), crmQues["longDescription"].alias("questionText"), crmQues["longDescription"].alias("questionDescription")) 


crmQuestion =   crmQuestion.withColumn("questionType", lit(None).cast("string")) \
                           .withColumn("questionPartId", lit(None).cast("string")) \
                           .withColumn("questionPartText", lit(None).cast("string")) \
                           .withColumn("answers", lit(None).cast("string")) \
                           .withColumn("choices", lit(None).cast("string")) \
                           .withColumn("surveyVersion", col("surveyVersion")) \
                           .withColumn("sourceSystem", lit('CRM').cast("string")) \
                           .select("surveySK", "surveyID", "questionid", "questionText", "questionDescription", "QuestionType", "questionPartId", "questionPartText", "answers", "choices", "surveyVersion", "sourceSystem")


union_df = union_df.unionByName(crmQuestion)

#crmQuestion.display()
#######################################################

# COMMAND ----------

def Transform():
    global df_final
    df_final = union_df

    # ------------- TRANSFORMS ------------- # 
    _.Transforms = [
        f"sourceSystem||'|'||surveyID||'|'||questionId||'|'||CASE WHEN questionPartId IS NULL THEN '' ELSE questionPartId END ||'|'||CASE WHEN sourceSystem = 'CRM' THEN surveyVersion ELSE '' END {BK}"
        ,"surveySK surveyFK"
        ,"surveyID surveyID"
        ,"surveyVersion surveyVersion"
        ,"questionId questionId"
        ,"questionText questionText"
        ,"questionDescription questionDescription" 
        ,"questionType questionType"
        ,"questionPartId questionPartId"
        ,"questionPartText questionPartText"
        ,"answers answers"
        ,"choices choices"
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
