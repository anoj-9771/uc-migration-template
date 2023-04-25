# Databricks notebook source
# MAGIC %md 
# MAGIC Vno| Date      | Who         |Purpose
# MAGIC ---|:---------:|:-----------:|:--------:
# MAGIC 1  |21/04/2023 |Mag          |Initial

# COMMAND ----------

# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

from pyspark.sql.functions import *

def transpose_df(df, quesdf, duplicate_columns):     
    cols_to_transpose = [col for col in df.columns if col.lower().startswith("question")]
    #cols_to_transpose = [col for col in df.columns if any (col.startswith(prefix) for prefix in ["Question", "question"])]    
    stack_expr = f"stack({len(cols_to_transpose)}, " + ", ".join([f"'{col}', cast({col} as string)" for col in cols_to_transpose]) + ")"

    respdf = df.selectExpr(
        stack_expr + " as (questionRelation, answerValue)",
        *duplicate_columns
    ) 
    
    initial_select =  [ respdf[cols] for cols in respdf.columns] + [quesdf[cols] for cols in quesdf.columns if cols not in ['questionRelation']]
    rc_df = respdf.join(quesdf, (respdf["questionRelation"] == quesdf["responseCodeColumn"]) &(respdf["surveyId"] == quesdf["surveyId"]), how = 'inner') \
                           .select(*initial_select).withColumnRenamed("answerValue", "responseCode") \
                           .drop(quesdf["surveyId"]) \
                           .drop("questionRelation", "responseTextColumn")
    rt_df = respdf.join(quesdf, (respdf["questionRelation"] == quesdf["responseTextColumn"]) &(respdf["surveyId"] == quesdf["surveyId"]), how = 'inner') \
                           .select(*initial_select).withColumnRenamed("answerValue", "responseText") \
                           .drop(quesdf["surveyId"]) \
                           .drop("questionRelation", "responseCodeColumn") 
                    
    df_list = [rc_df, rt_df]
    filter_nulls_df = [df.filter(df.surveyID.isNotNull() & df.surveyQuestionSK.isNotNull() & df.recordId.isNotNull()) for df in df_list]

    union_df = filter_nulls_df[0]
    for df in filter_nulls_df[1:]:
        union_df = union_df.union(df)

    distinctKeys_df = union_df.select("surveyId","surveyQuestionSK","recordId").distinct()

    distinctKeys_df.createOrReplaceTempView("mainTab")                    
    rc_df.createOrReplaceTempView("respCode")  
    rt_df.createOrReplaceTempView("respTxt")

    respQry = """ WITH mainQ as (SELECT AA.surveyID surveyId, 
                         coalesce(BB.surveyName, CC.surveyName ) surveyName,
                         AA.recordId recordId,
                         AA.surveyQuestionSK surveyQuestionSK,
                         coalesce(BB.surveyFK, CC.surveyFK ) surveyFK,
                         coalesce(BB.questionID, CC.questionID ) questionID,
                         coalesce(BB.questionPartID, CC.questionPartID ) questionPartID,
                         coalesce(BB.QuestionText, CC.QuestionText ) QuestionText,                       
                         BB.responseCode responseCode,
                         CC.responseText responseText,
                         '' as sentimentDsc, 
                         CAST(NULL as BIGINT) sentimentPolarityNumber, 
                         CAST(NULL as BIGINT) sentimentScore, 
                         '' as topicsText,
                         NULL as topicSentimentScore,
                         NULL as topicsSentimentsLabel,
                         NULL as parTopicsText,  
                         coalesce(BB.recipientEmail, CC.recipientEmail ) recipientEmail,
                         coalesce(BB.recipientFirstName, CC.recipientFirstName ) recipientFirstName,
                         coalesce(BB.recipientLastName, CC.recipientLastName ) recipientLastName,
                         coalesce(BB.propertyNumber, CC.propertyNumber ) propertyNumber,
                         coalesce(BB.assignedTo, CC.assignedTo ) assignedTo 
                    FROM mainTab AA LEFT JOIN respCode BB ON AA.recordId = BB.recordId and AA.surveyId = BB.surveyId and AA.surveyQuestionSK = BB.surveyQuestionSK 
                                    LEFT JOIN respTxt  CC ON AA.recordId = CC.recordId and AA.surveyId = CC.surveyId and AA.surveyQuestionSK = CC.surveyQuestionSK )
                     SELECT * FROM mainQ WHERE ((responseCode is not NULL) or  (responseText is not NULL)) """ 

    coredf = spark.sql(respQry)

    return coredf 

# COMMAND ----------

def transpose_feedback_df(df, quesdf, duplicate_columns):     
    cols_to_transpose = [col for col in df.columns if col.lower().startswith("question")]
    #cols_to_transpose = [col for col in df.columns if any (col.startswith(prefix) for prefix in ["Question", "question"])]    
    stack_expr = f"stack({len(cols_to_transpose)}, " + ", ".join([f"'{col}', cast({col} as string)" for col in cols_to_transpose]) + ")"

    respdf = df.selectExpr(
        stack_expr + " as (questionRelation, answerValue)",
        *duplicate_columns
    )     

    initial_select =  [ respdf[cols] for cols in respdf.columns] + [quesdf[cols] for cols in quesdf.columns if cols not in['questionRelation','responseCodeColumn', 'responseTextColumn',
                                                                                                                            'sentimentDscColumn','sentimentPolarityNumberColumn', 'sentimentScoreColumn',
                                                                                                                            'topicsTextColumn','topicSentimentScoreColumn','topicsSentimentsLabelColumn',
                                                                                                                            'parTopicsTextColumn']]
    rc_df = respdf.join(quesdf, (respdf["questionRelation"] == quesdf["responseCodeColumn"]) &(respdf["surveyId"] == quesdf["surveyId"]), how = 'inner') \
                           .select(*initial_select).withColumnRenamed("answerValue", "responseCode") \
                           .drop(quesdf["surveyId"]) \
                           .alias("rc")  
    
    rt_df = respdf.join(quesdf, (respdf["questionRelation"] == quesdf["responseTextColumn"]) &(respdf["surveyId"] == quesdf["surveyId"]), how = 'inner') \
                           .select(*initial_select).withColumnRenamed("answerValue", "responseText")  \
                           .drop(quesdf["surveyId"]) \
                           .alias("rt") 
    sd_df = respdf.join(quesdf, (respdf["questionRelation"] == quesdf["SentimentDscColumn"]) &(respdf["surveyId"] == quesdf["surveyId"]), how = 'inner') \
                           .select(*initial_select).withColumnRenamed("answerValue", "sentimentDsc") \
                           .drop(quesdf["surveyId"]) \
                           .alias("sd") 
    sp_df = respdf.join(quesdf, (respdf["questionRelation"] == quesdf["SentimentPolarityNumberColumn"]) &(respdf["surveyId"] == quesdf["surveyId"]), how = 'inner') \
                           .select(*initial_select).withColumnRenamed("answerValue", "sentimentPolarityNumber") \
                           .drop(quesdf["surveyId"]) \
                           .alias("sp")

    ss_df = respdf.join(quesdf, (respdf["questionRelation"] == quesdf["SentimentScoreColumn"]) &(respdf["surveyId"] == quesdf["surveyId"]), how = 'inner') \
                           .select(*initial_select).withColumnRenamed("answerValue", "sentimentScore") \
                           .drop(quesdf["surveyId"]) \
                           .alias("ss")

    tt_df = respdf.join(quesdf, (respdf["questionRelation"] == quesdf["TopicsTextColumn"]) &(respdf["surveyId"] == quesdf["surveyId"]), how = 'inner') \
                           .select(*initial_select).withColumnRenamed("answerValue", "topicsText") \
                           .drop(quesdf["surveyId"]) \
                           .alias("tt")

    ts_df = respdf.join(quesdf, (respdf["questionRelation"] == quesdf["TopicSentimentScoreColumn"]) &(respdf["surveyId"] == quesdf["surveyId"]), how = 'inner') \
                           .select(*initial_select).withColumnRenamed("answerValue", "topicSentimentScore") \
                           .drop(quesdf["surveyId"]) \
                           .alias("ts")

    tl_df = respdf.join(quesdf, (respdf["questionRelation"] == quesdf["TopicsSentimentsLabelColumn"]) &(respdf["surveyId"] == quesdf["surveyId"]), how = 'inner') \
                           .select(*initial_select).withColumnRenamed("answerValue", "topicsSentimentsLabel") \
                           .drop(quesdf["surveyId"]) \
                           .alias("tl")

    pt_df = respdf.join(quesdf, (respdf["questionRelation"] == quesdf["ParTopicsTextColumn"]) &(respdf["surveyId"] == quesdf["surveyId"]), how = 'inner') \
                           .select(*initial_select).withColumnRenamed("answerValue", "parTopicsText") \
                           .drop(quesdf["surveyId"]) \
                           .alias("pt")  

    df_list = [rc_df, rt_df, sd_df, sp_df, ss_df, tt_df, ts_df, tl_df, pt_df]
    filter_nulls_df = [df.filter(df.surveyID.isNotNull() & df.surveyQuestionSK.isNotNull() & df.recordId.isNotNull()) for df in df_list]

    union_df = filter_nulls_df[0]
    for df in filter_nulls_df[1:]:
        union_df = union_df.union(df)

    distinctKeys_df = union_df.select("surveyId","surveyQuestionSK","recordId").distinct().alias("mainK") 

    distinctKeys_df.createOrReplaceTempView("mainTab")                    
    rc_df.createOrReplaceTempView("respCode")  
    rt_df.createOrReplaceTempView("respTxt")
    sd_df.createOrReplaceTempView("sentDsc")
    sp_df.createOrReplaceTempView("sentPol") 
    ss_df.createOrReplaceTempView("sentSco") 
    tt_df.createOrReplaceTempView("topTxt") 
    ts_df.createOrReplaceTempView("topSen") 
    tl_df.createOrReplaceTempView("topLbl") 
    pt_df.createOrReplaceTempView("parTxt") 

    respQry = """ WITH mainQ as (SELECT AA.surveyId surveyId, 
                         coalesce(BB.surveyName, CC.surveyName, DD.surveyName, EE.surveyName, FF.surveyName, GG.surveyName, HH.surveyName, II.surveyName, JJ.surveyName) surveyName,
                         AA.recordId recordId,
                         AA.surveyQuestionSK surveyQuestionSK,
                         coalesce(BB.surveyFK, CC.surveyFK, DD.surveyFK, EE.surveyFK, FF.surveyFK, GG.surveyFK, HH.surveyFK, II.surveyFK, JJ.surveyFK) surveyFK,
                         coalesce(BB.questionID, CC.questionID, DD.questionID, EE.questionID, FF.questionID, GG.questionID, HH.questionID, II.questionID, JJ.questionID) questionID,
                         coalesce(BB.questionPartID, CC.questionPartID, DD.questionPartID, EE.questionPartID, FF.questionPartID, GG.questionPartID, HH.questionPartID, II.questionPartID, JJ.questionPartID) questionPartID,
                         coalesce(BB.QuestionText, CC.QuestionText, DD.QuestionText, EE.QuestionText, FF.QuestionText, GG.QuestionText, HH.QuestionText, II.QuestionText, JJ.QuestionText) QuestionText,                       
                         BB.responseCode responseCode,
                         CC.responseText responseText,
                         DD.sentimentDsc  sentimentDsc, 
                         EE.sentimentPolarityNumber sentimentPolarityNumber, 
                         FF.sentimentScore sentimentScore, 
                         GG.topicsText topicsText,
                         HH.topicSentimentScore topicSentimentScore,
                         II.topicsSentimentsLabel topicsSentimentsLabel,
                         JJ.parTopicsText parTopicsText,
                         -1 recipientEmail,
                         -1 recipientFirstName,
                         -1 recipientLastName,
                         -1 propertyNumber,
                         -1 assignedTo  
                    FROM mainTab AA LEFT JOIN respCode BB ON AA.recordId = BB.recordId and AA.surveyId = BB.surveyId and AA.surveyQuestionSK = BB.surveyQuestionSK 
                                    LEFT JOIN respTxt  CC ON AA.recordId = CC.recordId and AA.surveyId = CC.surveyId and AA.surveyQuestionSK = CC.surveyQuestionSK
                                    LEFT JOIN sentDsc  DD ON AA.recordId = DD.recordId and AA.surveyId = DD.surveyId and AA.surveyQuestionSK = DD.surveyQuestionSK
                                    LEFT JOIN sentPol  EE ON AA.recordId = EE.recordId and AA.surveyId = EE.surveyId and AA.surveyQuestionSK = EE.surveyQuestionSK
                                    LEFT JOIN sentSco  FF ON AA.recordId = FF.recordId and AA.surveyId = FF.surveyId and AA.surveyQuestionSK = FF.surveyQuestionSK
                                    LEFT JOIN topTxt   GG ON AA.recordId = GG.recordId and AA.surveyId = GG.surveyId and AA.surveyQuestionSK = GG.surveyQuestionSK
                                    LEFT JOIN topSen   HH ON AA.recordId = HH.recordId and AA.surveyId = HH.surveyId and AA.surveyQuestionSK = HH.surveyQuestionSK
                                    LEFT JOIN topLbl   II ON AA.recordId = II.recordId and AA.surveyId = II.surveyId and AA.surveyQuestionSK = II.surveyQuestionSK
                                    LEFT JOIN parTxt   JJ ON AA.recordId = JJ.recordId and AA.surveyId = JJ.surveyId and AA.surveyQuestionSK = JJ.surveyQuestionSK )
                     SELECT * FROM mainQ WHERE ((responseCode is not NULL) or  (responseText is not NULL) or  (sentimentDsc is not NULL) or
                                                (sentimentPolarityNumber is not NULL) or  (sentimentScore is not NULL) or  (topicsText is not NULL) or
                                                (topicSentimentScore is not NULL) or  (topicsSentimentsLabel is not NULL) or  (parTopicsText is not NULL)  ) """ 

    coredf = spark.sql(respQry)

    return coredf 

# COMMAND ----------

duplicate_columns = ["surveyID", "surveyName", "recordId", "recipientEmail", "recipientFirstName", "recipientLastName", "propertyNumber", "assignedTo"]

def add_missing_columns(df, required_columns):
    for col_name in required_columns:
        if col_name not in df.schema.fieldNames():
            df = df.withColumn(col_name, lit('-1'))
    return df


dimQuesQuery = f"""select surveyQuestionSK,
                            surveyFK,
                            surveyID surveyId,
                            CAST(ltrim('QID', questionId) AS INTEGER) questionId,
                            questionPartID questionPartId, 
                            concat_ws('-PartQ-', questionText, questionPartText)  QuestionText,       
                            concat_ws('','question',CAST(ltrim('QID', questionId) AS INTEGER), CASE WHEN questionPartID is null then '' else concat('Part', questionPartID) END,'ResponseCode') as responseCodeColumn,
                            concat_ws('','question',CAST(ltrim('QID', questionId) AS INTEGER), CASE WHEN questionPartID is null then '' else concat('Part', questionPartID) END,'ResponseText') as responseTextColumn,
                            concat_ws('','question',CAST(ltrim('QID', questionId) AS INTEGER), 'SentimentDsc') as sentimentDscColumn,
                            concat_ws('','question',CAST(ltrim('QID', questionId) AS INTEGER), 'SentimentPolarityNumber') as sentimentPolarityNumberColumn,
                            concat_ws('','question',CAST(ltrim('QID', questionId) AS INTEGER), 'SentimentScore') as sentimentScoreColumn,
                            concat_ws('','question',CAST(ltrim('QID', questionId) AS INTEGER), 'TopicsText') as topicsTextColumn,
                            concat_ws('','question',CAST(ltrim('QID', questionId) AS INTEGER), 'TopicSentimentScore') as topicSentimentScoreColumn,
                            concat_ws('','question',CAST(ltrim('QID', questionId) AS INTEGER), 'TopicsSentimentsLabel') as topicsSentimentsLabelColumn,
                            concat_ws('','question',CAST(ltrim('QID', questionId) AS INTEGER), 'ParTopicsText') as parTopicsTextColumn
                            from {TARGET}.dimSurveyQuestion 
                    """


df_dimQues = spark.sql(dimQuesQuery)

dsv = GetTable(f"{TARGET}.dimsurveyparticipant")
dsr = GetTable(f"{TARGET}.dimsurveyresponseinformation")
dp  = GetTable(f"{TARGET}.dimProperty") 
dbp = GetTable(f"{TARGET}.dimbusinesspartner")

df_billpaid =  GetTable(f"{SOURCE}.qualtrics_billpaidsuccessfullyresponses")
df_billpaid = add_missing_columns(df_billpaid, duplicate_columns) 

df_businessXconn =  GetTable(f"{SOURCE}.qualtrics_businessconnectservicerequestcloseresponses")
df_businessXconn = add_missing_columns(df_businessXconn, duplicate_columns)

df_complaintsClosed  =  GetTable(f"{SOURCE}.qualtrics_complaintscomplaintclosedresponses")
df_complaintsClosed = add_missing_columns(df_complaintsClosed, duplicate_columns)

df_contactCentreInteract  =  GetTable(f"{SOURCE}.qualtrics_contactcentreinteractionmeasurementsurveyresponses")
df_contactCentreInteract = add_missing_columns(df_contactCentreInteract, duplicate_columns)

df_Customercareresponses  =  GetTable(f"{SOURCE}.qualtrics_customercareresponses")
df_Customercareresponses = add_missing_columns(df_Customercareresponses, duplicate_columns)

df_devApplicationreceived  =  GetTable(f"{SOURCE}.qualtrics_developerapplicationreceivedresponses")
df_devApplicationreceived = add_missing_columns(df_devApplicationreceived, duplicate_columns)

df_feedbacktabgolive  =  GetTable(f"{SOURCE}.qualtrics_feedbacktabgoliveresponses")
df_feedbacktabgolive = add_missing_columns(df_feedbacktabgolive, duplicate_columns)

df_p4sonlinefeedback  =  GetTable(f"{SOURCE}.qualtrics_p4sonlinefeedbackresponses")
df_p4sonlinefeedback = add_missing_columns(df_p4sonlinefeedback, duplicate_columns)

df_s73surveyresponse   =  GetTable(f"{SOURCE}.qualtrics_s73surveyresponses")
df_s73surveyresponse = add_missing_columns(df_s73surveyresponse, duplicate_columns)

df_waterfixpost =  GetTable(f"{SOURCE}.qualtrics_waterfixpostinteractionfeedbackresponses")
df_waterfixpost = add_missing_columns(df_waterfixpost, duplicate_columns)

df_websitegolive =  GetTable(f"{SOURCE}.qualtrics_websitegoliveresponses")
df_websitegolive = add_missing_columns(df_websitegolive, duplicate_columns)

df_wscs73exp =  GetTable(f"{SOURCE}.qualtrics_wscs73experiencesurveyresponses")
df_wscs73exp = add_missing_columns(df_wscs73exp, duplicate_columns)

billpaid_df  = transpose_df(df_billpaid, df_dimQues, duplicate_columns)
billpaid_df = billpaid_df.join(dsv.filter(dsv._recordCurrent == 1), (billpaid_df["recipientEmail"] == dsv["emailRecepient"]) & (billpaid_df["recipientFirstName"] == dsv["emailRecepientFirstname"]) 
                                      & (billpaid_df["recipientLastName"] == dsv["emailRecepientSurname"]), how = 'left') \
                         .join(dsr.filter(dsr._recordCurrent == 1), (billpaid_df["surveyId"] == dsr["surveyID"]) & (billpaid_df["recordId"] == dsr["responseId"]), how = 'left') \
                         .join(dp.filter(dp._RecordCurrent == 1), (billpaid_df["propertyNumber"] == dp["propertyNumber"]), how = 'left') \
                         .select(billpaid_df["*"], dsv["surveyParticipantSK"], dsr["surveyResponseInformationSK"], dp["propertySK"]) \
                         .withColumn("businessPartnerSK", lit('-1')) \
                         .withColumn("sourceSystem", lit('Qualtrics'))

businessXconn_df = transpose_df(df_businessXconn, df_dimQues, duplicate_columns)
businessXconn_df = businessXconn_df.join(dbp.filter(dbp._RecordCurrent == 1), (businessXconn_df["assignedTo"] == dbp["businessPartnerNumber"]), how = 'left') \
                                   .select(businessXconn_df["*"], dbp["businessPartnerSK"]) \
                                   .withColumn("surveyParticipantSK", lit('-1'))\
                                   .withColumn("surveyResponseInformationSK", lit('-1'))\
                                   .withColumn("propertySK", lit('-1')) \
                                   .withColumn("sourceSystem", lit('Qualtrics'))

complaintsClosed_df = transpose_df(df_complaintsClosed, df_dimQues, duplicate_columns)
complaintsClosed_df = complaintsClosed_df.join(dsv.filter(dsv._recordCurrent == 1), (complaintsClosed_df["recipientEmail"] == dsv["emailRecepient"]) \
                                      & (complaintsClosed_df["recipientFirstName"] == dsv["emailRecepientFirstname"]) \
                                      & (complaintsClosed_df["recipientLastName"] == dsv["emailRecepientSurname"]), how = 'left') \
                         .join(dsr.filter(dsr._recordCurrent == 1), (complaintsClosed_df["surveyId"] == dsr["surveyID"]) & (complaintsClosed_df["recordId"] == dsr["responseId"]), how = 'left') \
                         .join(dp.filter(dp._RecordCurrent == 1), (complaintsClosed_df["propertyNumber"] == dp["propertyNumber"]), how = 'left') \
                         .select(complaintsClosed_df["*"], dsv["surveyParticipantSK"], dsr["surveyResponseInformationSK"], dp["propertySK"]) \
                         .withColumn("businessPartnerSK", lit('-1')) \
                         .withColumn("sourceSystem", lit('Qualtrics'))


contactCentreInteract_df = transpose_df(df_contactCentreInteract, df_dimQues, duplicate_columns)
contactCentreInteract_df = contactCentreInteract_df.join(dsv.filter(dsv._recordCurrent == 1), (contactCentreInteract_df["recipientEmail"] == dsv["emailRecepient"]) \
                                                         & (contactCentreInteract_df["recipientFirstName"] == dsv["emailRecepientFirstname"]) \
                                                         & (contactCentreInteract_df["recipientLastName"] == dsv["emailRecepientSurname"]), how = 'left') \
                                                    .select(contactCentreInteract_df["*"], dsv["surveyParticipantSK"]) \
                                                    .withColumn("businessPartnerSK", lit('-1')) \
                                                    .withColumn("surveyResponseInformationSK", lit('-1')) \
                                                    .withColumn("propertySK", lit('-1')) \
                                                    .withColumn("sourceSystem", lit('Qualtrics'))


Customercareresponses_df = transpose_df(df_Customercareresponses, df_dimQues, duplicate_columns)
Customercareresponses_df = Customercareresponses_df.join(dsv.filter(dsv._recordCurrent == 1), (Customercareresponses_df["recipientEmail"] == dsv["emailRecepient"]) \
                                                         & (Customercareresponses_df["recipientFirstName"] == dsv["emailRecepientFirstname"]) \
                                                         & (Customercareresponses_df["recipientLastName"] == dsv["emailRecepientSurname"]), how = 'left') \
                                                    .select(Customercareresponses_df["*"], dsv["surveyParticipantSK"]) \
                                                    .withColumn("businessPartnerSK", lit('-1')) \
                                                    .withColumn("surveyResponseInformationSK", lit('-1')) \
                                                    .withColumn("propertySK", lit('-1')) \
                                                    .withColumn("sourceSystem", lit('Qualtrics'))

waterfixpost_df = transpose_df(df_waterfixpost, df_dimQues, duplicate_columns)
waterfixpost_df = waterfixpost_df.join(dsv.filter(dsv._recordCurrent == 1), (waterfixpost_df["recipientEmail"] == dsv["emailRecepient"]) \
                                                         & (waterfixpost_df["recipientFirstName"] == dsv["emailRecepientFirstname"]) \
                                                         & (waterfixpost_df["recipientLastName"] == dsv["emailRecepientSurname"]), how = 'left') \
                                                    .select(waterfixpost_df["*"], dsv["surveyParticipantSK"]) \
                                                    .withColumn("businessPartnerSK", lit('-1')) \
                                                    .withColumn("surveyResponseInformationSK", lit('-1')) \
                                                    .withColumn("propertySK", lit('-1')) \
                                                    .withColumn("sourceSystem", lit('Qualtrics'))

devApplicationreceived_df = transpose_df(df_devApplicationreceived, df_dimQues, duplicate_columns)
devApplicationreceived_df = devApplicationreceived_df.withColumn("businessPartnerSK", lit('-1')) \
                                                     .withColumn("surveyResponseInformationSK", lit('-1')) \
                                                     .withColumn("propertySK", lit('-1')) \
                                                     .withColumn("sourceSystem", lit('Qualtrics')) \
                                                     .withColumn("surveyParticipantSK", lit('-1'))



p4sonlinefeedback_df = transpose_df(df_p4sonlinefeedback, df_dimQues, duplicate_columns)
p4sonlinefeedback_df = p4sonlinefeedback_df.withColumn("businessPartnerSK", lit('-1')) \
                                                     .withColumn("surveyResponseInformationSK", lit('-1')) \
                                                     .withColumn("propertySK", lit('-1')) \
                                                     .withColumn("sourceSystem", lit('Qualtrics')) \
                                                     .withColumn("surveyParticipantSK", lit('-1'))

s73surveyresponse_df = transpose_df(df_s73surveyresponse, df_dimQues, duplicate_columns)
s73surveyresponse_df = s73surveyresponse_df.withColumn("businessPartnerSK", lit('-1')) \
                                                     .withColumn("surveyResponseInformationSK", lit('-1')) \
                                                     .withColumn("propertySK", lit('-1')) \
                                                     .withColumn("sourceSystem", lit('Qualtrics')) \
                                                     .withColumn("surveyParticipantSK", lit('-1'))


websitegolive_df = transpose_df(df_websitegolive, df_dimQues, duplicate_columns)
websitegolive_df = websitegolive_df.withColumn("businessPartnerSK", lit('-1')) \
                                                     .withColumn("surveyResponseInformationSK", lit('-1')) \
                                                     .withColumn("propertySK", lit('-1')) \
                                                     .withColumn("sourceSystem", lit('Qualtrics')) \
                                                     .withColumn("surveyParticipantSK", lit('-1'))


wscs73exp_df = transpose_df(df_wscs73exp, df_dimQues, duplicate_columns)
wscs73exp_df = wscs73exp_df.withColumn("businessPartnerSK", lit('-1')) \
                                                     .withColumn("surveyResponseInformationSK", lit('-1')) \
                                                     .withColumn("propertySK", lit('-1')) \
                                                     .withColumn("sourceSystem", lit('Qualtrics')) \
                                                     .withColumn("surveyParticipantSK", lit('-1'))


feedbacktabgolive_df = transpose_feedback_df(df_feedbacktabgolive, df_dimQues, duplicate_columns)
feedbacktabgolive_df = feedbacktabgolive_df.withColumn("businessPartnerSK", lit('-1')) \
                                                     .withColumn("surveyResponseInformationSK", lit('-1')) \
                                                     .withColumn("propertySK", lit('-1')) \
                                                     .withColumn("sourceSystem", lit('Qualtrics')) \
                                                     .withColumn("surveyParticipantSK", lit('-1'))


finaldf = billpaid_df.union(businessXconn_df) \
                     .union(complaintsClosed_df) \
                     .union(contactCentreInteract_df) \
                     .union(Customercareresponses_df) \
                     .union(waterfixpost_df) \
                     .union(devApplicationreceived_df) \
                     .union(p4sonlinefeedback_df) \
                     .union(s73surveyresponse_df) \
                     .union(websitegolive_df) \
                     .union(wscs73exp_df) \
                     .union(feedbacktabgolive_df) 


# COMMAND ----------

def Transform():
    global df_final
    df_final = finaldf

    # ------------- TRANSFORMS ------------- # 
    _.Transforms = [
        f"sourceSystem||'|'||surveyId||'|'||questionId||'|'||surveyQuestionSK||'|'||recordId  {BK}"
        ,"propertySK propertyNumberFK" 
        ,"businessPartnerSK businessPartnerNumberFK"
        ,"surveyFK surveyFK"
        ,"surveyQuestionSK surveyQuestionFK"
        ,"surveyResponseInformationSK surveyResponseInformationFK" 
        ,"surveyParticipantSK surveyParticipantFK"
        ,"surveyId surveyId"
        ,"questionId surveyquestionId" 
        ,"questionPartId questionPartId" 
        ,"recordId responseId" 
        ,"responseCode responseCode" 
        ,"responseText responseText"
        ,"sentimentDsc sentimentDescription" 
        ,"sentimentPolarityNumber sentimentPolarityNumber" 
        ,"sentimentScore sentimentScore"         
        ,"topicSentimentScore topicSentimentScore" 
        ,"topicsSentimentsLabel topicSentimentLabel" 
        ,"topicsText topicsText" 
        ,"parTopicsText parTopicsText"               
        ,"sourceSystem sourceSystem"
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
