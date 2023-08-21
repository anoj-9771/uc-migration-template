# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

# CleanSelf()

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

from pyspark.sql.functions import *

def transpose_df(df, quesdf, duplicate_columns):     
    cols_to_transpose = [col for col in df.columns if col.lower().startswith("question")]
    #cols_to_transpose = [col for col in df.columns if any (col.startswith(prefix) for prefix in ["Question", "question"])]  
    df.fillna('NOT_ANSWERED_SKIPPED')
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

    distinctKeys_df = union_df.select("surveyId","surveyQuestionSK","recordId","recordedDate").distinct()

    distinctKeys_df.createOrReplaceTempView("mainTab")                    
    rc_df.createOrReplaceTempView("respCode")  
    rt_df.createOrReplaceTempView("respTxt")

    respQry = """ WITH mainQ as (SELECT AA.surveyID surveyId, 
                         coalesce(BB.surveyName, CC.surveyName ) surveyName,
                         AA.recordId recordId,
                         AA.recordedDate snapshotDate,
                         AA.surveyQuestionSK surveyQuestionSK,
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
    df.fillna('NOT_ANSWERED_SKIPPED')  
    stack_expr = f"stack({len(cols_to_transpose)}, " + ", ".join([f"'{col}', cast({col} as string)" for col in cols_to_transpose]) + ")"

    respdf = df.selectExpr(
        stack_expr + " as (questionRelation, answerValue)",
        *duplicate_columns
    )     

    initial_select =  [ respdf[cols] for cols in respdf.columns] + [quesdf[cols] for cols in quesdf.columns if cols not in['questionRelation','responseCodeColumn', 'responseTextColumn','sentimentDscColumn','sentimentPolarityNumberColumn', 'sentimentScoreColumn','topicsTextColumn','topicSentimentScoreColumn','topicsSentimentsLabelColumn','parTopicsTextColumn']]
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

    distinctKeys_df = union_df.select("surveyId","surveyQuestionSK","recordId","recordedDate").distinct().alias("mainK") 

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
                         AA.recordedDate as snapshotDate,
                         AA.surveyQuestionSK surveyQuestionSK,
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

duplicate_columns = ["surveyID", "surveyName", "recordId", "recordedDate", "recipientEmail", "recipientFirstName", "recipientLastName", "propertyNumber", "assignedTo"]

def add_missing_columns(df, required_columns):
    for col_name in required_columns:
        
        if col_name not in df.schema.fieldNames() and col_name not in ['recipientFirstName','recipientLastName']:
            df = df.withColumn(col_name, lit('-1'))
        elif col_name not in df.schema.fieldNames() and col_name in ['recipientFirstName','recipientLastName']:
            df = df.withColumn(col_name, lit(' '))
    
    df = df.na.fill(' ', subset = ['recipientFirstName','recipientLastName'])
    return df


dimQuesQuery = f"""select surveyQuestionSK,
                            surveyID surveyId,
                            CAST(ltrim('QID', surveyQuestionId) AS INTEGER) questionId,
                            surveyQuestionPartId questionPartId, 
                            concat_ws('-PartQ-', surveyQuestionText, surveyQuestionPartText)  QuestionText,       
                            concat_ws('','question',CAST(ltrim('QID', surveyQuestionId) AS INTEGER), CASE WHEN surveyQuestionPartId is null then '' else concat('Part', surveyQuestionPartId) END,'ResponseCode') as responseCodeColumn,
                            concat_ws('','question',CAST(ltrim('QID', surveyQuestionId) AS INTEGER), CASE WHEN surveyQuestionPartId is null then '' else concat('Part', surveyQuestionPartId) END,'ResponseText') as responseTextColumn,
                            concat_ws('','question',CAST(ltrim('QID', surveyQuestionId) AS INTEGER), 'SentimentDsc') as sentimentDscColumn,
                            concat_ws('','question',CAST(ltrim('QID', surveyQuestionId) AS INTEGER), 'SentimentPolarityNumber') as sentimentPolarityNumberColumn,
                            concat_ws('','question',CAST(ltrim('QID', surveyQuestionId) AS INTEGER), 'SentimentScore') as sentimentScoreColumn,
                            concat_ws('','question',CAST(ltrim('QID', surveyQuestionId) AS INTEGER), 'TopicsText') as topicsTextColumn,
                            concat_ws('','question',CAST(ltrim('QID', surveyQuestionId) AS INTEGER), 'TopicSentimentScore') as topicSentimentScoreColumn,
                            concat_ws('','question',CAST(ltrim('QID', surveyQuestionId) AS INTEGER), 'TopicsSentimentsLabel') as topicsSentimentsLabelColumn,
                            concat_ws('','question',CAST(ltrim('QID', surveyQuestionId) AS INTEGER), 'ParTopicsText') as parTopicsTextColumn
                            from {get_table_namespace(f'{TARGET}', 'dimSurveyQuestion')} 
                    """


df_dimQues = spark.sql(dimQuesQuery)

dsv = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimsurveyparticipant')}")
dsv = dsv.na.fill(' ', subset = ['surveyParticipantEmailRecipientFirstName','surveyParticipantEmailRecipientSurname'])
dsr = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimsurveyresponseinformation')}")
ds = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimsurvey')}")
dp  = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimProperty')}") 
dbp = GetTable(f"{get_table_namespace(f'{TARGET}', 'dimbusinesspartner')}")
#dpSKUC1 = str(spark.sql("Select propertySK from dimproperty where _businessKey = '-1'").collect()[0][0])
#dbppSKUC1 = str(spark.sql("Select businessPartnerSK from dimbusinesspartner where _businessKey = '-1'").collect()[0][0])
uc1DefaultSK = '60e35f602481e8c37d48f6a3e3d7c30d'

df_billpaid =  GetTable(f"{get_table_namespace(f'{SOURCE}', 'qualtrics_billpaidsuccessfullyresponses')}")
df_billpaid = add_missing_columns(df_billpaid, duplicate_columns) 


df_businessXconn =  GetTable(f"{get_table_namespace(f'{SOURCE}', 'qualtrics_businessconnectservicerequestcloseresponses')}")
df_businessXconn = add_missing_columns(df_businessXconn, duplicate_columns)

df_complaintsClosed  =  GetTable(f"{get_table_namespace(f'{SOURCE}', 'qualtrics_complaintscomplaintclosedresponses')}")
df_complaintsClosed = add_missing_columns(df_complaintsClosed, duplicate_columns)

df_contactCentreInteract  =  GetTable(f"{get_table_namespace(f'{SOURCE}', 'qualtrics_contactcentreinteractionmeasurementsurveyresponses')}")
df_contactCentreInteract = add_missing_columns(df_contactCentreInteract, duplicate_columns)

# df_Customercareresponses  =  GetTable(f"{get_table_namespace(f'{SOURCE}', 'qualtrics_customercareresponses')}")
# df_Customercareresponses = add_missing_columns(df_Customercareresponses, duplicate_columns)

df_devApplicationreceived  =  GetTable(f"{get_table_namespace(f'{SOURCE}', 'qualtrics_developerapplicationreceivedresponses')}")
df_devApplicationreceived = add_missing_columns(df_devApplicationreceived, duplicate_columns)

df_feedbacktabgolive  =  GetTable(f"{get_table_namespace(f'{SOURCE}', 'qualtrics_feedbacktabgoliveresponses')}")
df_feedbacktabgolive = add_missing_columns(df_feedbacktabgolive, duplicate_columns)

df_p4sonlinefeedback  =  GetTable(f"{get_table_namespace(f'{SOURCE}', 'qualtrics_p4sonlinefeedbackresponses')}")
df_p4sonlinefeedback = add_missing_columns(df_p4sonlinefeedback, duplicate_columns)

df_s73surveyresponse   =  GetTable(f"{get_table_namespace(f'{SOURCE}', 'qualtrics_s73surveyresponses')}")
df_s73surveyresponse = add_missing_columns(df_s73surveyresponse, duplicate_columns)

df_waterfixpost =  GetTable(f"{get_table_namespace(f'{SOURCE}', 'qualtrics_waterfixpostinteractionfeedbackresponses')}")
df_waterfixpost = add_missing_columns(df_waterfixpost, duplicate_columns)

df_websitegolive =  GetTable(f"{get_table_namespace(f'{SOURCE}', 'qualtrics_websitegoliveresponses')}")
df_websitegolive = add_missing_columns(df_websitegolive, duplicate_columns)

df_wscs73exp =  GetTable(f"{get_table_namespace(f'{SOURCE}', 'qualtrics_wscs73experiencesurveyresponses')}")
df_wscs73exp = add_missing_columns(df_wscs73exp, duplicate_columns)

billpaid_df  = transpose_df(df_billpaid, df_dimQues, duplicate_columns)
billpaid_df = (billpaid_df.join(dsv.filter(dsv._recordCurrent == 1), (billpaid_df["recipientEmail"] == dsv["surveyParticipantEmailRecipientId"]) & (billpaid_df["recipientFirstName"] == dsv["surveyParticipantEmailRecipientFirstName"]) 
                                      & (billpaid_df["recipientLastName"] == dsv["surveyParticipantEmailRecipientSurname"])
                                      & (billpaid_df["snapshotDate"].between(dsv["sourceValidFromTimestamp"],dsv["sourceValidToTimestamp"])), how = 'left')
                        .join(ds, (billpaid_df["surveyId"] == ds["surveyId"]) & (billpaid_df["snapshotDate"].between(ds["sourceValidFromTimestamp"],ds["sourceValidToTimestamp"])), how = 'left')  
                         .join(dsr.filter(dsr._recordCurrent == 1), (billpaid_df["surveyId"] == dsr["surveyId"]) & (billpaid_df["recordId"] == dsr["surveyResponseId"]), how = 'left') 
                         .join(dp.filter(dp._RecordCurrent == 1), (billpaid_df["propertyNumber"] == dp["propertyNumber"]), how = 'left') 
                         .select(billpaid_df["*"], dsv["surveyParticipantSK"], dsr["surveyResponseInformationSK"], dp["propertySK"],ds["surveySK"]) 
                         .withColumn("businessPartnerSK", lit(uc1DefaultSK)) 
                         .withColumn("sourceSystem", lit('Qualtrics')))

businessXconn_df = transpose_df(df_businessXconn, df_dimQues, duplicate_columns)
businessXconn_df = (businessXconn_df.join(dbp.filter(dbp._RecordCurrent == 1), (businessXconn_df["assignedTo"] == dbp["businessPartnerNumber"]), how = 'left') 
                                   .join(dsr.filter(dsr._recordCurrent == 1), (businessXconn_df["surveyId"] == dsr["surveyId"]) & (businessXconn_df["recordId"] == dsr["surveyResponseId"]), how = 'left')
                                   .join(dsv.filter(dsv._recordCurrent == 1), (businessXconn_df["recipientEmail"] == dsv["surveyParticipantEmailRecipientId"]) & (businessXconn_df["recipientFirstName"] == dsv["surveyParticipantEmailRecipientFirstName"]) 
                                      & (businessXconn_df["recipientLastName"] == dsv["surveyParticipantEmailRecipientSurname"]) & (businessXconn_df["snapshotDate"].between(dsv["sourceValidFromTimestamp"],dsv["sourceValidToTimestamp"])), how = 'left')
                                    .join(ds, (businessXconn_df["surveyId"] == ds["surveyId"]) & (businessXconn_df["snapshotDate"].between(ds["sourceValidFromTimestamp"],ds["sourceValidToTimestamp"])), how = 'left')
                                   .select(businessXconn_df["*"], dsv["surveyParticipantSK"], dbp["businessPartnerSK"], dsr["surveyResponseInformationSK"],ds["surveySK"]) 
                                   .withColumn("propertySK", lit(uc1DefaultSK)) 
                                   .withColumn("sourceSystem", lit('Qualtrics')))

complaintsClosed_df = transpose_df(df_complaintsClosed, df_dimQues, duplicate_columns)
complaintsClosed_df = (complaintsClosed_df.join(dsv.filter(dsv._recordCurrent == 1), (complaintsClosed_df["recipientEmail"] == dsv["surveyParticipantEmailRecipientId"]) 
                                      & (complaintsClosed_df["recipientFirstName"] == dsv["surveyParticipantEmailRecipientFirstName"]) 
                                      & (complaintsClosed_df["recipientLastName"] == dsv["surveyParticipantEmailRecipientSurname"])& (complaintsClosed_df["snapshotDate"].between(dsv["sourceValidFromTimestamp"],dsv["sourceValidToTimestamp"])), how = 'left')
                                    .join(ds, (complaintsClosed_df["surveyId"] == ds["surveyId"]) & (complaintsClosed_df["snapshotDate"].between(ds["sourceValidFromTimestamp"],ds["sourceValidToTimestamp"])), how = 'left')
                         .join(dsr.filter(dsr._recordCurrent == 1), (complaintsClosed_df["surveyId"] == dsr["surveyId"]) & (complaintsClosed_df["recordId"] == dsr["surveyResponseId"]), how = 'left') 
                         .join(dp.filter(dp._RecordCurrent == 1), (complaintsClosed_df["propertyNumber"] == dp["propertyNumber"]), how = 'left') 
                         .select(complaintsClosed_df["*"], dsv["surveyParticipantSK"], dsr["surveyResponseInformationSK"], dp["propertySK"],ds["surveySK"]) 
                         .withColumn("businessPartnerSK", lit(uc1DefaultSK)) 
                         .withColumn("sourceSystem", lit('Qualtrics')))


contactCentreInteract_df = transpose_df(df_contactCentreInteract, df_dimQues, duplicate_columns)
contactCentreInteract_df = (contactCentreInteract_df.join(dsr.filter(dsr._recordCurrent == 1), (contactCentreInteract_df["surveyId"] == dsr["surveyId"]) & 
                                                          (contactCentreInteract_df["recordId"] == dsr["surveyResponseId"]), how = 'left')
                                                    .join(dsv.filter(dsv._recordCurrent == 1), (contactCentreInteract_df["recipientEmail"] == dsv["surveyParticipantEmailRecipientId"]) 
                                                         & (contactCentreInteract_df["recipientFirstName"] == dsv["surveyParticipantEmailRecipientFirstName"]) 
                                                         & (contactCentreInteract_df["recipientLastName"] == dsv["surveyParticipantEmailRecipientSurname"])& (contactCentreInteract_df["snapshotDate"].between(dsv["sourceValidFromTimestamp"],dsv["sourceValidToTimestamp"])), how = 'left')
                                    .join(ds, (contactCentreInteract_df["surveyId"] == ds["surveyId"]) & (contactCentreInteract_df["snapshotDate"].between(ds["sourceValidFromTimestamp"],ds["sourceValidToTimestamp"])), how = 'left') 
                                                    .select(contactCentreInteract_df["*"], dsv["surveyParticipantSK"], dsr["surveyResponseInformationSK"],ds["surveySK"]) 
                                                    .withColumn("businessPartnerSK", lit(uc1DefaultSK))
                                                    .withColumn("propertySK", lit(uc1DefaultSK)) 
                                                    .withColumn("sourceSystem", lit('Qualtrics')))


# Customercareresponses_df = transpose_df(df_Customercareresponses, df_dimQues, duplicate_columns)
# Customercareresponses_df = Customercareresponses_df.join(dsv.filter(dsv._recordCurrent == 1), (Customercareresponses_df["recipientEmail"] == dsv["surveyParticipantEmailRecipientId"]) \
#                                                          & (Customercareresponses_df["recipientFirstName"] == dsv["surveyParticipantEmailRecipientFirstName"]) \
#                                                          & (Customercareresponses_df["recipientLastName"] == dsv["surveyParticipantEmailRecipientSurname"]), how = 'left') \
#                                                     .select(Customercareresponses_df["*"], dsv["surveyParticipantSK"]) \
#                                                     .withColumn("businessPartnerSK", lit(uc1DefaultSK)) \
#                                                     .withColumn("surveyResponseInformationSK", lit('-1')) \
#                                                     .withColumn("propertySK", lit(uc1DefaultSK)) \
#                                                     .withColumn("sourceSystem", lit('Qualtrics'))

waterfixpost_df = transpose_df(df_waterfixpost, df_dimQues, duplicate_columns)
waterfixpost_df = (waterfixpost_df.join(dsr.filter(dsr._recordCurrent == 1), (waterfixpost_df["surveyId"] == dsr["surveyId"]) & (waterfixpost_df["recordId"] == dsr["surveyResponseId"]), how = 'left')
                                  .join(dsv.filter(dsv._recordCurrent == 1), (waterfixpost_df["recipientEmail"] == dsv["surveyParticipantEmailRecipientId"]) 
                                                         & (waterfixpost_df["recipientFirstName"] == dsv["surveyParticipantEmailRecipientFirstName"]) 
                                                         & (waterfixpost_df["recipientLastName"] == dsv["surveyParticipantEmailRecipientSurname"])& (waterfixpost_df["snapshotDate"].between(dsv["sourceValidFromTimestamp"],dsv["sourceValidToTimestamp"])), how = 'left')
                                    .join(ds, (waterfixpost_df["surveyId"] == ds["surveyId"]) & (waterfixpost_df["snapshotDate"].between(ds["sourceValidFromTimestamp"],ds["sourceValidToTimestamp"])), how = 'left')
                                                    .select(waterfixpost_df["*"], dsv["surveyParticipantSK"], dsr["surveyResponseInformationSK"],ds["surveySK"]) 
                                                    .withColumn("businessPartnerSK", lit(uc1DefaultSK))                                                    
                                                    .withColumn("propertySK", lit(uc1DefaultSK)) 
                                                    .withColumn("sourceSystem", lit('Qualtrics')))

devApplicationreceived_df = transpose_df(df_devApplicationreceived, df_dimQues, duplicate_columns)
devApplicationreceived_df = (devApplicationreceived_df.join(dsr.filter(dsr._recordCurrent == 1), (devApplicationreceived_df["surveyId"] == dsr["surveyId"]) 
                                                            & (devApplicationreceived_df["recordId"] == dsr["surveyResponseId"]), how = 'left')
                                                      .join(dsv.filter(dsv._recordCurrent == 1), (devApplicationreceived_df["recipientEmail"] == dsv["surveyParticipantEmailRecipientId"]) 
                                                         & (devApplicationreceived_df["recipientFirstName"] == dsv["surveyParticipantEmailRecipientFirstName"]) 
                                                         & (devApplicationreceived_df["recipientLastName"] == dsv["surveyParticipantEmailRecipientSurname"])& (devApplicationreceived_df["snapshotDate"].between(dsv["sourceValidFromTimestamp"],dsv["sourceValidToTimestamp"])), how = 'left')
                                    .join(ds, (devApplicationreceived_df["surveyId"] == ds["surveyId"]) & (devApplicationreceived_df["snapshotDate"].between(ds["sourceValidFromTimestamp"],ds["sourceValidToTimestamp"])), how = 'left') 
                                                    .select(devApplicationreceived_df["*"], dsv["surveyParticipantSK"], dsr["surveyResponseInformationSK"],ds["surveySK"]) 
                                                     .withColumn("businessPartnerSK", lit(uc1DefaultSK))                                                      
                                                     .withColumn("propertySK", lit(uc1DefaultSK)) 
                                                     .withColumn("sourceSystem", lit('Qualtrics')))



p4sonlinefeedback_df = transpose_df(df_p4sonlinefeedback, df_dimQues, duplicate_columns)
p4sonlinefeedback_df = (p4sonlinefeedback_df.join(dsr.filter(dsr._recordCurrent == 1), (p4sonlinefeedback_df["surveyId"] == dsr["surveyId"]) & (p4sonlinefeedback_df["recordId"] == dsr["surveyResponseId"]), how = 'left')
                                            .join(dsv.filter(dsv._recordCurrent == 1), (p4sonlinefeedback_df["recipientEmail"] == dsv["surveyParticipantEmailRecipientId"]) 
                                                         & (p4sonlinefeedback_df["recipientFirstName"] == dsv["surveyParticipantEmailRecipientFirstName"]) 
                                                         & (p4sonlinefeedback_df["recipientLastName"] == dsv["surveyParticipantEmailRecipientSurname"])& (p4sonlinefeedback_df["snapshotDate"].between(dsv["sourceValidFromTimestamp"],dsv["sourceValidToTimestamp"])), how = 'left')
                                    .join(ds, (p4sonlinefeedback_df["surveyId"] == ds["surveyId"]) & (p4sonlinefeedback_df["snapshotDate"].between(ds["sourceValidFromTimestamp"],ds["sourceValidToTimestamp"])), how = 'left') 
                                                    .select(p4sonlinefeedback_df["*"], dsv["surveyParticipantSK"], dsr["surveyResponseInformationSK"],ds["surveySK"])
                                                     .withColumn("businessPartnerSK", lit(uc1DefaultSK))
                                                     .withColumn("propertySK", lit(uc1DefaultSK)) 
                                                     .withColumn("sourceSystem", lit('Qualtrics')))

s73surveyresponse_df = transpose_df(df_s73surveyresponse, df_dimQues, duplicate_columns)
s73surveyresponse_df = (s73surveyresponse_df.join(dsr.filter(dsr._recordCurrent == 1), (s73surveyresponse_df["surveyId"] == dsr["surveyId"]) & (s73surveyresponse_df["recordId"] == dsr["surveyResponseId"]), how = 'left')
                                            .join(dsv.filter(dsv._recordCurrent == 1), (s73surveyresponse_df["recipientEmail"] == dsv["surveyParticipantEmailRecipientId"]) 
                                                         & (s73surveyresponse_df["recipientFirstName"] == dsv["surveyParticipantEmailRecipientFirstName"]) 
                                                         & (s73surveyresponse_df["recipientLastName"] == dsv["surveyParticipantEmailRecipientSurname"])& (s73surveyresponse_df["snapshotDate"].between(dsv["sourceValidFromTimestamp"],dsv["sourceValidToTimestamp"])), how = 'left')
                                    .join(ds, (s73surveyresponse_df["surveyId"] == ds["surveyId"]) & (s73surveyresponse_df["snapshotDate"].between(ds["sourceValidFromTimestamp"],ds["sourceValidToTimestamp"])), how = 'left') 
                                                    .select(s73surveyresponse_df["*"], dsv["surveyParticipantSK"], dsr["surveyResponseInformationSK"],ds["surveySK"])
                                                     .withColumn("businessPartnerSK", lit(uc1DefaultSK)) 
                                                     .withColumn("propertySK", lit(uc1DefaultSK)) 
                                                     .withColumn("sourceSystem", lit('Qualtrics')))


websitegolive_df = transpose_df(df_websitegolive, df_dimQues, duplicate_columns)
websitegolive_df = (websitegolive_df.join(dsr.filter(dsr._recordCurrent == 1), (websitegolive_df["surveyId"] == dsr["surveyId"]) & (websitegolive_df["recordId"] == dsr["surveyResponseId"]), how = 'left')
                                    .join(dsv.filter(dsv._recordCurrent == 1), (websitegolive_df["recipientEmail"] == dsv["surveyParticipantEmailRecipientId"]) 
                                                         & (websitegolive_df["recipientFirstName"] == dsv["surveyParticipantEmailRecipientFirstName"]) 
                                                         & (websitegolive_df["recipientLastName"] == dsv["surveyParticipantEmailRecipientSurname"])& (websitegolive_df["snapshotDate"].between(dsv["sourceValidFromTimestamp"],dsv["sourceValidToTimestamp"])), how = 'left')
                                    .join(ds, (websitegolive_df["surveyId"] == ds["surveyId"]) & (websitegolive_df["snapshotDate"].between(ds["sourceValidFromTimestamp"],ds["sourceValidToTimestamp"])), how = 'left') 
                                                    .select(websitegolive_df["*"], dsv["surveyParticipantSK"], dsr["surveyResponseInformationSK"],ds["surveySK"])
                                                     .withColumn("businessPartnerSK", lit(uc1DefaultSK)) 
                                                     .withColumn("propertySK", lit(uc1DefaultSK)) 
                                                     .withColumn("sourceSystem", lit('Qualtrics')))


wscs73exp_df = transpose_df(df_wscs73exp, df_dimQues, duplicate_columns)
wscs73exp_df = (wscs73exp_df.join(dsr.filter(dsr._recordCurrent == 1), (wscs73exp_df["surveyId"] == dsr["surveyId"]) & (wscs73exp_df["recordId"] == dsr["surveyResponseId"]), how = 'left')
                            .join(dsv.filter(dsv._recordCurrent == 1), (wscs73exp_df["recipientEmail"] == dsv["surveyParticipantEmailRecipientId"]) 
                                                         & (wscs73exp_df["recipientFirstName"] == dsv["surveyParticipantEmailRecipientFirstName"]) 
                                                         & (wscs73exp_df["recipientLastName"] == dsv["surveyParticipantEmailRecipientSurname"])& (wscs73exp_df["snapshotDate"].between(dsv["sourceValidFromTimestamp"],dsv["sourceValidToTimestamp"])), how = 'left')
                                    .join(ds, (wscs73exp_df["surveyId"] == ds["surveyId"]) & (wscs73exp_df["snapshotDate"].between(ds["sourceValidFromTimestamp"],ds["sourceValidToTimestamp"])), how = 'left') 
                                                    .select(wscs73exp_df["*"], dsv["surveyParticipantSK"], dsr["surveyResponseInformationSK"],ds["surveySK"])
                                                     .withColumn("businessPartnerSK", lit(uc1DefaultSK)) 
                                                     .withColumn("propertySK", lit(uc1DefaultSK)) 
                                                     .withColumn("sourceSystem", lit('Qualtrics'))) 
                                                     


feedbacktabgolive_df = transpose_feedback_df(df_feedbacktabgolive, df_dimQues, duplicate_columns)
feedbacktabgolive_df = (feedbacktabgolive_df.join(dsr.filter(dsr._recordCurrent == 1), (feedbacktabgolive_df["surveyId"] == dsr["surveyId"]) & (feedbacktabgolive_df["recordId"] == dsr["surveyResponseId"]), how = 'left')
                                            .join(dsv.filter(dsv._recordCurrent == 1), (feedbacktabgolive_df["recipientEmail"] == dsv["surveyParticipantEmailRecipientId"]) 
                                                         & (feedbacktabgolive_df["recipientFirstName"] == dsv["surveyParticipantEmailRecipientFirstName"]) 
                                                         & (feedbacktabgolive_df["recipientLastName"] == dsv["surveyParticipantEmailRecipientSurname"])& (feedbacktabgolive_df["snapshotDate"].between(dsv["sourceValidFromTimestamp"],dsv["sourceValidToTimestamp"])), how = 'left')
                                    .join(ds, (feedbacktabgolive_df["surveyId"] == ds["surveyId"]) & (feedbacktabgolive_df["snapshotDate"].between(ds["sourceValidFromTimestamp"],ds["sourceValidToTimestamp"])), how = 'left') 
                                                    .select(feedbacktabgolive_df["*"], dsv["surveyParticipantSK"], dsr["surveyResponseInformationSK"],ds["surveySK"])
                                                     .withColumn("businessPartnerSK", lit(uc1DefaultSK)) 
                                                     .withColumn("propertySK", lit(uc1DefaultSK)) 
                                                     .withColumn("sourceSystem", lit('Qualtrics')))
                                                     

# .union(Customercareresponses_df) 
finaldf = (billpaid_df.unionByName(businessXconn_df) 
                     .unionByName(complaintsClosed_df) 
                     .unionByName(contactCentreInteract_df)                      
                     .unionByName(waterfixpost_df) 
                     .unionByName(devApplicationreceived_df) 
                     .unionByName(p4sonlinefeedback_df) 
                     .unionByName(s73surveyresponse_df) 
                     .unionByName(websitegolive_df) 
                     .unionByName(wscs73exp_df) 
                     .unionByName(feedbacktabgolive_df))


#finaldf.count()

# COMMAND ----------

#########Added CRM Survey #############################Ignore surveyId 'Z_BILLASSIST_SURVEY' for now as in contains Sensitive info
dimBuss = GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'dimSurvey')}")
# split_col = split(col("sourceBusinessKey"), r"\|")
# dimBuss = dimBuss.withColumn("surveyId", split_col.getItem(1)).withColumn("SourceSystem", split_col.getItem(0)).filter(col("SourceSystem") == 'CRM').select(col("surveyId")).distinct().collect()
#print(dimBuss)

crm_0crm_srv_req_inci_h_df = GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_0crm_srv_req_inci_h')}")
crm_crmd_link_df = GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_crmd_link')}")
crm_crmd_survey_df = GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_crmd_survey')}")
crm_crm_svy_db_sv_df = GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_crm_svy_db_sv')}")
crm_crm_svy_re_quest_df = GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_crm_svy_re_quest')}").filter(col("surveyID") != lit('Z_BILLASSIST_SURVEY'))
crm_crm_svy_db_s_df = GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_crm_svy_db_s')}").filter(col("surveyID") != lit('Z_BILLASSIST_SURVEY'))
crm_0svy_qstnnr_text_df = GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_0svy_qstnnr_text')}")
crm_0svy_qstnnr_text_df2 = GetTable(f"{get_table_namespace(f'{SOURCE}', 'crm_0svy_quest_text')}")
dimQuestion_df = GetTable(f"{get_table_namespace(f'{DEFAULT_TARGET}', 'dimSurveyQuestion')}").filter(col("surveyID") != lit('Z_BILLASSIST_SURVEY'))


max_svv = crm_crm_svy_db_sv_df.groupBy("surveyValuesGUID").agg(max(col("surveyValuesVersion")).alias("max_surveyValuesVersion"))
sv1 = (crm_crm_svy_db_sv_df.alias("sv1")
                           .join(max_svv.alias("sv2"), col("sv1.surveyValuesGUID") == col("sv2.surveyValuesGUID"))
                           .filter(col("sv1.surveyValuesVersion") == col("sv2.max_surveyValuesVersion"))
                           .select(col("sv1.*"), split(col("sv1.surveyValueKeyAttribute"), "/") [0].alias("questionId"))
                           .withColumn("snapshotDate",expr("to_timestamp(CAST(delta_ts as String) ,'yyyyMMddHHmmss')")) )

#sv1.count()


joined_df = (crm_0crm_srv_req_inci_h_df.alias("I")
    .join(crm_crmd_link_df.alias("L"), (col("I.serviceRequestGUID") == col("L.hiGUID")) & (col("L.setObjectType") == 58))
    .join(crm_crmd_survey_df.alias("S"), col("S.setGUID") == col("L.setGUID") )
    .join(sv1.alias("SV"), col("SV.surveyValuesGUID") == col("S.surveyValuesGUID"))
    .join(crm_crm_svy_re_quest_df.alias("R"), (col("SV.questionId") == col("R.questionId")) & (col("SV.surveyValuesVersion") == col("R.surveyVersion")))
    .join(crm_crm_svy_db_s_df.alias("SDB"), (col("R.surveyId") == col("SDB.surveyId")) & (col("R.surveyVersion") == col("SDB.surveyVersion")))
    .join(crm_0svy_qstnnr_text_df.alias("Q"), col("Q.questionnaireId") == col("R.questionnaire"))
    .join(crm_0svy_qstnnr_text_df2.alias("QT"), (col("QT.questionnaireId") == col("Q.questionnaireId")) & (col("QT.surveyQuestionId") == col("R.questionId")))
    .filter(col("R.surveyId").isNotNull())
    .join(dimQuestion_df.alias("DQ"), (col("R.surveyId") == col("DQ.surveyId")) & (col("R.questionId") == col("DQ.surveyQuestionId")) 
                                                                                & (col("R.surveyVersion") == col("DQ.surveyVersionNumber")) 
                                                                                & (col("DQ.sourceSystemCode") == lit("CRM")), how = 'left')
    .join(dsr.alias("d"), (col("R.surveyId") == col("d.surveyId")) & (col("SV.surveyValuesGUID") == col("d.surveyResponseId")) ,how = 'left').filter(col("d._recordCurrent") ==1)
    .join(dimBuss.filter("sourceSystemCode == 'CRM'").alias('crms'),(col("crms.surveyId") == col("DQ.surveyId")) & (col("SV.snapshotDate").between(col("crms.sourceValidFromTimestamp"),col("crms.sourceValidToTimestamp"))),how='left')
    
)


#  col("I.requestStartDate").alias("requestStartDate"),
                                #  col("I.serviceRequestId").alias("serviceRequestId"),
                                #  col("R.questionnaire").alias("questionnaire"),
                                #  col("R.surveyVersion").alias("surveyVersion"),
                                #  col("R.questionSequenceNumber").alias("questionSequenceNumber"),
                                #  col("QT.questionLong").alias("questionLong")   
                                #                               
joined_df = (joined_df.select (col("R.surveyId").alias("surveyId"),
                               col("Q.questionnaireLong").alias("surveyName"),
                               col("SV.surveyValuesGUID").alias("recordId"),  
                               col('snapshotDate'),                               
                               col("QT.surveyQuestionId").alias("questionId"),
                               col("SV.surveyValueAttribute").alias("responseText"),
                               col("DQ.surveyQuestionSK").alias("surveyQuestionSK"),
                               col("crms.surveySK").alias("surveySK"), 
                               col("d.surveyResponseInformationSK").alias("surveyResponseInformationSK")                                
                              ).withColumn("questionPartId", lit(None).cast("string"))
                                .withColumn("questionText", lit(None).cast("string"))
                                .withColumn("responseCode", lit(None).cast("string"))
                                .withColumn("sentimentDsc", lit(None).cast("string"))
                                .withColumn("sentimentPolarityNumber", lit(None).cast("string"))
                                .withColumn("sentimentScore", lit(None).cast("string"))
                                .withColumn("topicsText", lit(None).cast("string"))
                                .withColumn("topicSentimentScore", lit(None).cast("string"))
                                .withColumn("topicsSentimentsLabel", lit(None).cast("string"))
                                .withColumn("parTopicsText", lit(None).cast("string"))
                                .withColumn("recipientEmail", lit(None).cast("string"))
                                .withColumn("recipientFirstName", lit(None).cast("string"))
                                .withColumn("recipientLastName", lit(None).cast("string"))
                                .withColumn("propertyNumber", lit(None).cast("string"))
                                .withColumn("assignedTo", lit(None).cast("string"))
                                .withColumn("businessPartnerSK", lit(uc1DefaultSK)) 
                                # .withColumn("surveyResponseInformationSK", lit('-1')) 
                                .withColumn("propertySK", lit(uc1DefaultSK)) 
                                .withColumn("sourceSystem", lit('CRM')) 
                                .withColumn("surveyParticipantSK", lit('-1'))
            )

#joined_df.count()
finaldf = finaldf.unionByName(joined_df)
#print(finaldf.count())

############################no need to loop ###################################
# filtered_dfs = []

# for row in dimBuss:
#     surveyId = row["surveyId"]
#     filtered_df = joined_df.filter(col("surveyId") == surveyId)
#     filtered_dfs.append(filtered_df)


# final_crm_list_df = filtered_dfs[0]
# for i in range(1, len(filtered_dfs)):
#     final_crm_list_df = final_crm_list_df.unionByName(filtered_dfs[i])

# print(joined_df.count())
# print(final_crm_list_df.count())
#finaldf = finaldf.unionByName(final_crm_list_df)
# print(finaldf.count())

# COMMAND ----------

def Transform():
    global df_final
    df_final = finaldf

    # ------------- TRANSFORMS ------------- # 
    _.Transforms = [
        f"concat_ws('|',sourceSystem,surveyId,questionId,surveyQuestionSK,recordId,responseText,snapshotDate)  {BK}"
        ,"propertySK propertyNumberFK" 
        ,"businessPartnerSK businessPartnerNumberFK"
        ,"surveySK surveyFK"
        ,"surveyQuestionSK surveyQuestionFK"
        ,"surveyResponseInformationSK surveyResponseInformationFK" 
        ,"surveyParticipantSK surveyParticipantFK"
        ,"surveyId"
        ,"questionId surveyquestionId" 
        ,"questionPartId" 
        ,"recordId responseId" 
        ,"responseCode" 
        ,"responseText"
        ,"sentimentDsc sentimentDescription" 
        ,"sentimentPolarityNumber sentimentPolarityNumber" 
        ,"sentimentScore"         
        ,"topicSentimentScore" 
        ,"topicsSentimentsLabel topicSentimentLabel" 
        ,"topicsText" 
        ,"parTopicsText"               
        ,"sourceSystem sourceSystemCode"
        ,"snapshotDate snapshotTimestamp"
    ]
    
    df_final = df_final.selectExpr(
        _.Transforms
    ).drop_duplicates()

    # ------------- SAVE ------------------- #
    #df_final.display()
    Save(df_final)
    #DisplaySelf()
Transform() 

# COMMAND ----------

3598801
1732925

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ppd_curated.dim.surveyresponseinformation_test where surveyResponseId = 'R_es9IqQxcTuUzFyV'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ppd_curated.fact.surveyresponse_test where responseId  = 'R_1Nfs20vsRyc86SE' and questionPartId = '3' --Duplicated defect

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ppd_curated.fact.surveyresponse_test where responseId  = 'R_es9IqQxcTuUzFyV' -- data quality defect

# COMMAND ----------

# MAGIC %sql
# MAGIC select surveyResponseSK, count(1) from ppd_curated.fact.surveyresponse group by all having count(1)>1

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ppd_curated.fact.surveyresponse where surveyResponseSK = '1bd49fdd99c95770bd058503b65edba6'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ppd_curated.dim.surveyresponseinformation_test where surveyId = 'SV_89cmPdrG4sKNBvU' and surveyResponseId = 'R_2P50QI6CAXZSvoD'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ppd_curated.dim.surveyresponseinformation where surveyId = 'SV_89cmPdrG4sKNBvU' and surveyResponseId = 'R_2P50QI6CAXZSvoD'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ppd_curated.fact.surveyresponse_test where sourceSystemCode = 'Qualtrics' 

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table ppd_curated.fact.surveyresponse rename to ppd_curated.fact.surveyresponse_backup

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from 
# MAGIC ((select * from ppd_cleansed.qualtrics.billpaidsuccessfullyresponses)
# MAGIC union (select * from ppd_cleansed.qualtrics.businessConnectServiceRequestCloseResponses)
# MAGIC union (select * from ppd_cleansed.qualtrics.complaintsComplaintClosedResponses)
# MAGIC union (select * from ppd_cleansed.qualtrics.contactcentreinteractionmeasurementsurveyResponses)
# MAGIC union (select * from ppd_cleansed.qualtrics.customercareResponses)
# MAGIC union (select * from ppd_cleansed.qualtrics.developerapplicationreceivedResponses)
# MAGIC union (select * from ppd_cleansed.qualtrics.p4sonlinefeedbackResponses)
# MAGIC union (select * from ppd_cleansed.qualtrics.s73surveyResponses)
# MAGIC union (select * from ppd_cleansed.qualtrics.waterfixpostinteractionfeedbackResponses)
# MAGIC union (select * from ppd_cleansed.qualtrics.websitegoliveResponses)
# MAGIC union (select * from ppd_cleansed.qualtrics.wscs73experiencesurveyResponses)
# MAGIC union (select * from ppd_cleansed.qualtrics.feedbacktabgoliveResponses)) where recordId = 'R_es9IqQxcTuUzFyV'

# COMMAND ----------

# MAGIC %sql
# MAGIC select recordId,surveyName  
# MAGIC
# MAGIC from
# MAGIC
# MAGIC (
# MAGIC
# MAGIC select recordId, 'billpaidsuccessfullyresponses' as surveyName
# MAGIC
# MAGIC from ppd_cleansed.qualtrics.billpaidsuccessfullyresponses ql
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select recordId, 'businessconnectservicerequestcloseresponses' as surveyName
# MAGIC
# MAGIC from ppd_cleansed.qualtrics.businessconnectservicerequestcloseresponses ql
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select recordId ,'complaintscomplaintclosedresponses' as surveyName
# MAGIC
# MAGIC from ppd_cleansed.qualtrics.complaintscomplaintclosedresponses ql
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select recordId, 'contactcentreinteractionmeasurementsurveyresponses' as surveyName
# MAGIC
# MAGIC from ppd_cleansed.qualtrics.contactcentreinteractionmeasurementsurveyresponses ql
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select recordId, 'customercareresponses' as surveyName
# MAGIC
# MAGIC from ppd_cleansed.qualtrics.customercareresponses ql
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select recordId, 'daftestsurveyresponses' as surveyName
# MAGIC
# MAGIC from ppd_cleansed.qualtrics.daftestsurveyresponses ql
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select recordId, 'developerapplicationreceivedresponses'
# MAGIC
# MAGIC from ppd_cleansed.qualtrics.developerapplicationreceivedresponses ql
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select recordId, 'feedbacktabgoliveresponses' as surveyName
# MAGIC
# MAGIC from ppd_cleansed.qualtrics.feedbacktabgoliveresponses ql
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select recordId, 'p4sonlinefeedbackresponses' as surveyName
# MAGIC
# MAGIC from ppd_cleansed.qualtrics.p4sonlinefeedbackresponses ql
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select recordId, 's73surveyresponses' as surveyName
# MAGIC
# MAGIC from ppd_cleansed.qualtrics.s73surveyresponses ql
# MAGIC
# MAGIC )dt
# MAGIC
# MAGIC where recordId = 'R_es9IqQxcTuUzFyV'
# MAGIC
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC
# MAGIC from ppd_cleansed.qualtrics.contactcentreinteractionmeasurementsurveyresponses where  recordId = 'R_es9IqQxcTuUzFyV'
# MAGIC

# COMMAND ----------


