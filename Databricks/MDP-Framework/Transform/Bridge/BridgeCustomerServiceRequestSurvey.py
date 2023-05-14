# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

from pyspark.sql.functions import col
def Transform():
    global df 

    # ------------- TABLES ----------------- #    
    factSurveyMisc_df = GetTable(f"{TARGET}.factsurveymiscellaneousinformation").filter(col("surveyAttributeName") == lit('serviceRequestNumber')).select("surveyResponseInformationFK", "surveyAttributeValue").alias('svyInfo')    
    factServiceRequest_df = GetTable(f"{TARGET}.factcustomerservicerequest").select("customerServiceRequestId", "customerServiceRequestSK").alias('SR')    
    dimSuveyResp_df = GetTable(f"{TARGET}.dimsurveyresponseinformation").withColumn("relationshipType", lit("Service Request - Survey")).select("surveyResponseInformationSK", "surveyResponseId", "relationshipType").alias('DSI')
   
    # ------------- JOINS ------------------ #
    serviceReqSurvey_df = (
        factSurveyMisc_df
          .join(factServiceRequest_df, expr("svyInfo.surveyAttributeValue = sr.customerServiceRequestId"), "Inner")  
          .join(dimSuveyResp_df, expr("dsi.surveyResponseInformationSK = svyInfo.surveyResponseInformationFK"),"Inner")          
    ) 
    df = serviceReqSurvey_df   
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"customerServiceRequestSK||'|'||surveyResponseInformationFK {BK}"        
        ,"customerServiceRequestSK customerServiceRequestFK"
        ,"surveyResponseInformationFK surveyResponseInformationFK"
        ,"customerServiceRequestId"
        ,"surveyResponseId"
        ,"relationshipType customerServiceRelationshipTypeName"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
#     CleanSelf()
    Save(df)
#     DisplaySelf()
#pass
Transform()
