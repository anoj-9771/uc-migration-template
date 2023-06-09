# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ---------- 

# MAGIC %run ../../Common/common-helpers 
# COMMAND ---------- 


# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

from pyspark.sql.functions import col

# ------------- TABLES ----------------- #    
factSurveyMisc_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'factsurveymiscellaneousinformation')}").filter(col("surveyAttributeName") == lit('serviceRequestNumber')).select("surveyResponseInformationFK", "surveyAttributeValue").alias('svyInfo')    
factServiceRequest_df = GetTable(f"{get_table_namespace(f'{TARGET}', 'factcustomerservicerequest')}").select("customerServiceRequestId", "customerServiceRequestSK").alias('SR')    
dimSuveyResp_df = GetTable(f"{TARGET}.dimsurveyresponseinformation").withColumn("relationshipType", lit("Service Request - Survey")).select("surveyResponseInformationSK", "surveyResponseId", "relationshipType").alias('DSI')

    
# ------------- JOINS ------------------ #
serviceReqSurvey_df = ((
        factSurveyMisc_df
          .join(factServiceRequest_df, expr("svyInfo.surveyAttributeValue = sr.customerServiceRequestId"), "Inner")  
           .join(dimSuveyResp_df, expr("dsi.surveyResponseInformationSK = svyInfo.surveyResponseInformationFK"),"Inner")          
    ).selectExpr ("customerServiceRequestSK" 
                 ,"surveyResponseInformationFK"
                 ,"customerServiceRequestId"
                 ,"surveyResponseId"
                 ,"relationshipType as customerServiceRelationshipTypeName")
         
             )

crmDF = spark.sql(f"""Select distinct  SR.customerServiceRequestSK customerServiceRequestSK,  
                                   dsi.surveyResponseInformationSK surveyResponseInformationFK, 
                                   I.serviceRequestID customerServiceRequestId,
                                    SV.surveyValuesGUID as surveyResponseId, 
                                    'Customer Request - Survey' as customerServiceRelationshipTypeName
                            from  {get_table_namespace('cleansed', 'crm_0crm_srv_req_inci_h')} I  
                            INNER JOIN {get_table_namespace('cleansed', 'crm_crmd_link')} L on I.serviceRequestGUID = L.hiGUID and setobjecttype = 58
                            INNER JOIN {get_table_namespace('cleansed', 'crm_crmd_survey')} S on S.setGUID = L.setGUID
                            INNER JOIN (Select * , split_part(surveyValueKeyAttribute, '/',1) as questionID from {get_table_namespace('cleansed', 'crm_crm_svy_db_sv')} SV1 
                                         where surveyValuesVersion = (Select max(surveyValuesVersion) from {get_table_namespace('cleansed', 'crm_crm_svy_db_sv')} 
                                         where surveyValuesGUID = SV1.surveyValuesGUID )) SV on SV.surveyValuesGUID = S.surveyValuesGuid
                            INNER JOIN {get_table_namespace('cleansed', 'crm_crm_svy_re_quest')} R ON R.questionID = SV.questionID and SV.surveyValuesVersion = R.surveyVersion  
                            INNER JOIN {get_table_namespace('cleansed', 'crm_crm_svy_db_s')} SDB on R.surveyID = SDB.surveyID and R.surveyVersion = SDB.surveyVersion
                            INNER JOIN curated_v3.factcustomerservicerequest SR on sr.customerServiceRequestId = I.serviceRequestId
                            INNER JOIN  curated_v3.dimsurveyresponseinformation dsi on dsi._businessKey = concat('CRM','|',SDB.surveyID,'|',SV.surveyValuesGUID) """)
     

# COMMAND ----------

def Transform():
    global df 

    df = serviceReqSurvey_df.unionByName(crmDF)  
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"customerServiceRequestSK||'|'||surveyResponseInformationFK {BK}"        
        ,"customerServiceRequestSK customerServiceRequestFK"
        ,"surveyResponseInformationFK surveyResponseInformationFK"
        ,"customerServiceRequestId"
        ,"surveyResponseId"
        ,"customerServiceRelationshipTypeName"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    #CleanSelf()
    Save(df)
#     DisplaySelf()
#pass
Transform()
