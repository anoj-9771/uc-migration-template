# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

#####Determine Load #################
###############################
driverTable1 = 'curated.fact.customerservicerequest'   

if not(TableExists(_.Destination)):
    isDeltaLoad = False
    #####Table Full Load #####################
    derivedDF1 = GetTable(f"{getEnv()}{driverTable1}").withColumn("_change_type", lit(None))
else:
    #####CDF for eligible tables#####################
    isDeltaLoad = True
    derivedDF1 = getSourceCDF(driverTable1, None, False)
    derivedDF1.createOrReplaceTempView("derivedDF1Table") 
    if derivedDF1.count() == 0:
        print("No delta to be  processed")
        #dbutils.notebook.exit(f"no CDF to process for table for source {driverTable1} and {driverTable2} -- Destination {_.Destination}") 

# COMMAND ----------

from pyspark.sql.functions import col

# ------------- TABLES ----------------- #    
factSurveyMisc_df = GetTable(f"{getEnv()}curated.fact.surveymiscellaneousinformation").filter(col("surveyAttributeName") == lit('serviceRequestNumber')).select("surveyResponseInformationFK", "surveyAttributeValue").alias('svyInfo')    
factServiceRequest_df = derivedDF1.select("customerServiceRequestId", "customerServiceRequestSK", "_change_type").alias('SR')    
dimSuveyResp_df = GetTable(f"{getEnv()}curated.dim.surveyresponseinformation").withColumn("relationshipType", lit("Service Request - Survey")).select("surveyResponseInformationSK", "surveyResponseId", "relationshipType").alias('DSI')

    
# ------------- JOINS ------------------ #
serviceReqSurvey_df = ((
        factSurveyMisc_df
          .join(factServiceRequest_df, expr("svyInfo.surveyAttributeValue = sr.customerServiceRequestId"), "Inner")  
           .join(dimSuveyResp_df, expr("dsi.surveyResponseInformationSK = svyInfo.surveyResponseInformationFK"),"Inner")          
    ).selectExpr ("customerServiceRequestSK" 
                 ,"surveyResponseInformationFK"
                 ,"customerServiceRequestId"
                 ,"surveyResponseId"
                 ,"relationshipType as customerServiceRelationshipTypeName"
                 , "_change_type")
                 
             )

crmDF = spark.sql(f"""Select distinct  SR.customerServiceRequestSK customerServiceRequestSK,  
                                   dsi.surveyResponseInformationSK surveyResponseInformationFK, 
                                   I.serviceRequestID customerServiceRequestId,
                                    SV.surveyValuesGUID as surveyResponseId, 
                                    'Customer Request - Survey' as customerServiceRelationshipTypeName,
                                    'insert' _change_type
                            from  {getEnv()}cleansed.crm.0crm_srv_req_inci_h I  
                            INNER JOIN {getEnv()}cleansed.crm.crmd_link L on I.serviceRequestGUID = L.hiGUID and setobjecttype = 58
                            INNER JOIN {getEnv()}cleansed.crm.crmd_survey S on S.setGUID = L.setGUID
                            INNER JOIN (Select * , split_part(surveyValueKeyAttribute, '/',1) as questionID from {getEnv()}cleansed.crm.crm_svy_db_sv SV1 
                                         where surveyValuesVersion = (Select max(surveyValuesVersion) from {getEnv()}cleansed.crm.crm_svy_db_sv 
                                         where surveyValuesGUID = SV1.surveyValuesGUID )) SV on SV.surveyValuesGUID = S.surveyValuesGuid
                            INNER JOIN {getEnv()}cleansed.crm.crm_svy_re_quest R ON R.questionID = SV.questionID and SV.surveyValuesVersion = R.surveyVersion  
                            INNER JOIN {getEnv()}cleansed.crm.crm_svy_db_s SDB on R.surveyID = SDB.surveyID and R.surveyVersion = SDB.surveyVersion
                            INNER JOIN derivedDF1Table SR on sr.customerServiceRequestId = I.serviceRequestId
                            INNER JOIN {getEnv()}curated.dim.surveyresponseinformation dsi on dsi._businessKey = concat('CRM','|',SDB.surveyID,'|',SV.surveyValuesGUID) """)    

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
        ,"_change_type"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    #CleanSelf()
    SaveWithCDF(df, 'APPEND') #Save(df)
#     DisplaySelf()
#pass
Transform()
