# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

# MAGIC %run ../../Common/common-helpers 

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

from pyspark.sql.functions import col

# ------------- TABLES ----------------- #    
factSurveyMisc_df = (GetTable(f"{get_table_namespace(f'{TARGET}', 'factsurveymiscellaneousinformation')}")
                            .filter(upper(col("surveyAttributeName")) == lit('OBJECTID'))
                            .select(col("surveyResponseInformationFK"), 
                                    col("surveyAttributeValue")).alias('svyInfo'))
                            

factCustInter_df = (GetTable(f"{get_table_namespace(f'{TARGET}', 'factcustomerInteraction')}")
                    .select(col("customerInteractionId").cast('long').alias('customerInteractionNo'), 
                            col("customerInteractionId"),
                            col("customerInteractionSK")).alias('CI')) 



dimSuveyResp_df = (GetTable(f"{get_table_namespace(f'{TARGET}', 'dimsurveyresponseinformation')}")
                       .withColumn("relationshipType", lit('Customer Request - Interaction'))
                       .select("surveyResponseInformationSK", 
                               "surveyResponseId", 
                               "relationshipType").alias('DSI'))

    
# ------------- JOINS ------------------ #
serviceIntSurvey_df = ((
        factSurveyMisc_df
          .join(factCustInter_df, expr("svyInfo.surveyAttributeValue = ci.customerInteractionNo"), "Inner")  
           .join(dimSuveyResp_df, expr("dsi.surveyResponseInformationSK = svyInfo.surveyResponseInformationFK"),"Inner")          
    ).selectExpr ("customerInteractionSK" 
                 ,"surveyResponseInformationFK"
                 ,"customerInteractionId"
                 ,"surveyResponseId"
                 ,"relationshipType as customerServiceRelationshipTypeName")
         
             )    

# COMMAND ----------

def Transform():
    global df 

    df = serviceIntSurvey_df
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"customerInteractionSK||'|'||surveyResponseInformationFK {BK}"        
        ,"customerInteractionSK customerServiceRequestFK"
        ,"surveyResponseInformationFK surveyResponseInformationFK"
        ,"customerInteractionId"
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
