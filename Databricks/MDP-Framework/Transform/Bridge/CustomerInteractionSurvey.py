# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

#####Determine Load #################
###############################
driverTable1 = 'curated.fact.customerinteraction'   

if not(TableExists(_.Destination)):
    isDeltaLoad = False
    #####Table Full Load #####################
    derivedDF1 = GetTable(f"{getEnv()}{driverTable1}").withColumn("_change_type", lit(None))
else:
    #####CDF for eligible tables#####################
    isDeltaLoad = True
    derivedDF1 = getSourceCDF(driverTable1, None, False)
    if derivedDF1.count() == 0:
        print("No delta to be  processed")
        #dbutils.notebook.exit(f"no CDF to process for table for source {driverTable1} and {driverTable2} -- Destination {_.Destination}") 

# COMMAND ----------

from pyspark.sql.functions import col

# ------------- TABLES ----------------- #    
factSurveyMisc_df = (GetTable(f"{getEnv()}curated.fact.surveymiscellaneousinformation")
                            .filter(upper(col("surveyAttributeName")) == lit('OBJECTID'))
                            .select(col("surveyResponseInformationFK"), 
                                    col("surveyAttributeValue")).alias('svyInfo'))
                            

factCustInter_df = (derivedDF1
                    .select(col("customerInteractionId").cast('long').alias('customerInteractionNo'), 
                            col("customerInteractionId"),
                            col("customerInteractionSK"),
                            col("_change_type")).alias('CI')) 



dimSuveyResp_df = (GetTable(f"{getEnv()}curated.dim.surveyresponseinformation")
                       .withColumn("relationshipType", lit('Customer Request - Interaction'))
                       .select("surveyResponseInformationSK", 
                               "surveyResponseId", 
                               "relationshipType").alias('DSI'))

    
# ------------- JOINS ------------------ #
serviceIntSurvey_df = (( 
         factCustInter_df
          .join(factSurveyMisc_df, expr("ci.customerInteractionNo = svyInfo.surveyAttributeValue"), "Inner")  
           .join(dimSuveyResp_df, expr("dsi.surveyResponseInformationSK = svyInfo.surveyResponseInformationFK"),"Inner")          
    ).selectExpr ("customerInteractionSK" 
                 ,"surveyResponseInformationFK"
                 ,"customerInteractionId"
                 ,"surveyResponseId"
                 ,"relationshipType as customerServiceRelationshipTypeName"
                 ,"_change_type" )
                )    

# COMMAND ----------

def Transform():
    global df 

    df = serviceIntSurvey_df
    # ------------- TRANSFORMS ------------- #
      
    _.Transforms = [
         f"customerInteractionSK||'|'||surveyResponseInformationFK {BK}"        
        ,"customerInteractionSK customerInteractionFK"
        ,"surveyResponseInformationFK surveyResponseInformationFK"
        ,"customerInteractionId"
        ,"surveyResponseId"
        ,"customerServiceRelationshipTypeName"
        , "_change_type"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    #CleanSelf()
    SaveWithCDF(df, 'APPEND')
#     DisplaySelf()
#pass
Transform()
