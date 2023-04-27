# Databricks notebook source
# MAGIC %md 
# MAGIC Vno| Date      | Who         |Purpose
# MAGIC ---|:---------:|:-----------:|:--------:
# MAGIC 1  |28/04/2023 |Mag          |Initial

# COMMAND ----------

# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET = DEFAULT_TARGET

# COMMAND ----------

from pyspark.sql.functions import lit, monotonically_increasing_id, col, when

def add_missing_columns(df, required_columns):
    for col_name in required_columns:
        if col_name not in df.schema.fieldNames():
            df = df.withColumn(col_name, lit(None))
    return df.select(required_columns).filter(col("recipientEmail").isNotNull())


table_name = ["qualtrics_billpaidsuccessfullyresponses", "qualtrics_businessConnectServiceRequestCloseResponses", "qualtrics_complaintsComplaintClosedResponses", "qualtrics_contactcentreinteractionmeasurementsurveyResponses", "qualtrics_customercareResponses", "qualtrics_developerapplicationreceivedResponses",
              "qualtrics_p4sonlinefeedbackResponses", "qualtrics_s73surveyResponses", "qualtrics_waterfixpostinteractionfeedbackResponses",
              "qualtrics_websitegoliveResponses", "qualtrics_wscs73experiencesurveyResponses", "qualtrics_feedbacktabgoliveResponses"]

required_columns = ["recipientEmail", "recipientFirstName", "recipientLastName", "customerFirstName", "customerLastName", "companyName", "ageGroup"]

union_df = None

for table in table_name:
    df = GetTable(f"{SOURCE}.{table}")
    df = add_missing_columns(df, required_columns) 
    
    if union_df is None:
        union_df = df
    else:
        union_df = union_df.unionByName(df)
        

finaldf = union_df.withColumn("sourceSystemCode", lit('Qualtrics').cast("string")) \
                  .withColumn("BusinessKey", concat_ws('|', union_df.recipientEmail, \
                                                            when((union_df.recipientFirstName).isNull(), lit('')).otherwise(union_df.recipientFirstName),
                                                            when((union_df.recipientLastName).isNull(), lit('')).otherwise(union_df.recipientLastName))) \
                  .dropDuplicates()  


# COMMAND ----------

def Transform():
    global df_final
    df_final = finaldf

    # ------------- TRANSFORMS ------------- # 
    _.Transforms = [
        f"BusinessKey {BK}"
        ,"recipientEmail emailRecipient"
        ,"recipientFirstName emailRecipientFirstName"
        ,"recipientLastName emailRecipientSurname"
        ,"customerFirstName customerFirstName"
        ,"customerLastName customerSurname" 
        ,"companyName companyName"
        ,"ageGroup ageGroup"             
        ,"sourceSystemCode sourceSystemCode"
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
