# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

#####Determine Load #################
###############################
driverTable1 = 'cleansed.crm.zcs_long_text_f'   

if not(TableExists(_.Destination)):
    isDeltaLoad = False
    #####Table Full Load #####################
    derivedDF1 = GetTable(f"{getEnv()}{driverTable1}")
else:
    #####CDF for eligible tables#####################
    isDeltaLoad = True
    derivedDF1 = getSourceCDF(driverTable1, None, False).filter(col("_change_type") == lit("insert"))
    if derivedDF1.count() == 0:
        print("No delta to be  processed")
        #dbutils.notebook.exit(f"no CDF to process for table for source {driverTable1}  -- Destination {_.Destination}") 

# COMMAND ----------

from pyspark.sql.functions import col, pandas_udf, to_timestamp, rank, row_number
from pyspark.sql.types import ArrayType,StringType
import pandas as pd
def summary_res_string_spliter(text: pd.Series) -> pd.Series:  
    return text.str.findall("\[\s(?:SUMMARY|RESOLUTION)\sTEXT-\sDATE-\s[0-9]{8}\sTIME:\s[0-9]{6}\s\][^\[]*")
  
def interaction_string_spliter(text: pd.Series) -> pd.Series:  
    return text.str.findall("\[\sSummary\sText--\sDate-\s[0-9]{8}\sTime-\s[0-9]{6}\s\][^\[]*")

parse_summary_res_string = pandas_udf(summary_res_string_spliter, returnType=ArrayType(StringType()))
parse_interaction_string = pandas_udf(interaction_string_spliter, returnType=ArrayType(StringType()))

# COMMAND ----------

summary_res_match_regex = "(\[\s)(SUMMARY|RESOLUTION)(\sTEXT\-\sDATE\-\s)([0-9]{8})(\sTIME:\s)([0-9]{6})(\s\])([^\[]*)"
interaction_match_regex = "(\[\s)(Summary)(\sText\-\-\sDate\-\s)([0-9]{8})(\sTime\-\s)([0-9]{6})(\s\])([^\[]*)"
    
summary_res_notes_df =  (derivedDF1 #GetTable(f"{getEnv()}cleansed.crm.zcs_long_text_f")
    .select("serviceRequestGUID","serviceRequestID","summaryNote1","summaryNote2","summaryNote3","resolutionNote1","resolutionNote2","resolutionNote3"))

#     Prepare Summary DF
summary_df =  (summary_res_notes_df.select("serviceRequestGUID","serviceRequestID","summaryNote1","summaryNote2","summaryNote3") 
    .withColumn("workNoteType",lit("Summary")) 
    .withColumn("df_noteTypeCode",lit('Z001')))
summary_df = summary_df.withColumn("summaryNotes", concat_ws(" ","summaryNote1","summaryNote2","summaryNote3"))
summary_df = summary_df.withColumn('summaryNotes_new', parse_summary_res_string(summary_df.summaryNotes))
summary_df = summary_df.withColumn("notes", explode("summaryNotes_new"))
summary_df = summary_df.withColumn("notes", trim("notes"))

#     Prepare Resolution DF
res_df = (summary_res_notes_df.select("serviceRequestGUID","serviceRequestID","resolutionNote1","resolutionNote2","resolutionNote3") 
    .withColumn("workNoteType",lit("Resolution")) 
    .withColumn("df_noteTypeCode",lit('Z002')))
res_df = res_df.withColumn("resolutionNotes", concat_ws(" ","resolutionNote1","resolutionNote2","resolutionNote3"))
res_df = res_df.withColumn('resolutionNotes_new', parse_summary_res_string(res_df.resolutionNotes))
res_df = res_df.withColumn("notes", explode("resolutionNotes_new"))
res_df = res_df.withColumn("notes", trim("notes"))

columns = ["serviceRequestGUID","serviceRequestID","notes","workNoteType","df_noteTypeCode"]
summary_res_df = (summary_df.select([col for col in columns]) 
    .union(res_df.select([col for col in columns])))

summary_res_df = (summary_res_df.withColumn('Date', regexp_extract(col('notes'), summary_res_match_regex,4)) 
    .withColumn('Time', regexp_extract(col('notes'), summary_res_match_regex,6)) 
    .withColumn('Content', regexp_extract(col('notes'), summary_res_match_regex,8)))
summary_res_df = summary_res_df.withColumn('CreateDateTime', to_timestamp(concat(summary_res_df.Date,summary_res_df.Time), "yyyyMMddHHmmss"))
summary_res_df = summary_res_df.select("serviceRequestGUID","serviceRequestID","CreateDateTime","Content","workNoteType","df_noteTypeCode")
windowSpec1  = Window.partitionBy("serviceRequestGUID","serviceRequestID","CreateDateTime","workNoteType") 
summary_res_df = summary_res_df.withColumn("row_number",row_number().over(windowSpec1.orderBy(lit(1))))
summary_res_df = summary_res_df.withColumn("objectTypeCode",lit("BUS2000223")).withColumn("objectType",lit("Service Request"))
    

    
df1 = (GetTable(f"{getEnv()}cleansed.crm.zpstxhwithcguid").select("noteID","noteGUID","noteTypeCode","CreatedDateTime","CreatedBy","changeBy","changedDatetime","noteLineNum"))
aurion_employee_df = spark.sql(f"""Select userid, givenNames, surname from {getEnv()}cleansed.aurion.active_employees 
                               union Select userid, givenNames, surname from {getEnv()}cleansed.aurion.terminated_employees""")
windowSpecUserID  = Window.partitionBy("userid") 
aurion_employee_df = (aurion_employee_df.withColumn("rankUser",row_number().over(windowSpecUserID.orderBy(col("surname")))).filter("rankuser == 1"))
    

windowSpec2  = Window.partitionBy("noteGUID","CreatedDateTime","noteTypeCode")
df1 = df1.withColumn("rank",rank().over(windowSpec2.orderBy(col("noteID"))))
windowSpec3  = Window.partitionBy("noteGUID","noteTypeCode")
df1 = df1.withColumn("workNoteLineNumber",rank().over(windowSpec3.orderBy(row_number().over(windowSpec3.orderBy(col("CreatedDateTime"))))))
df1 = df1.join(aurion_employee_df,df1.CreatedBy == aurion_employee_df.userid,"left")
df1 = df1.withColumn("createdBy",coalesce(concat("givenNames", lit(" "),"surname"), "CreatedBy")).drop("userid","givenNames","surname")
df1 = df1.join(aurion_employee_df,df1.changeBy == aurion_employee_df.userid,"left")
df1 = df1.withColumn("modifiedBy",coalesce(concat("givenNames", lit(" "), "surname"), "changeBy")).drop("userid","givenNames","surname")
    
    # ------------- JOINS ------------------ #
worknote_df = (summary_res_df.join(df1,(
                  (summary_res_df.CreateDateTime ==df1.CreatedDateTime) &
                  (summary_res_df.serviceRequestGUID == df1.noteGUID) &
                  (summary_res_df.row_number ==df1.rank)  &
                  (summary_res_df.df_noteTypeCode == df1.noteTypeCode)),"inner"))
    
    
    # ------------- TRANSFORMS ------------- #
    
worknote_df = (worknote_df.withColumnRenamed("serviceRequestID","objectID") 
    .withColumnRenamed("changedDatetime","modifiedTimeStamp"))

# COMMAND ----------

def Transform():
    global df
    df = worknote_df.withColumn("_change_type", lit("insert"))

    _.Transforms = [
    f"objectTypeCode||'|'||workNoteType||'|'||noteID {BK}"
    ,"objectID customerServiceObjectId"
    ,"objectTypeCode customerServiceobjectTypeCode"
    ,"objectType customerServiceObjectTypeName"
    ,"noteID customerServiceWorkNoteId"
    ,"workNoteLineNumber customerServiceWorkNoteLineNumber"
    ,"workNoteType customerServiceWorkNoteTypeName"
    ,"Content customerServiceWorkNotesDescription"
    ,"createdBy customerServiceWorkNoteCreatedByUserName"
    ,"CreateDateTime customerServiceWorkNoteCreatedTimestamp"
    ,"modifiedBy customerServiceWorkNoteModifiedByUserName"
    ,"modifiedTimeStamp customerServiceWorkNoteModifiedTimestamp"
    ,"_change_type"
    ] 
    df = df.selectExpr(
        _.Transforms
    )
    
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
#     display(df)
    #CleanSelf()
    #Save(df)
    SaveWithCDF(df, 'APPEND')
    #DisplaySelf()
pass
Transform()
