# Databricks notebook source
#config parameters
source = 'CRM' #either CRM or ISU
table = '0SVY_ANSWER_TEXT'

environment = 'test'
storage_account_name = "sablobdaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sablob-key")
containerName = "archive"


# COMMAND ----------

# MAGIC %run ../../includes/tableEvaluation

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC select
# MAGIC bwApplication,questionnaireId,
# MAGIC questionId,answerOption,
# MAGIC answerShort,answerMedium,
# MAGIC answerLong,answerId
# MAGIC from
# MAGIC (select
# MAGIC APPLICATION as bwApplication,
# MAGIC QSTNNR as questionnaireId,
# MAGIC QUESTION_ID as questionId,
# MAGIC ANSWER as answerOption,
# MAGIC TXTSH as answerShort,
# MAGIC TXTMD as answerMedium,
# MAGIC TXTLG as answerLong,
# MAGIC MAIN_ANSWER as answerId,
# MAGIC row_number() over (partition by APPLICATION, QSTNNR,QUESTION_ID,ANSWER order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC )a  
# MAGIC  where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC bwApplication,
# MAGIC questionnaireId,
# MAGIC questionId,
# MAGIC answerOption,
# MAGIC answerShort,
# MAGIC answerMedium,
# MAGIC answerLong,
# MAGIC answerId
# MAGIC from
# MAGIC (select
# MAGIC APPLICATION as bwApplication,
# MAGIC QSTNNR as questionnaireId,
# MAGIC QUESTION_ID as questionId,
# MAGIC ANSWER as answerOption,
# MAGIC TXTSH as answerShort,
# MAGIC TXTMD as answerMedium,
# MAGIC TXTLG as answerLong,
# MAGIC MAIN_ANSWER as answerId,
# MAGIC row_number() over (partition by APPLICATION, QSTNNR,QUESTION_ID,ANSWER order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC )a  
# MAGIC  where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT 
# MAGIC bwApplication,questionnaireId,
# MAGIC questionId,answerOption,
# MAGIC answerShort,answerMedium,
# MAGIC answerLong,answerId
# MAGIC , COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY bwApplication,questionnaireId,
# MAGIC questionId,answerOption,
# MAGIC answerShort,answerMedium,
# MAGIC answerLong,answerId
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY bwApplication,questionnaireId,questionId,answerOption
# MAGIC order by bwApplication,questionnaireId,questionId,answerOption
# MAGIC ) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1 

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC bwApplication,questionnaireId,
# MAGIC questionId,answerOption,
# MAGIC answerShort,answerMedium,
# MAGIC answerLong,answerId
# MAGIC from
# MAGIC (select
# MAGIC APPLICATION as bwApplication,
# MAGIC QSTNNR as questionnaireId,
# MAGIC QUESTION_ID as questionId,
# MAGIC ANSWER as answerOption,
# MAGIC TXTSH as answerShort,
# MAGIC TXTMD as answerMedium,
# MAGIC TXTLG as answerLong,
# MAGIC MAIN_ANSWER as answerId,
# MAGIC row_number() over (partition by APPLICATION, QSTNNR,QUESTION_ID,ANSWER order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table})a  
# MAGIC  where  a.rn = 1
# MAGIC  except
# MAGIC select
# MAGIC bwApplication,questionnaireId,
# MAGIC questionId,answerOption,
# MAGIC answerShort,answerMedium,
# MAGIC answerLong,answerId
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC bwApplication,questionnaireId,
# MAGIC questionId,answerOption,
# MAGIC answerShort,answerMedium,
# MAGIC answerLong,answerId
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC bwApplication,questionnaireId,
# MAGIC questionId,answerOption,
# MAGIC answerShort,answerMedium,
# MAGIC answerLong,answerId
# MAGIC from
# MAGIC (select
# MAGIC APPLICATION as bwApplication,
# MAGIC QSTNNR as questionnaireId,
# MAGIC QUESTION_ID as questionId,
# MAGIC ANSWER as answerOption,
# MAGIC TXTSH as answerShort,
# MAGIC TXTMD as answerMedium,
# MAGIC TXTLG as answerLong,
# MAGIC MAIN_ANSWER as answerId,
# MAGIC row_number() over (partition by APPLICATION, QSTNNR,QUESTION_ID,ANSWER order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table})a  
# MAGIC  where  a.rn = 1
