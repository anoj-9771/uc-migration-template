# Databricks notebook source
# MAGIC %run ../../common/common-atf

# COMMAND ----------

# DBTITLE 1,Define fields and table names
keyColumns =  'installationNumber,validToDate'
mandatoryColumns = 'installationNumber,validFromDate,validToDate'

columns = ("""sourceSystemCode,
installationNumber,
validFromDate,
validToDate,
rateCategoryCode,
rateCategory,
portionNumber,
portionText,
industrySystemCode,
IndustrySystem,
industryCode,
industry,
billingClassCode,
billingClass,
meterReadingUnit
""")


# COMMAND ----------

def ManualDateCheck_1():
    df = spark.sql(f"SELECT * from {GetSelfFqn()} \
                where ValidFromDate > ValidToDate")
    count = df.count()
    #display(df)
    Assert(count, 0)
#ManualDupeCheck()

# COMMAND ----------

def ManualDateCheck_2():
    df = spark.sql(f"SELECT * from {GetSelfFqn()} \
                where (ValidFromDate <> date(_recordStart)) or (ValidToDate <>  date(_recordEnd))")
    count = df.count()
    #display(df)
    Assert(count, 0)
#ManualDupeCheck()

# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()

# COMMAND ----------

df = spark.sql(f"SELECT * from {GetSelfFqn()} where sourceSystemCode ='Unknown'")
display(df)
