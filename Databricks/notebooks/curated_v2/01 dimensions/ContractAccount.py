# Databricks notebook source
# MAGIC %md
# MAGIC # dimContractAccount

# COMMAND ----------

# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Contract Account Dataframe

# COMMAND ----------


#==================================
# Main Contract Account Dataframe
#==================================
df_contract_account = spark.sql(f"""
    SELECT
        'ISU'                         AS sourceSystemCode,
        contractAccountNumber         AS contractAccountNumber,
        legacyContractAccountNumber   AS legacyContractAccountNumber,
        applicationAreaCode           AS applicationAreaCode,
        applicationArea               AS applicationArea,
        contractAccountCategoryCode   AS contractAccountCategoryCode,
        contractAccountCategory       AS contractAccountCategory,
        createdBy                     AS createdBy,
        createdDate                   AS createdDate,
        lastChangedBy                 AS lastChangedBy,
        lastChangedDate               AS lastChangedDate,
        con._RecordDeleted            AS _RecordDeleted 
    FROM {ADS_DATABASE_CLEANSED}.isu_0cacont_acc_attr_2 con
    WHERE 
        con._RecordCurrent = 1 
"""    
).drop_duplicates()

#==================
# Dummy Dimension
#==================
dummyDimRecDf = spark.createDataFrame(
    ["-1"],
    "string"
).toDF("contractAccountNumber")

# Union Tables
df_contract_account = (
    df_contract_account
    .unionByName(dummyDimRecDf, allowMissingColumns = True)
    .drop_duplicates()
)    


# COMMAND ----------

schema = StructType([
    StructField('contractAccountSK',StringType(),False),
    StructField('sourceSystemCode',StringType(),True),
    StructField('contractAccountNumber',StringType(),False),
    StructField('legacyContractAccountNumber',StringType(),True),
    StructField('applicationAreaCode',StringType(),True),
    StructField('applicationArea',StringType(),True),
    StructField('contractAccountCategoryCode',StringType(),True),
    StructField('contractAccountCategory',StringType(),True),
    StructField('createdBy',StringType(),True),
    StructField('createdDate',DateType(),True),
    StructField('lastChangedBy',StringType(),True),
    StructField('lastChangedDate',DateType(),True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load

# COMMAND ----------

TemplateEtlSCD(
    df_contract_account, 
    entity="dimContractAccount", 
    businessKey="contractAccountNumber", 
    schema=schema
)

# COMMAND ----------

dbutils.notebook.exit("1")
