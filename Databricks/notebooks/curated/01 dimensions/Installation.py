# Databricks notebook source
###########################################################################################################################
# Loads INSTALLATION dimension 
#############################################################################################################################
# Method
# 1.Load Cleansed layer table data into dataframe and transform
# 2.UNION TABLES
# 3.SCHEMA DEFINITION
#############################################################################################################################

# COMMAND ----------

# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Need to Run Installation History Before Installation

# COMMAND ----------

# MAGIC %run ./InstallationHistory

# COMMAND ----------

def getInstallation():

    #1.Load Cleansed layer table data into dataframe
    isu0ucinstallaAttrDf  = spark.sql(f"""
        select 
            'ISU' as sourceSystemCode, 
            installationNumber,
            divisionCode,
            division,
            propertyNumber,
            Premise,
            meterReadingBlockedReason,
            basePeriodCategory,
            installationType,
            meterReadingControlCode,
            meterReadingControl,
            reference,
            authorizationGroupCode,
            guaranteedSupplyReason,
            serviceTypeCode,
            serviceType,
            deregulationStatus,
            createdDate,
            createdBy,
            lastChangedDate,
            lastChangedBy,
            _RecordDeleted 
        FROM {ADS_DATABASE_CLEANSED}.isu.0ucinstalla_attr_2 
        WHERE _RecordCurrent = 1 
        """
    )
    #print(f'Rows in isu0ucinstallaAttrDf:',isu0ucinstallaAttrDf.count())
    
    #Dummy Record to be added to Installation Dimension
    dummyDimRecDf = spark.createDataFrame(["-1"],"string").toDF("installationNumber")

    #2.UNION TABLES
    df = (
        isu0ucinstallaAttrDf
        .unionByName(dummyDimRecDf, allowMissingColumns = True)
        .drop_duplicates()
    )    
       
    #3.Apply schema definition
    schema = StructType([
        StructField('installationSK',StringType(),False),
        StructField('sourceSystemCode',StringType(),True),
        StructField('installationNumber',StringType(),False),
        StructField('divisionCode',StringType(),True),
        StructField('division',StringType(),True),
        StructField('propertyNumber',StringType(),True),
        StructField('Premise',StringType(),True),
        StructField('meterReadingBlockedReason',StringType(),True),
        StructField('basePeriodCategory',StringType(),True),
        StructField('installationType',StringType(),True),
        StructField('meterReadingControlCode',StringType(),True),
        StructField('meterReadingControl',StringType(),True),
        StructField('reference',StringType(),True),
        StructField('authorizationGroupCode',StringType(),True),
        StructField('guaranteedSupplyReason',StringType(),True),
        StructField('serviceTypeCode',StringType(),True),
        StructField('serviceType',StringType(),True),
        StructField('deregulationStatus',StringType(),True),
        StructField('createdDate',DateType(),True),
        StructField('createdBy',StringType(),True),
        StructField('lastChangedDate',DateType(),True),
        StructField('lastChangedBy',StringType(),True)
    ])

    return df, schema  

# COMMAND ----------

df, schema = getInstallation()

curnt_table = f'{ADS_DATABASE_CURATED}.dim.installation'
curnt_pk = 'installationNumber' 
curnt_recordStart_pk = 'installationNumber'
history_table = f'{ADS_DATABASE_CURATED}.dim.installationHistory'
history_table_pk = 'installationNumber'
history_table_pk_convert = 'installationNumber'

df_ = appendRecordStartFromHistoryTable(df,history_table,history_table_pk,curnt_pk,history_table_pk_convert,curnt_recordStart_pk)
updateDBTableWithLatestRecordStart(df_, curnt_table, curnt_pk)


TemplateEtlSCD(
    df_, 
    entity="dim.installation", 
    businessKey="installationNumber", 
    schema=schema
)

# COMMAND ----------

dbutils.notebook.exit("1")
