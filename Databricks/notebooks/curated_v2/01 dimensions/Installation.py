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
        FROM {ADS_DATABASE_CLEANSED}.isu_0ucinstalla_attr_2 
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

TemplateEtlSCD(
    df, 
    entity="dimInstallation", 
    businessKey="installationNumber", 
    schema=schema
)

# COMMAND ----------

dbutils.notebook.exit("1")
