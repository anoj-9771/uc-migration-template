# Databricks notebook source
###########################################################################################################################
# Loads STORMWATERNETWORK dimension 
#############################################################################################################################
# Method
# 1.Load Cleansed layer table data into dataframe and transform
# 2.JOIN TABLES
# 3.UNION TABLES
# 4.SELECT / TRANSFORM
# 5.SCHEMA DEFINITION
#############################################################################################################################

# COMMAND ----------

# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

def getStormWaterNetwork():

    #1.Load Cleansed layer table data into dataframe
    baseDf = spark.sql(f"""select level30 as stormWaterNetwork, 
                                level40 as stormWaterCatchment 
                        from {ADS_DATABASE_CLEANSED}.hydra_TSYSTEMAREA 
                        where product = 'StormWater' 
                        and   _RecordDeleted = 0 
                        and   _RecordCurrent = 1 
                        """)

    #Dummy Record to be added to Property Dimension
    dummyDimRecDf = spark.createDataFrame([("Unknown","Unknown")], ["stormWaterNetwork", "stormWaterCatchment"])

    #2.JOIN TABLES  
    #3.UNION TABLES
    df = baseDf.unionByName(dummyDimRecDf, allowMissingColumns = True)
    #print(f'{df.count():,} rows after Union 2')

    #4.SELECT / TRANSFORM
    df = df.selectExpr(\
                             "stormWaterNetwork" \
                            ,"stormWaterCatchment" \
                            )
                                            
    #5.Apply schema definition
    schema = StructType([
                            StructField('stormWaterNetworkSK', StringType(), False),
                            StructField("stormWaterNetwork", StringType(), False),
                            StructField("stormWaterCatchment", StringType(), False)
                        ])

    return df, schema


# COMMAND ----------

df, schema = getStormWaterNetwork()
#TemplateEtl(df, entity="dimStormWaterNetwork", businessKey="stormWaterCatchment", schema=schema, writeMode=ADS_WRITE_MODE_OVERWRITE, AddSK=True)
TemplateEtlSCD(df, entity="dimStormWaterNetwork", businessKey="stormWaterCatchment", schema=schema)