# Databricks notebook source
###########################################################################################################################
# Loads SEWERNETWORK dimension 
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

def getSewerNetwork():

    #1.Load Cleansed layer table data into dataframe
    baseDf = spark.sql(f"select level30 as sewerNetwork, \
                                level40 as sewerCatchment, \
                                level50 as SCAMP \
                        from {ADS_DATABASE_CLEANSED}.hydra_TSYSTEMAREA \
                        where product = 'WasteWater' \
                        and   _RecordDeleted = 0 \
                        and   _RecordCurrent = 1 \
                        ")

    #Dummy Record to be added to Property Dimension
    #dummyDimRecDf = spark.createDataFrame([("Unknown","Unknown","-1")], ["sewerNetwork", "sewerCatchment","SCAMP"])

    #2.JOIN TABLES  
    #3.UNION TABLES
    #df = baseDf.unionByName(dummyDimRecDf, allowMissingColumns = True)
    #print(f'{df.count():,} rows after Union 2')

    #4.SELECT / TRANSFORM
    df = baseDf.selectExpr( \
     "sewerNetwork" \
    ,"sewerCatchment" \
    ,"SCAMP" \
    )
                                            
    #5.Apply schema definition
    schema = StructType([
                            StructField('dimSewerNetworkSK', LongType(), False),
                            StructField("sewerNetwork", StringType(), False),
                            StructField("sewerCatchment", StringType(), False),
                            StructField("SCAMP", StringType(), False)
                      ])

    return df, schema


# COMMAND ----------

df, schema = getSewerNetwork()
TemplateEtl(df, entity="dimSewerNetwork", businessKey="SCAMP", schema=schema, AddSK=True)

# COMMAND ----------

# ADS_DATABASE_CLEANSED = 'cleansed'
# df = getSewerNetwork()
# display(df)

# COMMAND ----------

dbutils.notebook.exit("1")
