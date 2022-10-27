# Databricks notebook source
###########################################################################################################################
# Loads WATERNETWORK dimension 
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

def getWaterNetwork():

    #1.Load Cleansed layer table data into dataframe
    baseDf = spark.sql(f"""select level30 as deliverySystem, 
                                level40 as distributionSystem, 
                                level50 as supplyZone, 
                                coalesce(level60,'Unknown')  as pressureArea, 
                                case when product = 'Water'  then 'Y' else 'N' end as isPotableWaterNetwork, 
                                case when product = 'RecycledWater'  then 'Y' else 'N' end as isRecycledWaterNetwork ,
                                _RecordDeleted 
                        from {ADS_DATABASE_CLEANSED}.hydra_TSYSTEMAREA 
                        where product in ('Water','RecycledWater') 
                        and   _RecordCurrent = 1 
                        """)

    #Dummy Record to be added to Property Dimension
    dummyDimRecDf = spark.createDataFrame([("Unknown","Unknown","Unknown","Unknown","Y","Y")], ["deliverySystem", "distributionSystem","supplyZone","pressureArea","isRecycledWaterNetwork","isPotableWaterNetwork"])

    #2.JOIN TABLES  
    #3.UNION TABLES
    df = baseDf.unionByName(dummyDimRecDf, allowMissingColumns = True)
    #print(f'{df.count():,} rows after Union 2')

    #4.SELECT / TRANSFORM
    df = df.selectExpr( \
                             "deliverySystem" \
                            ,"distributionSystem" \
                            ,"supplyZone" \
                            ,"pressureArea" \
                            ,"isPotableWaterNetwork" \
                            ,"isRecycledWaterNetwork" \
                           ,"_RecordDeleted" \
                            )
                                            
    #5.Apply schema definition
    schema = StructType([
                            StructField('waterNetworkSK', StringType(), False),
                            StructField("deliverySystem", StringType(), False),
                            StructField("distributionSystem", StringType(), False),
                            StructField("supplyZone", StringType(), False),
                            StructField("pressureArea", StringType(), True),
                            StructField("isPotableWaterNetwork", StringType(), False),
                            StructField("isRecycledWaterNetwork", StringType(), False)
                      ])

    return df, schema

# COMMAND ----------

df, schema = getWaterNetwork()
#TemplateEtl(df, entity="dimWaterNetwork", businessKey="supplyZone,pressureArea", schema=schema, writeMode=ADS_WRITE_MODE_OVERWRITE, AddSK=True)
TemplateEtlSCD(df, entity="dimWaterNetwork", businessKey="supplyZone,pressureArea", schema=schema)

# COMMAND ----------

# dbutils.notebook.exit("1")
