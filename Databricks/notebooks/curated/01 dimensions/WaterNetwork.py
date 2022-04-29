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
    baseDf = spark.sql(f"select level30 as deliverySystem, \
                                level40 as distributionSystem, \
                                level50 as supplyZone, \
                                coalesce(level60,'n/a')  as pressureArea, \
                                case when product = 'Water'  then 'Y' else 'N' end as isPotableWaterNetwork, \
                                case when product = 'RecycledWater'  then 'Y' else 'N' end as isRecycledWaterNetwork \
                        from {ADS_DATABASE_CLEANSED}.hydra_TSYSTEMAREA \
                        where product in ('Water','RecycledWater') \
                        and   _RecordDeleted = 0 \
                        and   _RecordCurrent = 1 \
                        ")

    #Dummy Record to be added to Property Dimension
    #dummyDimRecDf = spark.createDataFrame([("Unknown","Unknown","Unknown","-1","Unknown"),("Unknown","Unknown","-1",None,"Unknown")], ["deliverySystem", "distributionSystem","reservoirZone","pressureArea","isRecycled"])

    #2.JOIN TABLES  
    #3.UNION TABLES
    #df = baseDf.unionByName(dummyDimRecDf, allowMissingColumns = True)
    #print(f'{df.count():,} rows after Union 2')

    #4.SELECT / TRANSFORM
    df = baseDf.selectExpr( \
                             "deliverySystem" \
                            ,"distributionSystem" \
                            ,"supplyZone" \
                            ,"pressureArea" \
                            ,"isPotableWaterNetwork" \
                            ,"isRecycledWaterNetwork" \
                            )
                                            
    #5.Apply schema definition
    schema = StructType([
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
TemplateEtl(df, entity="dimWaterNetwork", businessKey="supplyZone,pressureArea", schema=schema, AddSK=True)

# COMMAND ----------

# ADS_DATABASE_CLEANSED = 'cleansed'
# df = getWaterNetwork()
# display(df)

# COMMAND ----------

dbutils.notebook.exit("1")
