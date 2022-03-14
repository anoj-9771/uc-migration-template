# Databricks notebook source
#%run ../../includes/util-common

# COMMAND ----------

###########################################################################################################################
# Function: getWaterNetwork
#  GETS water network DIMENSION 
# Returns:
#  Dataframe of transformed WaterNetwork
#############################################################################################################################
# Method
# 1.Create Function
# 2.Load Cleansed layer table data into dataframe and transform
# 3.JOIN TABLES
# 4.UNION TABLES
# 5.SELECT / TRANSFORM
#############################################################################################################################
#1.Create Function
def getWaterNetwork():

#     spark.udf.register("TidyCase", GeneralToTidyCase)  

    #2.Load Cleansed layer table data into dataframe

    baseDf = spark.sql(f"select level30 as deliverySystem, \
                                level40 as distributionSystem, \
                                level50 as reservoirZone, \
                                level60 as pressureArea, \
                                case when product = 'Water' then 'N' else 'Y' end as isRecycled \
                        from {ADS_DATABASE_CLEANSED}.hydra_TSYSTEMAREA \
                        where product in ('Water','RecycledWater') \
                        and   _RecordDeleted = 0 \
                        and   _RecordCurrent = 1 \
                        ")

    #Dummy Record to be added to Property Dimension
    dummyDimRecDf = spark.createDataFrame([("Unknown","Unknown","Unknown","-1","Unknown"),("Unknown","Unknown","-1",None,"Unknown")], ["deliverySystem", "distributionSystem","reservoirZone","pressureArea","isRecycled"])

    #3.JOIN TABLES  
    #4.UNION TABLES
    df = baseDf.unionByName(dummyDimRecDf, allowMissingColumns = True)
    print(f'{df.count():,} rows after Union 2')

    #5.SELECT / TRANSFORM
    df = df.selectExpr( \
     "deliverySystem" \
    ,"distributionSystem" \
    ,"reservoirZone" \
    ,"pressureArea" \
    ,"isRecycled" \
    )
                                            
    #6.Apply schema definition
    newSchema = StructType([
                            StructField("deliverySystem", StringType(), False),
                            StructField("distributionSystem", StringType(), False),
                            StructField("reservoirZone", StringType(), False),
                            StructField("pressureArea", StringType(), True),
                            StructField("isRecycled", StringType(), False)
                      ])

    df = spark.createDataFrame(df.rdd, schema=newSchema)
    return df


# COMMAND ----------

# ADS_DATABASE_CLEANSED = 'cleansed'
# df = getWaterNetwork()
# display(df)

# COMMAND ----------


