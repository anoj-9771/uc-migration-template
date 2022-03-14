# Databricks notebook source
#%run ../../includes/util-common

# COMMAND ----------

###########################################################################################################################
# Function: getStormWaterNetwork
#  GETS storm water network DIMENSION 
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
def getStormWaterNetwork():

#     spark.udf.register("TidyCase", GeneralToTidyCase)  

    #2.Load Cleansed layer table data into dataframe

    baseDf = spark.sql(f"select level30 as stormWaterNetwork, \
                                level40 as stormWaterCatchment \
                        from {ADS_DATABASE_CLEANSED}.hydra_TSYSTEMAREA \
                        where product = 'StormWater' \
                        and   _RecordDeleted = 0 \
                        and   _RecordCurrent = 1 \
                        ")

    #Dummy Record to be added to Property Dimension
    dummyDimRecDf = spark.createDataFrame([("Unknown","-1")], ["stormWaterNetwork", "stormWaterCatchment"])

    #3.JOIN TABLES  
    #4.UNION TABLES
    df = baseDf.unionByName(dummyDimRecDf, allowMissingColumns = True)
    print(f'{df.count():,} rows after Union 2')

    #5.SELECT / TRANSFORM
    df = df.selectExpr( \
     "stormWaterNetwork" \
    ,"stormWaterCatchment" \
    )
                                            
    #6.Apply schema definition
    newSchema = StructType([
                            StructField("stormWaterNetwork", StringType(), False),
                            StructField("stormWaterCatchment", StringType(), False)
                      ])

    df = spark.createDataFrame(df.rdd, schema=newSchema)
    return df


# COMMAND ----------

# ADS_DATABASE_CLEANSED = 'cleansed'
# df = getStormWaterNetwork()
# display(df)

# COMMAND ----------


