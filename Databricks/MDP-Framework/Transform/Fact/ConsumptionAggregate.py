# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

DEFAULT_TARGET = 'curated_v3'

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #

    # ------------- JOINS ------------------ #

    df = spark.sql(f"select * from ( Select * EXCEPT (latestRecordRanking) from ( select  row_number() over (partition by waterNetworkSK,unmeteredConnectedFlag, \
    unmeteredConstructionFlag order by reportDate desc) latestRecordRanking \
    ,waterNetworkSK, count(distinct propertyNumber) as propertyCount, sum(consumptionQuantity) as consumptionQuantity, \
    reportDate,unmeteredConnectedFlag,unmeteredConstructionFlag \
    from curated_v3.factUnmeteredConsumption where unmeteredConnectedFlag = 'Y' \
    group by waterNetworkSK,reportDate,unmeteredConnectedFlag,unmeteredConstructionFlag ) where latestRecordRanking = 1 \
    union \
    Select * EXCEPT (latestRecordRanking) from ( select  row_number() over (partition by waterNetworkSK,unmeteredConnectedFlag, \
    unmeteredConstructionFlag order by reportDate desc) latestRecordRanking \
    ,waterNetworkSK, count(distinct propertyNumber) as propertyCount, sum(consumptionQuantity) as consumptionQuantity, \
    reportDate,unmeteredConnectedFlag,unmeteredConstructionFlag \
    from curated_v3.factUnmeteredConsumption where unmeteredConstructionFlag = 'Y' \
    group by waterNetworkSK,reportDate,unmeteredConnectedFlag,unmeteredConstructionFlag ) where latestRecordRanking = 1 \
    ) as c order by unmeteredConnectedFlag desc,unmeteredConstructionFlag desc")
          

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"waterNetworkSK||'|'||unmeteredConnectedFlag||'|'||unmeteredConstructionFlag||'|'||reportDate {BK}"
        ,"waterNetworkSK waterNetworkSK"
        ,"reportDate reportDate"        
        ,"unmeteredConnectedFlag unmeteredConnectedFlag"     
        ,"unmeteredConstructionFlag unmeteredConstructionFlag"
        ,"propertyCount propertyCount"
        ,"consumptionQuantity consumptionQuantity"
        
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
    # display(df)
    # CleanSelf()
    Save(df)
    #DisplaySelf()
pass
Transform()
