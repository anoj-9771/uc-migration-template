# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

DEFAULT_TARGET = 'curated_v3'

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #

    # ------------- JOINS ------------------ #
    
    df = spark.sql(f"select * from ( Select * EXCEPT (latestRecordRanking) from ( select  row_number() over (partition by waterNetworkSK,deliverySystem,distributionSystem,supplyZone, \
    pressureArea, unmeteredConnectedFlag,unmeteredConstructionFlag order by reportDate desc) latestRecordRanking \
    ,waterNetworkSK,deliverySystem,distributionSystem,supplyZone,pressureArea, count(distinct propertyNumber) as propertyCount, sum(consumptionKLMonth) as consumptionKLMonth, \
    reportDate,unmeteredConnectedFlag,unmeteredConstructionFlag \
    from {DEFAULT_TARGET}.factUnmeteredConsumption where unmeteredConnectedFlag = 'Y' \
    group by waterNetworkSK,deliverySystem,distributionSystem,supplyZone,pressureArea, reportDate,unmeteredConnectedFlag,unmeteredConstructionFlag ) where latestRecordRanking = 1 \
    union \
    Select * EXCEPT (latestRecordRanking) from ( select  row_number() over (partition by waterNetworkSK,deliverySystem,distributionSystem,supplyZone,pressureArea,unmeteredConnectedFlag, \
    unmeteredConstructionFlag order by reportDate desc) latestRecordRanking \
    ,waterNetworkSK,deliverySystem,distributionSystem,supplyZone,pressureArea, count(distinct propertyNumber) as propertyCount, sum(consumptionKLMonth) as consumptionKLMonth, \
    reportDate,unmeteredConnectedFlag,unmeteredConstructionFlag \
    from {DEFAULT_TARGET}.factUnmeteredConsumption where unmeteredConstructionFlag = 'Y' \
    group by waterNetworkSK,deliverySystem,distributionSystem,supplyZone,pressureArea, reportDate,unmeteredConnectedFlag,unmeteredConstructionFlag ) where latestRecordRanking = 1 \
    ) as c order by unmeteredConnectedFlag desc,unmeteredConstructionFlag desc,deliverySystem,distributionSystem,supplyZone,pressureArea")
          

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"supplyZone||'|'||pressureArea||'|'||unmeteredConnectedFlag||'|'||unmeteredConstructionFlag||'|'||reportDate {BK}"
        ,"waterNetworkSK waterNetworkSK"
        ,"deliverySystem deliverySystem"
        ,"distributionSystem distributionSystem"
        ,"supplyZone supplyZone"
        ,"pressureArea pressureArea"
        ,"propertyCount propertyCount"
        ,"consumptionKLMonth consumptionKLMonth"
        ,"reportDate reportDate"        
        ,"unmeteredConnectedFlag unmeteredConnectedFlag"     
        ,"unmeteredConstructionFlag unmeteredConstructionFlag"
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
