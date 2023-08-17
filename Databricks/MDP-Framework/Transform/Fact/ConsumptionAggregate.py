# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ----------

# MAGIC %run ../../Common/common-helpers 

# COMMAND ----------

DEFAULT_TARGET = 'curated'

# COMMAND ----------

def Transform():
    global df
    # ------------- TABLES ----------------- #

    # ------------- JOINS ------------------ #

    df = spark.sql(f"select * from ( Select * EXCEPT (latestRecordRanking) from ( select  row_number() over (partition by waterNetworkSK,unmeteredConnectedFlag, \
    unmeteredConstructionFlag order by reportDate desc) latestRecordRanking \
    ,waterNetworkSK, count(distinct propertyNumber) as propertyCount, sum(consumptionQuantity) as consumptionQuantity, \
    reportDate,unmeteredConnectedFlag,unmeteredConstructionFlag \
    from {get_table_namespace('curated', 'factUnmeteredConsumption')} where unmeteredConnectedFlag = 'Y' \
    group by waterNetworkSK,reportDate,unmeteredConnectedFlag,unmeteredConstructionFlag ) where latestRecordRanking = 1 \
    union \
    Select * EXCEPT (latestRecordRanking) from ( select  row_number() over (partition by waterNetworkSK,unmeteredConnectedFlag, \
    unmeteredConstructionFlag order by reportDate desc) latestRecordRanking \
    ,waterNetworkSK, count(distinct propertyNumber) as propertyCount, sum(consumptionQuantity) as consumptionQuantity, \
    reportDate,unmeteredConnectedFlag,unmeteredConstructionFlag \
    from {get_table_namespace('curated', 'factUnmeteredConsumption')} where unmeteredConstructionFlag = 'Y' \
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

# COMMAND ----------

# runflag = spark.sql(f"""
#           select * from (select first_value(calendarDate) over (order by calendarDate asc) as businessDay from {get_env()}curated.dim.date where month(calendarDate) = month(current_date()) and year(calendarDate) = year(current_date()) and isWeekDayFlag = 'Y' and coalesce(isHolidayFlag,'N') = 'N' limit 1) where businessDay = current_date()""")
 
# if runflag.count() > 0:
#     print("Running on first business day of the month")
#     Transform()
# else:
#     # print("Skipping - Runs only on first business day of the month")  
#     dbutils.notebook.exit('{"Counts": {"SpotCount": 0}}')
