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

    df = spark.sql(f"""select * from ( select waternetworksk,count(distinct propertynumber) as propertycount,sum(consumptionquantity) as consumptionquantity,calculationdate,consumptionDate
    from {get_env()}curated.fact.StoppedMeterConsumption 
    where calculationdate = (select max(calculationdate) from {get_env()}curated.fact.StoppedMeterConsumption)
    group by waternetworksk,calculationdate,consumptionDate
    ) as c order by consumptionDate asc""")
          

    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"waterNetworkSK||'|'||calculationDate||'|'||consumptionDate {BK}"
        ,"waterNetworkSK waterNetworkSK"
        ,"calculationDate calculationDate"
        ,"consumptionDate consumptionDate"
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
# Transform()

# COMMAND ----------

runflag = spark.sql(f"""
          select * from (select first_value(calendarDate) over (order by calendarDate asc) as businessDay from {get_env()}curated.dim.date where month(calendarDate) = month(current_date()) and year(calendarDate) = year(current_date()) and isWeekDayFlag = 'Y' and coalesce(isHolidayFlag,'N') = 'N' limit 1) where businessDay = current_date()""")
 
if runflag.count() > 0:
    print("Running on first business day of the month")
    Transform()
else:
    # print("Skipping - Runs only on first business day of the month")  
    dbutils.notebook.exit('Skipping - Runs only on first business day of the month')
