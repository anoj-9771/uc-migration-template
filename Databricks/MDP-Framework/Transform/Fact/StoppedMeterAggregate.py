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
Transform()
