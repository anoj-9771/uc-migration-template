# Databricks notebook source
from datetime import *
from pyspark.sql.functions import lit
from pyspark.sql import functions as F

# COMMAND ----------

def get_env() -> str:
    """centralised function to get prefix for the environment's catalogs"""
    rg_id = dbutils.secrets.get('ADS', 'databricks-workspace-resource-id')

    if 'dev' in rg_id:
        return 'dev_'
    elif 'test' in rg_id:
        return 'test_'
    elif 'preprod' in rg_id:
        return 'ppd_'
    elif 'prod' in rg_id:
        return ''
    else:
        raise Exception

# COMMAND ----------

spark.sql(f"""CREATE VOLUME if not exists {get_env()}raw.wingara.contractorhours""")
spark.sql(f"""CREATE VOLUME if not exists {get_env()}raw.wingara.Incident_HIPO""")

# COMMAND ----------

spark.sql(f"""create schema if not exists {get_env()}raw.wingara""")
spark.sql(f"""create schema if not exists {get_env()}cleansed.wingara""")

# COMMAND ----------

def Removespace(df):
    df= df.select([F.col(col).alias(col.replace(' ', '_')) for col in df.columns])
    return df

def addLineage(df):
    df=df.withColumn("LoadDateTime",lit(datetime.now()))
    return df


# COMMAND ----------

def ExcelReader(Path,Name):
    df = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"/Volumes/{get_env()}raw/wingara/{Path}/{Name}.xlsx")
    df=addLineage(df)
    df=Removespace(df)
    #display(df)
    df.write.mode("Overwrite").saveAsTable(f"{get_env()}raw.wingara.{Name}")
    return df

# COMMAND ----------

WingaraFlatFile=spark.createDataFrame([
    ("contractorhours","BI_SFTY_Contractor_Hours"),
    ("incident_hipo","BI_SFTY_Incident_HIPO")
    ],["Path","Name"])  

# COMMAND ----------

dataCollect = WingaraFlatFile.collect()
for row in dataCollect:
    ExcelReader(row['Path'],row['Name'])

# COMMAND ----------

display(spark.sql(f"""select * from {get_env()}raw.wingara.BI_SFTY_Contractor_Hours"""))
