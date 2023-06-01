# Databricks notebook source
import json

# COMMAND ----------

def GetCatalogPrefix():
    try:
        return dbutils.secrets.get("ADS" if not(any([i for i in json.loads(spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags")) if i["key"] == "Application" and "hackathon" in i["value"].lower()])) else "ADS-AKV", 'databricks-env')
    except:
        return ""

# COMMAND ----------

_CURATED_MAP = {}
def LoadCuratedMap():
    global _CURATED_MAP
    path = "dbfs:/FileStore/uc/curated_map.csv"
    df = (spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("multiline", "true")
            .option("quote", "\"") 
            .load(path))
    prefix = GetCatalogPrefix()
    for i in df.collect():
        _CURATED_MAP[f"{i.current_database_name}.{i.current_table_name}"] = f"{prefix}curated.{i.future_database_name}.{i.future_table_name}"
LoadCuratedMap()

# COMMAND ----------

def GetCatalog(namespace):
    if "." not in namespace or not(any([i for i in ["raw", "cleansed", "curated"] if namespace.lower().startswith(i)])):
        return ""
    prefix = GetCatalogPrefix()
    c = namespace.split(".")
    return prefix+c[0]

# COMMAND ----------

def GetSchema(namespace):
    list = ["dim", "fact", "brg"]
    if "_" not in namespace and not(any([i for i in list if i in namespace])):
        return ""
    
    s = namespace.lower().replace("hive_metastore.", "")
    if "." in namespace:
        s = namespace.split(".", -1)[-1]
    if "_" in s:
        s = s.split("_", 1)[0]
    startsWith = [i for i in list if s.startswith(i)]
    return startsWith[0] if any(startsWith) else s

# COMMAND ----------

def GetTable(namespace):
    list = ["dim", "fact", "brg"]
    if "_" not in namespace and "." not in namespace and not(any([i for i in list if i in namespace])):
        return namespace
    
    table = namespace.split("_", 1)[-1:][0]
    table = table.split(".")[-1]

    startsWith = [i for i in list if table.startswith(i)]
    return table.replace(startsWith[0], "") if any(startsWith) else table

# COMMAND ----------

def ConvertTableName(tableFqn):
    if "datalab" in tableFqn:
        return tableFqn
    prefix = GetCatalogPrefix()
    if "hive_metastore" not in tableFqn.lower() and tableFqn.count(".") == 2 and not tableFqn.startswith(prefix):
        return prefix+tableFqn

    curatedMap = _CURATED_MAP.get(tableFqn)
    if curatedMap is not None:
        return curatedMap

    tableFqn = tableFqn.lower().replace("hive_metastore.", "")

    catalog = GetCatalog(tableFqn)
    catalog = "" if catalog == "" else catalog+"."
    schema = GetSchema(tableFqn)
    schema = "" if schema == "" else schema+"."
    table = GetTable(tableFqn)
    return tableFqn if not catalog and not schema else f"{catalog}{schema}{table}"

# COMMAND ----------


