# Databricks notebook source
import json
import re

# COMMAND ----------

def GetEnvironmentTag():
    j = json.loads(spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags"))
    return [x['value'] for x in j if x['key'] == 'Environment'][0]

# COMMAND ----------

def GetPrefix(suffix="_"):
    try:
        prefix = dbutils.secrets.get("ADS" if not(any([i for i in json.loads(spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags")) if i["key"] == "Application" and "hackathon" in i["value"].lower()])) else "ADS-AKV", 'databricks-env')
        return "" if prefix == "~~" else prefix.replace("_", suffix)
    except:
        return None

# COMMAND ----------

_CURATED_MAP = {}
def LoadCuratedMap():
    global _CURATED_MAP
    path = "/mnt/datalake-raw/cleansed_csv/curated_mapping.csv"
    df = (spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("multiline", "true")
            .option("quote", "\"") 
            .load(path))
    prefix = GetPrefix()
    for i in df.collect():
        _CURATED_MAP[f"{i.current_database_name}.{i.current_table_name}"] = f"{prefix}curated.{i.future_database_name}.{i.future_table_name}"
LoadCuratedMap()

# COMMAND ----------

def GetCatalog(namespace):
    if "." not in namespace or not(any([i for i in ["raw", "cleansed", "curated"] if namespace.lower().startswith(i)])):
        return ""
    prefix = GetPrefix()
    c = namespace.split(".")
    return prefix+c[0]

# COMMAND ----------

def GetSchema(namespace):
    list = ["dim", "fact", "brg"]
    if "_" not in namespace and not(any([i for i in list if i in namespace])):
        return namespace.split(".", -1)[-2]
    
    s = namespace.lower().replace("hive_metastore.", "")
    if "." in namespace and namespace.count(".") == 2:
        s = namespace.split(".", -1)[-2]
    elif "." in namespace:
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
    prefix = GetPrefix()
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

def ReplaceQuery(sql):
    repalcedSql = sql
    for m in re.finditer("(?i)[ |\r|\n](from|join)[ |\r|\n]*[a-zA-Z0-9_.]+", repalcedSql, re.S | re.IGNORECASE):
            operation = re.split("[ |\r|\n]", m.group(0))[1]
            table = re.split("[ |\r|\n]", m.group(0))[-1]
            if len(table) <= 7 or "datalab" in table:
                continue
            repalcedSql = re.sub(m.group(0), f" {operation} " + ConvertTableName(table), repalcedSql, re.IGNORECASE)
    return repalcedSql

# COMMAND ----------

def CreateView(sql, newOwner="dev-Admins", preview=False):
    viewFqn = [re.split("[ |\r|\n]", m.group(0))[-1:][0] for m in re.finditer("(?i)[ |\r|\n](create)*(or)*(replace)*(view)[ |\r|\n]*[a-zA-Z0-9_.`]*", sql, re.S)][-1]
    db = viewFqn.split(".")[:-1][0].replace("`", "")
    table = viewFqn.split(".")[-1:][0].replace("`", "")
    count = spark.sql(f"SHOW VIEWS FROM {db} LIKE '{table}'").count()
    sql = ReplaceQuery(sql)

    # ALTER
    if count == 1:
        sql = re.sub("(?i)create\s*.*?view","alter view", sql, re.DOTALL | re.IGNORECASE)
    
    print(sql) if preview else spark.sql(sql)

    # OWNER ASSIGN
    spark.sql(f"ALTER VIEW {viewFqn} OWNER TO `{newOwner}`") if count == 0 and not(preview) else ()
