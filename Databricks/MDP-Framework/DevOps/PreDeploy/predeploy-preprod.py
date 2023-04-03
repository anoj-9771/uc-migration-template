# Databricks notebook source
# MAGIC %run /MDP-Framework/ControlDB/common-controldb

# COMMAND ----------

ExecuteStatement("""
    DELETE FROM controldb.dbo.extractloadmanifest
    WHERE (SystemCode in ('swirldata') AND sourceID >= 80232)""")

# COMMAND ----------

ExecuteStatement("""
    DELETE FROM controldb.dbo.extractloadmanifest
    WHERE (SystemCode in ('swirlref') AND sourceID >= 81002)""")

# COMMAND ----------

ExecuteStatement("""
    DELETE FROM controldb.dbo.extractloadmanifest
    WHERE (SystemCode in ('swirldata') 
    AND SourceTableName IN ('BMS_9999999_184'
                           ,'BMS_9999999_189'
                           ,'BMS_9999999_194'
                           ,'BMS_9999999_222'
                           ,'BMS_9999999_233'
                           ,'BMS_9999999_845'
                           ,'BMS_9999999_847'
                           ,'BMS_9999999_848'
                           ,'PENDINGINCIDENTS_TEMP'
                           )
            )""")
