# Databricks notebook source
# MAGIC %run ./global-variables-python

# COMMAND ----------

def LogEtl(text):
  from datetime import datetime
  import pytz
  TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
  text = "{} - {}".format(datetime.now(pytz.timezone(ADS_TZ_LOCAL)).strftime(TIME_FORMAT), text)
  print(text)

# COMMAND ----------

#LogEtl("Test Message")

# COMMAND ----------

def LogExtended(text):
  if ADS_LOG_VERBOSE:
    print(text)