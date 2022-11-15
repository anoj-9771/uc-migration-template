# Databricks notebook source
# MAGIC %run ../../common/common-atf

# COMMAND ----------

keyColumns =  'channelCode'
mandatoryColumns = 'channel,channelCode'

columns = ("""channel
,channelCode
""")


# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()
