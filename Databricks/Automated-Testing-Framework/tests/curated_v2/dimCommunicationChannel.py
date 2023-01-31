# Databricks notebook source
# MAGIC %run ../../common/common-atf

# COMMAND ----------

keyColumns =  'sourceChannelCode'
mandatoryColumns = 'sourceChannelCode'

columns = ("""sourceChannelCode
,sourceChannelDescription
,channelCode
,channelDescription
,sourceSystemCode
""")


# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()
