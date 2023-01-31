# Databricks notebook source
# MAGIC %run ../../common/common-atf

# COMMAND ----------

keyColumns =  'categoryGroupCode'
mandatoryColumns = 'categoryGroupCode'

columns = ("""categoryUsage
,categoryType
,categoryGroupCode
,categoryGroupDescription
,categoryLevel1Code
,categoryLevel1Description
,categoryLevel2Code
,categoryLevel2Description
,categoryLevel3Code
,categoryLevel3Description
,categoryLevel4Code
,categoryLevel4Description
""")


# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()
