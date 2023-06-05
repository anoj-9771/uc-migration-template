# Databricks notebook source
# MAGIC %run ../../Common/common-helpers

# COMMAND ----------

####Curated viewServiceRequestReceivedCategory
spark.sql(f"""
CREATE OR REPLACE VIEW {get_table_namespace('curated_v3', 'viewservicerequestreceivedcategory')}
AS
SELECT
categorySK
,categoryUsage
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
,sourceValidFromDatetime
,sourceValidToDatetime
,sourceRecordCurrent
,sourceBusinessKey
FROM
  {get_table_namespace('curated_v3', 'dimCategory')}
WHERE categoryUsage = 'Service Request'
  AND categoryType = 'Received Category'
""")

# COMMAND ----------

#Curated viewServiceRequestResolutionCategory
spark.sql(f"""
CREATE OR REPLACE VIEW {get_table_namespace('curated_v3', 'viewserviceresolutioncategory')}
AS
SELECT
categorySK
,categoryUsage
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
,sourceValidFromDatetime
,sourceValidToDatetime
,sourceRecordCurrent
,sourceBusinessKey
FROM
  {get_table_namespace('curated_v3', 'dimCategory')}
WHERE categoryUsage = 'Service Request'
  AND categoryType = 'Resolution Category'
""")
