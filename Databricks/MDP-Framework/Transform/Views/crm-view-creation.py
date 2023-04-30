# Databricks notebook source
####Curated viewServiceRequestReceivedCategory
spark.sql("""
CREATE OR REPLACE VIEW curated_v3.viewServiceRequestReceivedCategory
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
  curated_v3.dimCategory
WHERE categoryUsage = 'Service Request'
  AND categoryType = 'Received Category'
""")

# COMMAND ----------

#Curated viewServiceRequestResolutionCategory
spark.sql("""
CREATE OR REPLACE VIEW curated_v3.viewServiceResolutionCategory
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
  curated_v3.dimCategory
WHERE categoryUsage = 'Service Request'
  AND categoryType = 'Resolution Category'
""")
