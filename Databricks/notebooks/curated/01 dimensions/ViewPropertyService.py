# Databricks notebook source
notebookPath = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")
view = notebookPath[-1:][0]
db = notebookPath[-3:][0]

spark.sql("""
-- View: viewPropertyService
-- Description: viewProprtyService
CREATE OR REPLACE VIEW curated.viewPropertyService
as

SELECT * FROM 
(
SELECT
		--dimpropertyservice.propertyServiceSK,
		 dimpropertyservice.sourceSystemCode
		,dimpropertyservice.propertyNumber
		,dimpropertyservice.architecturalObjectInternalId
		,dimpropertyservice.validToDate
		,dimpropertyservice.validFromDate
		,dimpropertyservice.fixtureAndFittingCharacteristicCode
		,dimpropertyservice.fixtureAndFittingCharacteristic
        ,dimpropertyservice.supplementInfo
        ,dimpropertyservice._RecordStart as _effectiveFrom
        ,dimpropertyservice._RecordEnd as _effectiveTo
    , CASE
      WHEN CURRENT_TIMESTAMP() BETWEEN dimpropertyservice._RecordStart AND dimpropertyservice._RecordEnd then 'Y'
      ELSE 'N'
      END AS currentFlag,
      'Y' AS currentRecordFlag 
FROM curated.dimpropertyservice
        where dimpropertyservice._recordDeleted = 0
        and dimpropertyservice.fixtureAndFittingCharacteristicCode NOT IN ('Unknown','ZDW1','ZDW2','ZPW1','ZPW2','ZPW3','ZPW4','ZRW1','ZRW2','ZRW3','ZWW1','ZWW2','ZWW3')
)
ORDER BY _effectiveFrom
""".replace("CREATE OR REPLACE VIEW", "ALTER VIEW" if spark.sql(f"SHOW VIEWS FROM {db} LIKE '{view}'").count() == 1 else "CREATE OR REPLACE VIEW"))

# COMMAND ----------


