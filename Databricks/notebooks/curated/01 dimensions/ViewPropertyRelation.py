# Databricks notebook source
# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

# notebookPath = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")
# view = notebookPath[-1:][0]
# db = notebookPath[-3:][0]
schema_name = 'water_consumption'
view_name = 'viewpropertyrelation'
view_fqn = f"{ADS_DATABASE_CURATED}.{schema_name}.{view_name}"

spark.sql(f"""
-- View: viewPropertyRelation
-- Description: viewProprtyRelation
CREATE OR REPLACE VIEW {view_fqn} AS

SELECT * FROM 
(
SELECT
		--propertyRelationSK,
		sourceSystemCode,
		property1Number,
		property2Number,
		validFromDate,
		validToDate,
		relationshipTypeCode1,
		relationshipType1,
		relationshipTypeCode2,
		relationshipType2,
        _RecordStart as _effectiveFrom,
        _RecordEnd as _effectiveTo,
      CASE
      WHEN CURRENT_TIMESTAMP() BETWEEN _RecordStart AND _RecordEnd then 'Y'
      ELSE 'N'
      END AS currentFlag,
      'Y' AS currentRecordFlag 
FROM {ADS_DATABASE_CURATED}.dim.propertyrelation
WHERE _recordDeleted = 0
)
ORDER BY _effectiveFrom
""".replace("CREATE OR REPLACE VIEW", "ALTER VIEW" if viewExists(view_fqn) else "CREATE OR REPLACE VIEW"))
