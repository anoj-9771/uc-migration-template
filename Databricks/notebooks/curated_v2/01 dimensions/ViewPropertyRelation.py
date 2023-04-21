# Databricks notebook source
notebookPath = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")
view = notebookPath[-1:][0]
db = notebookPath[-3:][0]

spark.sql("""
-- View: viewPropertyRelation
-- Description: viewProprtyRelation
CREATE OR REPLACE VIEW curated_v2.viewPropertyRelation AS

SELECT * FROM 
(
SELECT
		propertyRelationSK,
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
FROM curated_v2.dimpropertyrelation
WHERE _recordDeleted = 0
)
ORDER BY _effectiveFrom
""".replace("CREATE OR REPLACE VIEW", "ALTER VIEW" if spark.sql(f"SHOW VIEWS FROM {db} LIKE '{view}'").count() == 0 else "CREATE OR REPLACE VIEW"))
