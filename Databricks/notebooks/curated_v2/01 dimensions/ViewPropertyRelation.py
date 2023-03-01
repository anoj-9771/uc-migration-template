# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC -- View: viewPropertyRelation
# MAGIC -- Description: viewProprtyRelation
# MAGIC CREATE OR REPLACE VIEW curated_v2.viewPropertyRelation AS
# MAGIC 
# MAGIC SELECT * FROM 
# MAGIC (
# MAGIC SELECT
# MAGIC 		propertyRelationSK,
# MAGIC 		sourceSystemCode,
# MAGIC 		property1Number,
# MAGIC 		property2Number,
# MAGIC 		validFromDate,
# MAGIC 		validToDate,
# MAGIC 		relationshipTypeCode1,
# MAGIC 		relationshipType1,
# MAGIC 		relationshipTypeCode2,
# MAGIC 		relationshipType2,
# MAGIC         _RecordStart as _effectiveFrom,
# MAGIC         _RecordEnd as _effectiveTo,
# MAGIC       CASE
# MAGIC       WHEN CURRENT_TIMESTAMP() BETWEEN _RecordStart AND _RecordEnd then 'Y'
# MAGIC       ELSE 'N'
# MAGIC       END AS currentFlag,
# MAGIC       'Y' AS currentRecordFlag 
# MAGIC FROM curated_v2.dimpropertyrelation
# MAGIC WHERE _recordDeleted = 0
# MAGIC )
# MAGIC ORDER BY _effectiveFrom
