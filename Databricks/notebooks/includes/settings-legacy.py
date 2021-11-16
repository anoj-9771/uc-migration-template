# Databricks notebook source
#In Spark 3.1, loading and saving of timestamps from/to parquet files fails if the timestamps are before 1900-01-01 00:00:00Z, and loaded (saved) as the INT96 type. In Spark 3.0, the actions don’t fail but might lead to shifting of the input timestamps due to rebasing from/to Julian to/from Proleptic Gregorian calendar. To restore the behavior before Spark 3.1, you can set spark.sql.legacy.parquet.int96RebaseModeInRead or/and spark.sql.legacy.parquet.int96RebaseModeInWrite to LEGACY.
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "LEGACY")

# COMMAND ----------

spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")

# COMMAND ----------

#SparkUpgradeException: You may get a different result due to the upgrading of Spark 3.0: Fail to parse '1/03/2021 8:00' in the new parser. You can set spark.sql.legacy.timeParserPolicy to LEGACY to restore the behavior before Spark 3.0, or set to CORRECTED and treat it as an invalid datetime string. Caused by: DateTimeParseException: Text '1/03/2021 8:00' could not be parsed at index 0
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")