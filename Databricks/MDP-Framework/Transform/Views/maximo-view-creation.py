# Databricks notebook source
systemCode = dbutils.widgets.get("system_code")

# COMMAND ----------

# SR View
spark.sql("""
CREATE OR REPLACE VIEW cleansed.vw_maximo_sr AS
SELECT
  *
FROM
  cleansed.maximo_ticket
WHERE
  class IN (
    SELECT
      value
    FROM
      cleansed.maximo_synonymdomain
    WHERE
      domain = 'TKCLASS'
      AND internalValue = 'SR'
  )
""")

# COMMAND ----------

#WOACTIVITY View
spark.sql("""
CREATE OR REPLACE VIEW cleansed.vw_maximo_woactivity AS
SELECT
  *
FROM
  cleansed.maximo_workorder
WHERE
  class IN (
    SELECT
      value
    from
      cleansed.maximo_synonymdomain
    WHERE
      domain = 'WOCLASS'
      AND internalValue = 'ACTIVITY'
  )
""")
