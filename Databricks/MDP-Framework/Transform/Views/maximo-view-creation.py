# Databricks notebook source
systemCode = dbutils.widgets.get("system_code")
#systemCode = 'maximo'

# COMMAND ----------

# SR View
spark.sql("""
CREATE OR REPLACE VIEW curated.vw_maximo_sr AS
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
      domainid = 'TKCLASS'
      AND maxvalue = 'SR'
  )
""")

# COMMAND ----------

#WOACTIVITY View
spark.sql("""
CREATE OR REPLACE VIEW curated.vw_maximo_woactivity AS
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
      domainid = 'WOCLASS'
      AND maxvalue = 'ACTIVITY'
  )
""")
