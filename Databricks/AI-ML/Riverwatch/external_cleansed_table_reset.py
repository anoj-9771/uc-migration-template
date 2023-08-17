# Databricks notebook source
# MAGIC %run /build/includes/global-variables-python

# COMMAND ----------

spark.conf.set("c.catalog_name", ADS_DATABASE_CLEANSED)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ${c.catalog_name}.bom.dailyweatherobservation_sydneyairport

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ${c.catalog_name}.bom.fortdenision_tide

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ${c.catalog_name}.bom.weatherforecast

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ${c.catalog_name}.bom.weatherobservation_parramatta

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ${c.catalog_name}.bom.weatherobservation_sydneyairport
