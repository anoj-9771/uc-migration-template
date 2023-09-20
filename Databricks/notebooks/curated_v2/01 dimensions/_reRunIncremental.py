# Databricks notebook source
dbutils.notebook.run("./_RejectOverlapSourceData", 60*60)

# COMMAND ----------

dbutils.notebook.run("./Date", 60*60)

# COMMAND ----------

dbutils.notebook.run("./AccountBusinessPartner", 60*60)

# COMMAND ----------

dbutils.notebook.run("./BusinessPartner", 60*60)

# COMMAND ----------

dbutils.notebook.run("./BusinessPartnerAddress", 60*60)

# COMMAND ----------

dbutils.notebook.run("./BusinessPartnerGroup", 60*60)

# COMMAND ----------

dbutils.notebook.run("./BusinessPartnerIdentification", 60*60)

# COMMAND ----------

dbutils.notebook.run("./BusinessPartnerRelation", 60*60)

# COMMAND ----------

dbutils.notebook.run("./Contract", 60*60)

# COMMAND ----------

dbutils.notebook.run("./ContractAccount", 60*60)

# COMMAND ----------

dbutils.notebook.run("./ContractHistory", 60*60)

# COMMAND ----------

dbutils.notebook.run("./Device", 60*60)

# COMMAND ----------

dbutils.notebook.run("./DeviceCharacteristics", 60*60)

# COMMAND ----------

dbutils.notebook.run("./DeviceHistory", 60*60)

# COMMAND ----------

dbutils.notebook.run("./DeviceInstallationHistory", 60*60)

# COMMAND ----------

dbutils.notebook.run("./DisconnectionDocument", 60*60)

# COMMAND ----------

dbutils.notebook.run("./Installation", 60*60)

# COMMAND ----------

dbutils.notebook.run("./InstallationFacts", 60*60)

# COMMAND ----------

dbutils.notebook.run("./InstallationHistory", 60*60)

# COMMAND ----------

dbutils.notebook.run("./Location", 60*60)

# COMMAND ----------

dbutils.notebook.run("./MeterConsumptionBillingDocument", 60*60)

# COMMAND ----------

dbutils.notebook.run("./MeterConsumptionBillingLineItem", 60*60)

# COMMAND ----------

dbutils.notebook.run("./Property", 60*60)

# COMMAND ----------

dbutils.notebook.run("./PropertyLot", 60*60)

# COMMAND ----------

dbutils.notebook.run("./PropertyRelation", 60*60)

# COMMAND ----------

dbutils.notebook.run("./PropertyService", 60*60)

# COMMAND ----------

dbutils.notebook.run("./PropertyTypeHistory", 60*60)

# COMMAND ----------

dbutils.notebook.run("./RegisterHistory", 60*60)

# COMMAND ----------

dbutils.notebook.run("./RegisterInstallationHistory", 60*60)

# COMMAND ----------

dbutils.notebook.run("./viewBusinessPartner", 60*60)

# COMMAND ----------

dbutils.notebook.run("./ViewContract", 60*60)

# COMMAND ----------

dbutils.notebook.run("./ViewContractAccount", 60*60)

# COMMAND ----------

dbutils.notebook.run("./ViewDevice", 60*60)

# COMMAND ----------

dbutils.notebook.run("./ViewDeviceCharacteristics", 60*60)

# COMMAND ----------

dbutils.notebook.run("./viewInstallation", 60*60)

# COMMAND ----------

dbutils.notebook.run("./ViewProperty", 60*60)

# COMMAND ----------

dbutils.notebook.run("../02 facts/BilledWaterConsumption", 60*60)

# COMMAND ----------

dbutils.notebook.run("../02 facts/BilledWaterConsumptionMonthly", 60*60)
