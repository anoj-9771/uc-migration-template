# Databricks notebook source
def DeleteDirectoryRecursive(dirname):
    files=dbutils.fs.ls(dirname)
    for f in files:
        if f.isDir():
            DeleteDirectoryRecursive(f.path)
        dbutils.fs.rm(f.path, recurse=True)
    dbutils.fs.rm(dirname, True)
    
def CleanTable(tableNameFqn):
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {tableNameFqn}").collect()[0]
        DeleteDirectoryRecursive(detail.location)
    except:    
        pass
    try:
        spark.sql(f"DROP TABLE {tableNameFqn}")
    except:
        pass

# COMMAND ----------

dbutils.notebook.run("./_RejectOverlapSourceData", 60*60)

# COMMAND ----------

CleanTable('{ADS_DATABASE_CURATED}.dim.Date')
dbutils.notebook.run("./Date", 60*60)

# COMMAND ----------

CleanTable('{ADS_DATABASE_CURATED}.dim.AccountBusinessPartner')
dbutils.notebook.run("./AccountBusinessPartner", 60*60)

# COMMAND ----------

# CleanTable('{ADS_DATABASE_CURATED}.dim.BusinessPartner')
# dbutils.notebook.run("./BusinessPartner", 60*60)

# COMMAND ----------

CleanTable('{ADS_DATABASE_CURATED}.dim.BusinessPartnerAddress')
dbutils.notebook.run("./BusinessPartnerAddress", 60*60)

# COMMAND ----------

# CleanTable('{ADS_DATABASE_CURATED}.dim.BusinessPartnerGroup')
# dbutils.notebook.run("./BusinessPartnerGroup", 60*60)

# COMMAND ----------

CleanTable('{ADS_DATABASE_CURATED}.dim.BusinessPartnerIdentification')
dbutils.notebook.run("./BusinessPartnerIdentification", 60*60)

# COMMAND ----------

CleanTable('{ADS_DATABASE_CURATED}.dim.BusinessPartner')
CleanTable('{ADS_DATABASE_CURATED}.dim.BusinessPartnerGroup')
CleanTable('{ADS_DATABASE_CURATED}.dim.BusinessPartnerRelation')
dbutils.notebook.run("./BusinessPartnerRelation", 60*60)

# COMMAND ----------

CleanTable('{ADS_DATABASE_CURATED}.dim.ContractHistory')
CleanTable('{ADS_DATABASE_CURATED}.dim.Contract')
dbutils.notebook.run("./Contract", 60*60)

# COMMAND ----------

CleanTable('{ADS_DATABASE_CURATED}.dim.ContractAccount')
dbutils.notebook.run("./ContractAccount", 60*60)

# COMMAND ----------

# CleanTable('{ADS_DATABASE_CURATED}.dim.ContractHistory')
# dbutils.notebook.run("./ContractHistory", 60*60)

# COMMAND ----------

CleanTable('{ADS_DATABASE_CURATED}.dim.DeviceHistory')
CleanTable('{ADS_DATABASE_CURATED}.dim.DeviceCharacteristics')
CleanTable('{ADS_DATABASE_CURATED}.dim.Device')
dbutils.notebook.run("./Device", 60*60)

# COMMAND ----------

# CleanTable('{ADS_DATABASE_CURATED}.dim.DeviceCharacteristics')
# dbutils.notebook.run("./DeviceCharacteristics", 60*60)

# COMMAND ----------

# CleanTable('{ADS_DATABASE_CURATED}.dim.DeviceHistory')
# dbutils.notebook.run("./DeviceHistory", 60*60)

# COMMAND ----------

CleanTable('{ADS_DATABASE_CURATED}.dim.DeviceInstallationHistory')
dbutils.notebook.run("./DeviceInstallationHistory", 60*60)

# COMMAND ----------

CleanTable('{ADS_DATABASE_CURATED}.dim.DisconnectionDocument')
dbutils.notebook.run("./DisconnectionDocument", 60*60)

# COMMAND ----------

CleanTable('{ADS_DATABASE_CURATED}.dim.InstallationHistory')
CleanTable('{ADS_DATABASE_CURATED}.dim.Installation')
dbutils.notebook.run("./Installation", 60*60)

# COMMAND ----------

CleanTable('{ADS_DATABASE_CURATED}.dim.InstallationFacts')
dbutils.notebook.run("./InstallationFacts", 60*60)

# COMMAND ----------

# CleanTable('{ADS_DATABASE_CURATED}.dim.InstallationHistory')
# dbutils.notebook.run("./InstallationHistory", 60*60)

# COMMAND ----------

# CleanTable('{ADS_DATABASE_CURATED}.dim.Location')
# dbutils.notebook.run("./Location", 60*60)

# COMMAND ----------

CleanTable('{ADS_DATABASE_CURATED}.dim.MeterConsumptionBillingDocument')
dbutils.notebook.run("./MeterConsumptionBillingDocument", 60*60)

# COMMAND ----------

CleanTable('{ADS_DATABASE_CURATED}.dim.MeterConsumptionBillingLineItem')
dbutils.notebook.run("./MeterConsumptionBillingLineItem", 60*60)

# COMMAND ----------

#CleanTable('{ADS_DATABASE_CURATED}.dim.Location')
#CleanTable('{ADS_DATABASE_CURATED}.dim.PropertyLot')
#CleanTable('{ADS_DATABASE_CURATED}.dim.PropertyTypeHistory')
#CleanTable('{ADS_DATABASE_CURATED}.dim.Property')
#dbutils.notebook.run("./Property", 60*60)

# COMMAND ----------

CleanTable('{ADS_DATABASE_CURATED}.dim.WaterNetwork')
CleanTable('{ADS_DATABASE_CURATED}.dim.StormWaterNetwork')
CleanTable('{ADS_DATABASE_CURATED}.dim.SewerNetwork')
CleanTable('{ADS_DATABASE_CURATED}.dim.Location')
CleanTable('{ADS_DATABASE_CURATED}.dim.PropertyLot')
CleanTable('{ADS_DATABASE_CURATED}.dim.PropertyTypeHistory')
CleanTable('{ADS_DATABASE_CURATED}.dim.Property')
dbutils.notebook.run("./Property", 60*60)

# COMMAND ----------

# CleanTable('{ADS_DATABASE_CURATED}.dim.PropertyLot')
# dbutils.notebook.run("./PropertyLot", 60*60)

# COMMAND ----------

CleanTable('{ADS_DATABASE_CURATED}.dim.PropertyRelation')
dbutils.notebook.run("./PropertyRelation", 60*60)

# COMMAND ----------

CleanTable('{ADS_DATABASE_CURATED}.dim.PropertyService')
dbutils.notebook.run("./PropertyService", 60*60)

# COMMAND ----------

# CleanTable('{ADS_DATABASE_CURATED}.dim.PropertyTypeHistory')
# dbutils.notebook.run("./PropertyTypeHistory", 60*60)

# COMMAND ----------

CleanTable('{ADS_DATABASE_CURATED}.dim.RegisterHistory')
dbutils.notebook.run("./RegisterHistory", 60*60)

# COMMAND ----------

CleanTable('{ADS_DATABASE_CURATED}.dim.RegisterInstallationHistory')
dbutils.notebook.run("./RegisterInstallationHistory", 60*60)

# COMMAND ----------

dbutils.notebook.run("./viewBusinessPartnerGroup", 60*60)

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

dbutils.notebook.run("./ViewPropertyRelation", 60*60)

# COMMAND ----------

dbutils.notebook.run("./ViewPropertyService", 60*60)

# COMMAND ----------

dbutils.notebook.run("../02 facts/BilledWaterConsumption", 60*60)

# COMMAND ----------

dbutils.notebook.run("../02 facts/BilledWaterConsumptionMonthly", 60*60)
