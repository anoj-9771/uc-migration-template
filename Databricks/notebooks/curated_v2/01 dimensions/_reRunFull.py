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
        detail = spark.sql(f"DESCRIBE DETAIL {tableNameFqn}").rdd.collect()[0]
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

CleanTable('curated_v2.dimDate')
dbutils.notebook.run("./Date", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimAccountBusinessPartner')
dbutils.notebook.run("./AccountBusinessPartner", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimBusinessPartner')
dbutils.notebook.run("./BusinessPartner", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimBusinessPartnerAddress')
dbutils.notebook.run("./BusinessPartnerAddress", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimBusinessPartnerGroup')
dbutils.notebook.run("./BusinessPartnerGroup", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimBusinessPartnerIdentification')
dbutils.notebook.run("./BusinessPartnerIdentification", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimBusinessPartnerRelation')
dbutils.notebook.run("./BusinessPartnerRelation", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimContract')
dbutils.notebook.run("./Contract", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimContractAccount')
dbutils.notebook.run("./ContractAccount", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimContractHistory')
dbutils.notebook.run("./ContractHistory", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimDevice')
dbutils.notebook.run("./Device", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimDeviceCharacteristics')
dbutils.notebook.run("./DeviceCharacteristics", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimDeviceHistory')
dbutils.notebook.run("./DeviceHistory", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimDeviceInstallationHistory')
dbutils.notebook.run("./DeviceInstallationHistory", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimDisconnectionDocument')
dbutils.notebook.run("./DisconnectionDocument", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimInstallation')
dbutils.notebook.run("./Installation", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimInstallationFacts')
dbutils.notebook.run("./InstallationFacts", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimInstallationHistory')
dbutils.notebook.run("./InstallationHistory", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimLocation')
dbutils.notebook.run("./Location", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimMeterConsumptionBillingDocument')
dbutils.notebook.run("./MeterConsumptionBillingDocument", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimMeterConsumptionBillingLineItem')
dbutils.notebook.run("./MeterConsumptionBillingLineItem", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimWaterNetwork')
CleanTable('curated_v2.dimStormWaterNetwork')
CleanTable('curated_v2.dimSewerNetwork')

# COMMAND ----------

CleanTable('curated_v2.dimProperty')
dbutils.notebook.run("./Property", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimPropertyLot')
dbutils.notebook.run("./PropertyLot", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimPropertyRelation')
dbutils.notebook.run("./PropertyRelation", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimPropertyService')
dbutils.notebook.run("./PropertyService", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimPropertyTypeHistory')
dbutils.notebook.run("./PropertyTypeHistory", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimRegisterHistory')
dbutils.notebook.run("./RegisterHistory", 60*60)

# COMMAND ----------

CleanTable('curated_v2.dimRegisterInstallationHistory')
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
