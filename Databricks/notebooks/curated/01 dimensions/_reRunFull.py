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

CleanTable('curated.dimDate')
dbutils.notebook.run("./Date", 60*60)

# COMMAND ----------

CleanTable('curated.dimAccountBusinessPartner')
dbutils.notebook.run("./AccountBusinessPartner", 60*60)

# COMMAND ----------

# CleanTable('curated.dimBusinessPartner')
# dbutils.notebook.run("./BusinessPartner", 60*60)

# COMMAND ----------

CleanTable('curated.dimBusinessPartnerAddress')
dbutils.notebook.run("./BusinessPartnerAddress", 60*60)

# COMMAND ----------

# CleanTable('curated.dimBusinessPartnerGroup')
# dbutils.notebook.run("./BusinessPartnerGroup", 60*60)

# COMMAND ----------

CleanTable('curated.dimBusinessPartnerIdentification')
dbutils.notebook.run("./BusinessPartnerIdentification", 60*60)

# COMMAND ----------

CleanTable('curated.dimBusinessPartner')
CleanTable('curated.dimBusinessPartnerGroup')
CleanTable('curated.dimBusinessPartnerRelation')
dbutils.notebook.run("./BusinessPartnerRelation", 60*60)

# COMMAND ----------

CleanTable('curated.dimContractHistory')
CleanTable('curated.dimContract')
dbutils.notebook.run("./Contract", 60*60)

# COMMAND ----------

CleanTable('curated.dimContractAccount')
dbutils.notebook.run("./ContractAccount", 60*60)

# COMMAND ----------

# CleanTable('curated.dimContractHistory')
# dbutils.notebook.run("./ContractHistory", 60*60)

# COMMAND ----------

CleanTable('curated.dimDeviceHistory')
CleanTable('curated.dimDeviceCharacteristics')
CleanTable('curated.dimDevice')
dbutils.notebook.run("./Device", 60*60)

# COMMAND ----------

# CleanTable('curated.dimDeviceCharacteristics')
# dbutils.notebook.run("./DeviceCharacteristics", 60*60)

# COMMAND ----------

# CleanTable('curated.dimDeviceHistory')
# dbutils.notebook.run("./DeviceHistory", 60*60)

# COMMAND ----------

CleanTable('curated.dimDeviceInstallationHistory')
dbutils.notebook.run("./DeviceInstallationHistory", 60*60)

# COMMAND ----------

CleanTable('curated.dimDisconnectionDocument')
dbutils.notebook.run("./DisconnectionDocument", 60*60)

# COMMAND ----------

CleanTable('curated.dimInstallationHistory')
CleanTable('curated.dimInstallation')
dbutils.notebook.run("./Installation", 60*60)

# COMMAND ----------

CleanTable('curated.dimInstallationFacts')
dbutils.notebook.run("./InstallationFacts", 60*60)

# COMMAND ----------

# CleanTable('curated.dimInstallationHistory')
# dbutils.notebook.run("./InstallationHistory", 60*60)

# COMMAND ----------

# CleanTable('curated.dimLocation')
# dbutils.notebook.run("./Location", 60*60)

# COMMAND ----------

CleanTable('curated.dimMeterConsumptionBillingDocument')
dbutils.notebook.run("./MeterConsumptionBillingDocument", 60*60)

# COMMAND ----------

CleanTable('curated.dimMeterConsumptionBillingLineItem')
dbutils.notebook.run("./MeterConsumptionBillingLineItem", 60*60)

# COMMAND ----------

#CleanTable('curated.dimLocation')
#CleanTable('curated.dimPropertyLot')
#CleanTable('curated.dimPropertyTypeHistory')
#CleanTable('curated.dimProperty')
#dbutils.notebook.run("./Property", 60*60)

# COMMAND ----------

CleanTable('curated.dimWaterNetwork')
CleanTable('curated.dimStormWaterNetwork')
CleanTable('curated.dimSewerNetwork')
CleanTable('curated.dimLocation')
CleanTable('curated.dimPropertyLot')
CleanTable('curated.dimPropertyTypeHistory')
CleanTable('curated.dimProperty')
dbutils.notebook.run("./Property", 60*60)

# COMMAND ----------

# CleanTable('curated.dimPropertyLot')
# dbutils.notebook.run("./PropertyLot", 60*60)

# COMMAND ----------

CleanTable('curated.dimPropertyRelation')
dbutils.notebook.run("./PropertyRelation", 60*60)

# COMMAND ----------

CleanTable('curated.dimPropertyService')
dbutils.notebook.run("./PropertyService", 60*60)

# COMMAND ----------

# CleanTable('curated.dimPropertyTypeHistory')
# dbutils.notebook.run("./PropertyTypeHistory", 60*60)

# COMMAND ----------

CleanTable('curated.dimRegisterHistory')
dbutils.notebook.run("./RegisterHistory", 60*60)

# COMMAND ----------

CleanTable('curated.dimRegisterInstallationHistory')
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
