# Databricks notebook source
##################################################################
#Master Notebook
#1.Include all util user function for the notebook
#2.Include all dimension/bridge/fact user function for the notebook
#3.Define and get Widgets/Parameters
#4.Spark Config
#5.Function: Load data into Curated delta table
#6.Function: Load Dimensions/Bridge/Facts
#7.Function: Create stage and curated database if not exist
#8.Flag Dimension/Bridge/Fact load
#9.Function: Main - ETL
#10.Call Main function
#11.Exit Notebook
##################################################################

# COMMAND ----------

# MAGIC %pip install fiscalyear

# COMMAND ----------

# DBTITLE 1,1. Include all util user functions for this notebook
# MAGIC %run ./includes/util-common

# COMMAND ----------

# DBTITLE 1,2.1 Include all dimension related user function for the notebook
# MAGIC %run ./functions/common-functions-dimensions

# COMMAND ----------

# DBTITLE 1,2.2 Include all relationship related user function for the notebook
# MAGIC %run ./functions/common-functions-relationships

# COMMAND ----------

# DBTITLE 1,2.2 Include all bridge tables related user function for the notebook
# MAGIC %run ./functions/common-functions-bridgeTables

# COMMAND ----------

# DBTITLE 1,2.3 Include all fact related user function for the notebook
# MAGIC %run ./functions/common-functions-facts

# COMMAND ----------

# DBTITLE 1,3. Define and get Widgets/Parameters
#Set Parameters
dbutils.widgets.removeAll()

dbutils.widgets.text("Start_Date","")
dbutils.widgets.text("End_Date","")

#Get Parameters
start_date = dbutils.widgets.get("Start_Date")
end_date = dbutils.widgets.get("End_Date")

params = {"start_date": start_date, "end_date": end_date}

#DEFAULT IF ITS BLANK
start_date = "2000-01-01" if not start_date else start_date
end_date = "9999-12-31" if not end_date else end_date

#Print Date Range
print(f"Start_Date = {start_date}| End_Date = {end_date}")

# COMMAND ----------

# DBTITLE 1,4. Spark Config
# When set to true Spark SQL will automatically select a compression codec for each column based on statistics of the data.
spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed",True)

# Limit of total size of serialized results of all partitions for each Spark action (e.g. collect) in bytes. Should be at least 1M, or 0 for unlimited. Jobs will be aborted if the total size is above this limit. Having a high limit may cause out-of-memory errors in driver (depends on spark.driver.memory and memory overhead of objects in JVM). Setting a proper limit can protect the driver from out-of-memory errors.
#spark.conf.set("spark.driver.maxResultSize",0)

#Configures the maximum size in bytes for a table that will be broadcast to all worker nodes when performing a join. By setting this value to -1 broadcasting can be disabled.
# spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 0)

# COMMAND ----------

# DBTITLE 1,Test - Remove it
# #Remove - For testing
#
# spark.sql("DROP TABLE curated.dimproperty")
# spark.sql("DROP TABLE stage.dimproperty")

# df = spark.sql("select * from cleansed.stg_sapisu_0uc_connobj_attr_2 where haus = 6206050")
# df = spark.sql("select * from cleansed.t_sapisu_0uc_connobj_attr_2 where propertyNumber = 6206050")


# spark.sql("update curated.dimproperty set propertyType = 'Single Dwelling', _recordCurrent = 1 where propertyid = 3100038") #2357919
# spark.sql("update curated.dimproperty set propertyType = 'test12', _RecordStart = '2021-10-09T09:59:24.000+0000' where propertyid = 3100005") # Single Dwelling
# df = spark.sql("select * from curated.dimproperty where propertyid = 3100005") #2357919
# display(df)
# spark.sql("update curated.dimproperty set  _RecordStart = '2021-10-08T09:59:24.000+0000' where dimpropertysk = 895312") # Single Dwelling
# df = spark.sql("select * from curated.dimproperty where dimpropertysk = 895312") 
# display(df)
# df = spark.sql("select count(*) from curated.Factdailyapportionedconsumption") 
# display(df)


# COMMAND ----------

# DBTITLE 1,5. Function: Load data into Curated delta table
def TemplateEtl(df : object, entity, businessKey, AddSK = True):
    rawEntity = entity
    entity = GeneralToPascalCase(rawEntity)
    LogEtl(f"Starting {entity}.")

    v_COMMON_SQL_SCHEMA = "dbo"
    v_COMMON_CURATED_DATABASE = "curated"
    v_COMMON_DATALAKE_FOLDER = "curated"

    DeltaSaveDataFrameToDeltaTable(df, 
                                   rawEntity, 
                                   ADS_DATALAKE_ZONE_CURATED, 
                                   v_COMMON_CURATED_DATABASE, 
                                   v_COMMON_DATALAKE_FOLDER, 
                                   ADS_WRITE_MODE_MERGE, 
                                   track_changes = False, 
                                   is_delta_extract = False, 
                                   business_key = businessKey, 
                                   AddSKColumn = AddSK, 
                                   delta_column = "", 
                                   start_counter = "0", 
                                   end_counter = "0")

    #Commenting the below code, pending decision on Synapse
#     delta_table = f"{v_COMMON_CURATED_DATABASE}.{rawEntity}"
#     print(delta_table)
#     dw_table = f"{v_COMMON_SQL_SCHEMA}.{rawEntity}"
#     print(dw_table)

#     maxDate = SynapseExecuteSQLRead("SELECT isnull(cast(max([_RecordStart]) as varchar(50)),'2000-01-01') as maxval FROM " + dw_table + " ").first()["maxval"]
#     print(maxDate)

#     DeltaSyncToSQLDW(delta_table, v_COMMON_SQL_SCHEMA, entity, businessKey, start_counter = maxDate, data_load_mode = ADS_WRITE_MODE_MERGE, additional_property = "")

    LogEtl(f"Finished {entity}.")

# COMMAND ----------

# DBTITLE 1,6.1 Function: Load Dimensions
#Call BusinessPartner function to load DimBusinessPartnerGroup
def businessPartner():
    TemplateEtl(df=getBusinessPartner(), 
             entity="dimBusinessPartner", 
             businessKey="businessPartnerNumber,sourceSystemCode",
             AddSK=True
            )  

#Call BusinessPartnerGroup function to load DimBusinessPartnerGroup
def businessPartnerGroup():
    TemplateEtl(df=getBusinessPartnerGroup(), 
             entity="dimBusinessPartnerGroup", 
             businessKey="businessPartnerGroupNumber,sourceSystemCode",
             AddSK=True
            )    

#Call BillingDocumentSapisu function to load DimBillingDocument
def billingDocumentIsu():
    TemplateEtl(df=getBillingDocumentIsu(), 
             entity="dimBillingDocument", 
             businessKey="sourceSystemCode,billingDocumentNumber",
             AddSK=True
            )

#Call Contract function to load DimContract
def contract():
    TemplateEtl(df=getContract(), 
             entity="dimContract", 
             businessKey="contractId,validFromDate",
             AddSK=True
            )  

#Call Installation function to load DimLocation
def installation():
    TemplateEtl(df=getInstallation(), 
             entity="dimInstallation", 
             businessKey="installationId",
             AddSK=True
            )
    
#Call Location function to load DimLocation
def location():
    TemplateEtl(df=getLocation(), 
             entity="dimLocation", 
             businessKey="locationId",
             AddSK=True
            )

#Call Date function to load DimDate
def makeDate(): #renamed because date() gets overloaded elsewhere
    TemplateEtl(df=getDate(), 
             entity="dimDate", 
             businessKey="calendarDate",
             AddSK=True
            )

#Call Property function to load DimProperty
def makeProperty(): #renamed because property is a keyword
    TemplateEtl(df=getProperty(), 
             entity="dimProperty", 
             businessKey="sourceSystemCode,propertyNumber",
             AddSK=True
            )

#Call Meter function to load DimMeter
def meter():
    TemplateEtl(df=getMeter(), 
             entity="dimMeter", 
             businessKey="sourceSystemCode,meterNumber",
             AddSK=True
            )

#Call SewerNetwork function to load dimSewerNetwork
def sewerNetwork():
    TemplateEtl(df=getSewerNetwork(), 
             entity="dimSewerNetwork", 
             businessKey="SCAMP",
             AddSK=True
            )

#Call StormWater function to load dimStormWaterNetwork
def stormWaterNetwork():
    TemplateEtl(df=getStormWaterNetwork(), 
             entity="dimStormWaterNetwork", 
             businessKey="stormWaterCatchment",
             AddSK=True
            )

#Call StormWater function to load dimStormWaterNetwork
def waterNetwork():
    TemplateEtl(df=getWaterNetwork(), 
             entity="dimWaterNetwork", 
             businessKey="reservoirZone,pressureArea",
             AddSK=True
            )

# Add New Dim in alphabetical order
# def Dim2_Example():
#   TemplateEtl(df=GetDim2Example(), 
#              entity="Dim2Example",
#              businessKey="col1",
#              AddSK=True
#             )


# COMMAND ----------

# DBTITLE 1,6.2 Function: Load Relationship Tables
#Call meterTimeslice function to load meterTimeslice
def meterTimeslice(): 
    TemplateEtl(df=getmeterTimeslice(), 
             entity="meterTimeslice", 
             businessKey="meterSK,equipmentNumber,validToDate",
             AddSK=True
            )
    
def meterInstallation():
    TemplateEtl(df=getmeterInstallation(), 
             entity="meterInstallation", 
             businessKey="installationSK,installationId,logicalDeviceNumber,validToDate",
             AddSK=True
            )
    


# COMMAND ----------

# DBTITLE 1,6.3. Function: Load Facts

def billedWaterConsumption():
    TemplateEtl(df=getBilledWaterConsumption(),
             entity="factBilledWaterConsumption", 
             businessKey="sourceSystemCode,dimBillingDocumentSK,dimPropertySK,dimMeterSK,billingPeriodStartDate",
             AddSK=False
            )

def billedWaterConsumptionDaily():
    TemplateEtl(df=getBilledWaterConsumptionDaily(), 
             entity="factDailyApportionedConsumption", 
             businessKey="sourceSystemCode,consumptionDate,dimBillingDocumentSK,dimPropertySK,dimMeterSK",
             AddSK=False
            )


# Add New Fact in alphabetical order
# def Fact2_Example():
#   TemplateEtl(df=GetFact2Example(), 
#              entity="Fact2Example",
#              businessKey="col1",
#              AddSK=True
#             )


# COMMAND ----------

# DBTITLE 1,7. Function: Create stage and curated database if not exist
def DatabaseChanges():
  #CREATE stage AND curated DATABASES IS NOT PRESENT
  spark.sql("CREATE DATABASE IF NOT EXISTS stage")
  spark.sql("CREATE DATABASE IF NOT EXISTS curated")  


# COMMAND ----------

# DBTITLE 1,8. Flag Dimension/Bridge/Fact load
LoadDimensions = True
LoadFacts = True
LoadRelationships = True

# COMMAND ----------

# DBTITLE 1,9. Function: Main - ETL
def Main():
    DatabaseChanges()
    
    #==============
    # DIMENSIONS
    #==============

    if LoadDimensions:
        LogEtl("Start Dimensions")
        billingDocumentIsu()
        businessPartnerGroup()
        businessPartner()
        contract()
        makeDate()
        installation()
        location()
        meter()
        sewerNetwork()
        stormWaterNetwork()
        waterNetwork()
        #-----------------------------------------------------------------------------------------------
        # Note: Due to the fact that dimProperty relies on the system area tables having been populated,
        # makeProperty() must run after these three have been loaded
        #-----------------------------------------------------------------------------------------------
        makeProperty()
        #Add new Dim in alphabetical position
        
        LogEtl("End Dimensions")
    else:
        LogEtl("Dimension table load not requested")

    #==============
    # RELATIONSHIP TABLES
    #==============    
    if LoadRelationships:
        LogEtl("Start Relationship Tables")
        meterTimeslice()
        meterInstallation()

        LogEtl("End Relatioship Tables")
    else:
        LogEtl("Relationship table load not requested")
    #==============
    # FACTS
    #==============
    if LoadFacts:
        LogEtl("Start Facts")
        billedWaterConsumption()
        billedWaterConsumptionDaily()

        #fact2()
        LogEtl("End Facts")
    else:
        LogEtl("Fact table load not requested")


# COMMAND ----------

# DBTITLE 1,10. Call Main function
Main()

# COMMAND ----------

# DBTITLE 1,The dimWaterNetwork initially gets created with a value of n/a for pressure area to enable the allocation of the SK. This code sets it back to null
# MAGIC %sql
# MAGIC update curated.dimWaterNetwork
# MAGIC set pressureArea = null
# MAGIC where pressureArea = 'n/a'

# COMMAND ----------

# DBTITLE 1,11.View Generation
# MAGIC %sql
# MAGIC --View to get property history for Billed Water Consumption.
# MAGIC Create or replace view curated.viewBilledWaterConsumption as
# MAGIC select prop.propertyNumber,
# MAGIC prophist.inferiorPropertyTypeCode,
# MAGIC prophist.inferiorPropertyType,
# MAGIC prophist.superiorPropertyTypeCode,
# MAGIC prophist.superiorPropertyType,
# MAGIC fact.*
# MAGIC from curated.factbilledwaterconsumption fact
# MAGIC left outer join curated.dimproperty prop
# MAGIC on fact.dimPropertySK = prop.dimPropertySK
# MAGIC left outer join (select * from (select prophist.*,RANK() OVER   (PARTITION BY propertyNumber ORDER BY prophist.createddate desc,prophist.validToDate desc,prophist.validfromdate desc) as flag from cleansed.isu_zcd_tpropty_hist prophist) where flag=1) prophist
# MAGIC on prop.propertyNumber = prophist.propertyNumber
# MAGIC ;
# MAGIC 
# MAGIC --View to get property history for Apportioned Water Consumption.
# MAGIC Create or replace view curated.viewDailyApportionedConsumption as
# MAGIC select prop.propertyNumber,
# MAGIC prophist.inferiorPropertyTypeCode,
# MAGIC prophist.inferiorPropertyType,
# MAGIC prophist.superiorPropertyTypeCode,
# MAGIC prophist.superiorPropertyType,
# MAGIC fact.*
# MAGIC from curated.factDailyApportionedConsumption fact
# MAGIC left outer join curated.dimproperty prop
# MAGIC on fact.dimPropertySK = prop.dimPropertySK
# MAGIC left outer join (select * from (select prophist.*,RANK() OVER   (PARTITION BY propertyNumber ORDER BY prophist.createddate desc,prophist.validToDate desc,prophist.validfromdate desc) as flag from cleansed.isu_zcd_tpropty_hist prophist) where flag=1) prophist
# MAGIC on prop.propertyNumber = prophist.propertyNumber
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,12. Exit Notebook
dbutils.notebook.exit("1")
