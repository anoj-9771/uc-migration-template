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

# DBTITLE 1,1. Include all util user functions for this notebook
# MAGIC %run ./includes/util-common

# COMMAND ----------

# DBTITLE 1,2.1 Include all bridge tables related user function for the notebook
# MAGIC %run ./functions/common-functions-bridgeTables

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
def TemplateEtl(df : object, entity, businessKey, schema, AddSK = True):
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
                                   schema,
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

# DBTITLE 1,6.1. Function: Load Bridge Tables
#Call Business Partner Group Relation function to load brgBusinessPartnerGroupRelation
def businessPartnerGroupRelationship():
    df, schema = getBusinessPartnerGroupRelationship()
    TemplateEtl(df, 
             entity="brgBusinessPartnerGroupRelationship", 
             businessKey="businessPartnerGroupSK,businessPartnerSK,validFromDate",
             schema,
             AddSK=False
            ) 

#Call InstallationPropertyMeterContract function to load brgInstallationPropertyMeterCon
def installationPropertyMeterContract():
    df, schema = getInstallationPropertyMeterContract()
    TemplateEtl(df, 
             entity="brgInstallationPropertyMeterContract", 
             businessKey="dimInstallationSK",
             schema,
             AddSK=False
            ) 

# COMMAND ----------

# DBTITLE 1,7. Function: Create stage and curated database if not exist
def DatabaseChanges():
  #CREATE stage AND curated DATABASES IS NOT PRESENT
  spark.sql("CREATE DATABASE IF NOT EXISTS stage")
  spark.sql("CREATE DATABASE IF NOT EXISTS curated")  


# COMMAND ----------

# DBTITLE 1,8. Flag Dimension/Bridge/Fact load
LoadBridgeTables = True

# COMMAND ----------

# DBTITLE 1,9. Function: Main - ETL
def Main():
    DatabaseChanges()

    #==============
    # BRIDGE TABLES
    #==============    
    if LoadBridgeTables:
        LogEtl("Start Bridge Tables")
        businessPartnerGroupRelationship()
        installationPropertyMeterContract()
        
        LogEtl("End Bridge Tables")
    else:
        LogEtl("Bridge table load not requested")    


# COMMAND ----------

# DBTITLE 1,10. Call Main function
Main()

# COMMAND ----------

# DBTITLE 1,12. Exit Notebook
dbutils.notebook.exit("1")
