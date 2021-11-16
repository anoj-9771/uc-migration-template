# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "sadaftest01"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-datalake-key")
container_name = "raw"
file_location = "wasbs://raw@sadaftest01.blob.core.windows.net/landing/hydraarchive/TLOTPARCEL.csv"
file_type = "csv"
print(storage_account_name)
print(file_location)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] loading to a dataframe
 df = spark.read.format("csv").option('delimiter','|').option('header','true').load("wasbs://raw@sadaftest01.blob.core.windows.net/landing/hydraarchive/TLOTPARCEL.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.createOrReplaceTempView("Source")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Source

# COMMAND ----------

# DBTITLE 1,[Source] displaying records
# MAGIC %sql
# MAGIC select * from Source where 
# MAGIC `Property Number`='4858656'

# COMMAND ----------

# DBTITLE 1,[Load source to a dataframe]
dbdfnew = spark.sql("select case when `Property Number` = 'N/A' then null else `Property Number` end AS propertyNumber,case when LGA = 'N/A' then null else LGA end AS LGA,case when Address = ' ' then null else initcap(Address) end AS propertyAddress,case when Suburb = 'N/A' then null else initcap(Suburb) end AS suburb,case when `Land Use` = 'N/A' then null else initcap(`Land Use`) end AS landUse,case when `Superior Land Use` = 'N/A' then null else initcap(`Superior Land Use`) end AS superiorLandUse,case when `Area m2` = 'N/A' then null else `Area m2` end AS areaSize,case when Lon = 'N/A' then null else cast(Lon as float) end AS longitude,case when Lat = 'N/A' then null else cast(Lat as float) end AS latitude,case when `MGA56 X` = 'N/A' then null else cast(`MGA56 X` as float) end AS x_coordinate_MGA56,case when `MGA56 Y` = 'N/A' then null else cast(`MGA56 Y` as float) end AS y_coordinate_MGA56, case when `Water Delivery System` = 'N/A' then null else `Water Delivery System` end AS waterDeliverySystem, case when `Water Distribution System` = 'N/A' then null else `Water Distribution System` end AS waterDistributionSystem, case when `Water Supply Zone` = 'N/A' then null else `Water Supply Zone` end AS waterSupplyZone, case when `Water Pressure Zone` = 'N/A' then null else `Water Pressure Zone` end AS waterPressureZone, case when `Sewer Network` = 'N/A' then null else `Sewer Network` end AS sewerNetwork,case when `Sewer Catchment` = 'N/A' then null else `Sewer Catchment` end AS sewerCatchment,case when `Sewer SCAMP` = 'N/A' then null else `Sewer SCAMP` end AS sewerScamp,case when `Recycled Delivery System` = 'N/A' then null else `Recycled Delivery System` end AS recycledDeliverySystem,case when `Recycled Distribution System` = 'N/A' then null else `Recycled Distribution System` end AS recycledDistributionSystem,case when `Recycled Supply Zone` = 'N/A' then null else `Recycled Supply Zone` end AS recycledSupplyZone,case when `Stormwater Catchment` = 'N/A' then null else `Stormwater Catchment` end AS stormwaterCatchment from Source")

# COMMAND ----------

# DBTITLE 1,[Display dataframe]
display(dbdfnew)

# COMMAND ----------

# DBTITLE 1,[Add column 'areaSizeUnit' with default value]
from pyspark.sql.functions import lit

df = df.withColumn("areaSizeUnit", lit('m2'))
df.show()

# COMMAND ----------

# DBTITLE 1,[create a temp table]
df.createOrReplaceTempView("Source1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Source1

# COMMAND ----------

# DBTITLE 1,[Source] displaying records with mapping
# MAGIC %sql
# MAGIC select
# MAGIC cast(`System Key` as int) AS systemKey,
# MAGIC cast(`Property Number` as int) AS propertyNumber,
# MAGIC case when LGA = 'N/A' then null else initcap(LGA) end AS LGA,
# MAGIC case when Address = ' ' then null else '1 Mumble St, Somewhere NSW 2000' end as propertyAddress,
# MAGIC case when Suburb = 'N/A' then null else initcap(Suburb) end AS suburb,
# MAGIC case when `Land Use` = 'N/A' then null else initcap(`Land Use`) end AS landUse,
# MAGIC case when `Superior Land Use` = 'N/A' then null else initcap(`Superior Land Use`) end AS superiorLandUse,
# MAGIC case when `Area m2` = 'N/A' then null else `Area m2` end AS areaSize,
# MAGIC case when Lon = 'N/A' then null else cast(Lon as float)+17 end AS longitude,
# MAGIC case when Lat = 'N/A' then null else cast(Lat as float)+23 end AS latitude,
# MAGIC case when `MGA56 X` = 'N/A' then null else cast(`MGA56 X` as float)+11235 end AS x_coordinate_MGA56,
# MAGIC case when `MGA56 Y` = 'N/A' then null else cast(`MGA56 Y` as float)+33245 end AS y_coordinate_MGA56,
# MAGIC case when `Water Delivery System` = 'N/A' then null else `Water Delivery System` end AS waterDeliverySystem,
# MAGIC case when `Water Distribution System` = 'N/A' then null else `Water Distribution System` end AS waterDistributionSystem,
# MAGIC case when `Water Supply Zone` = 'N/A' then null else `Water Supply Zone` end AS waterSupplyZone,
# MAGIC case when `Water Pressure Zone` = 'N/A' then null else `Water Pressure Zone` end AS waterPressureZone,
# MAGIC case when `Sewer Network` = 'N/A' then null else `Sewer Network` end AS sewerNetwork,
# MAGIC case when `Sewer Catchment` = 'N/A' then null else `Sewer Catchment` end AS sewerCatchment,
# MAGIC case when `Sewer SCAMP` = 'N/A' then null else `Sewer SCAMP` end AS sewerScamp,
# MAGIC case when `Recycled Delivery System` = 'N/A' then null else `Recycled Delivery System` end AS recycledDeliverySystem,
# MAGIC case when `Recycled Distribution System` = 'N/A' then null else `Recycled Distribution System` end AS recycledDistributionSystem,
# MAGIC case when `Recycled Supply Zone` = 'N/A' then null else `Recycled Supply Zone` end AS recycledSupplyZone,
# MAGIC case when `Stormwater Catchment` = 'N/A' then null else `Stormwater Catchment` end AS stormwaterCatchment,
# MAGIC areaSizeUnit
# MAGIC from Source1

# COMMAND ----------

# DBTITLE 1,[Target] displaying records
# MAGIC %sql
# MAGIC select * from cleansed.t_hydra_tlotparcel

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
cleansedf = spark.sql("select * from cleansed.t_hydra_tlotparcel")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
cleansedf.printSchema()

# COMMAND ----------

cleansedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate check
# MAGIC %sql
# MAGIC SELECT systemKey, COUNT (*) as count
# MAGIC FROM cleansed.t_hydra_tlotparcel
# MAGIC GROUP BY systemKey
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Records count check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_hydra_tlotparcel
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source  

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC cast(`System Key` as int) AS systemKey,
# MAGIC cast(`Property Number` as int) AS propertyNumber,
# MAGIC case when LGA = 'N/A' then null else initcap(LGA) end AS LGA,
# MAGIC case when Address = ' ' then null else '1 Mumble St, Somewhere NSW 2000' end as propertyAddress,
# MAGIC case when Suburb = 'N/A' then null else initcap(Suburb) end AS suburb,
# MAGIC case when `Land Use` = 'N/A' then null else initcap(`Land Use`) end AS landUse,
# MAGIC case when `Superior Land Use` = 'N/A' then null else initcap(`Superior Land Use`) end AS superiorLandUse,
# MAGIC case when `Area m2` = 'N/A' then null else `Area m2` end AS areaSize,
# MAGIC case when Lon = 'N/A' then null else cast(Lon as decimal(9,6))+.17 end AS longitude,
# MAGIC case when Lat = 'N/A' then null else cast(Lat as decimal(9,6))+.23 end AS latitude,
# MAGIC case when `MGA56 X` = 'N/A' then null else cast(`MGA56 X` as long)+112 end as x_coordinate_MGA56, 
# MAGIC case when `MGA56 Y` = 'N/A' then null else cast(`MGA56 Y` as long)+332 end as y_coordinate_MGA56,
# MAGIC case when `Water Delivery System` = 'N/A' then null else `Water Delivery System` end AS waterDeliverySystem,
# MAGIC case when `Water Distribution System` = 'N/A' then null else `Water Distribution System` end AS waterDistributionSystem,
# MAGIC case when `Water Supply Zone` = 'N/A' then null else `Water Supply Zone` end AS waterSupplyZone,
# MAGIC case when `Water Pressure Zone` = 'N/A' then null else `Water Pressure Zone` end AS waterPressureZone,
# MAGIC case when `Sewer Network` = 'N/A' then null else `Sewer Network` end AS sewerNetwork,
# MAGIC case when `Sewer Catchment` = 'N/A' then null else `Sewer Catchment` end AS sewerCatchment,
# MAGIC case when `Sewer SCAMP` = 'N/A' then null else `Sewer SCAMP` end AS sewerScamp,
# MAGIC case when `Recycled Delivery System` = 'N/A' then null else `Recycled Delivery System` end AS recycledDeliverySystem,
# MAGIC case when `Recycled Distribution System` = 'N/A' then null else `Recycled Distribution System` end AS recycledDistributionSystem,
# MAGIC case when `Recycled Supply Zone` = 'N/A' then null else `Recycled Supply Zone` end AS recycledSupplyZone,
# MAGIC case when `Stormwater Catchment` = 'N/A' then null else `Stormwater Catchment` end AS stormwaterCatchment,
# MAGIC areaSizeUnit
# MAGIC from Source1
# MAGIC except
# MAGIC select
# MAGIC systemKey,
# MAGIC propertyNumber,
# MAGIC LGA,
# MAGIC propertyAddress,
# MAGIC suburb,
# MAGIC landUse,
# MAGIC superiorLandUse,
# MAGIC areaSize,
# MAGIC longitude,
# MAGIC latitude,
# MAGIC x_coordinate_MGA56,
# MAGIC y_coordinate_MGA56,
# MAGIC waterDeliverySystem,
# MAGIC waterDistributionSystem,
# MAGIC waterSupplyZone,
# MAGIC waterPressureZone,
# MAGIC sewerNetwork,
# MAGIC sewerCatchment,
# MAGIC sewerScamp,
# MAGIC recycledDeliverySystem,
# MAGIC recycledDistributionSystem,
# MAGIC recycledSupplyZone,
# MAGIC stormwaterCatchment,
# MAGIC areaSizeUnit
# MAGIC from cleansed.t_hydra_tlotparcel

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC systemKey,
# MAGIC propertyNumber,
# MAGIC LGA,
# MAGIC propertyAddress,
# MAGIC suburb,
# MAGIC landUse,
# MAGIC superiorLandUse,
# MAGIC areaSize,
# MAGIC longitude,
# MAGIC latitude,
# MAGIC x_coordinate_MGA56,
# MAGIC y_coordinate_MGA56,
# MAGIC waterDeliverySystem,
# MAGIC waterDistributionSystem,
# MAGIC waterSupplyZone,
# MAGIC waterPressureZone,
# MAGIC sewerNetwork,
# MAGIC sewerCatchment,
# MAGIC sewerScamp,
# MAGIC recycledDeliverySystem,
# MAGIC recycledDistributionSystem,
# MAGIC recycledSupplyZone,
# MAGIC stormwaterCatchment,
# MAGIC areaSizeUnit
# MAGIC from cleansed.t_hydra_tlotparcel
# MAGIC except
# MAGIC select
# MAGIC cast(`System Key` as int) AS systemKey,
# MAGIC cast(`Property Number` as int) AS propertyNumber,
# MAGIC case when LGA = 'N/A' then null else initcap(LGA) end AS LGA,
# MAGIC case when Address = ' ' then null else '1 Mumble St, Somewhere NSW 2000' end as propertyAddress,
# MAGIC case when Suburb = 'N/A' then null else initcap(Suburb) end AS suburb,
# MAGIC case when `Land Use` = 'N/A' then null else initcap(`Land Use`) end AS landUse,
# MAGIC case when `Superior Land Use` = 'N/A' then null else initcap(`Superior Land Use`) end AS superiorLandUse,
# MAGIC case when `Area m2` = 'N/A' then null else `Area m2` end AS areaSize,
# MAGIC case when Lon = 'N/A' then null else cast(Lon as decimal(9,6))+.17 end AS longitude,
# MAGIC case when Lat = 'N/A' then null else cast(Lat as decimal(9,6))+.23 end AS latitude,
# MAGIC case when `MGA56 X` = 'N/A' then null else cast(`MGA56 X` as long)+112 end as x_coordinate_MGA56, 
# MAGIC case when `MGA56 Y` = 'N/A' then null else cast(`MGA56 Y` as long)+332 end as y_coordinate_MGA56,
# MAGIC case when `Water Delivery System` = 'N/A' then null else `Water Delivery System` end AS waterDeliverySystem,
# MAGIC case when `Water Distribution System` = 'N/A' then null else `Water Distribution System` end AS waterDistributionSystem,
# MAGIC case when `Water Supply Zone` = 'N/A' then null else `Water Supply Zone` end AS waterSupplyZone,
# MAGIC case when `Water Pressure Zone` = 'N/A' then null else `Water Pressure Zone` end AS waterPressureZone,
# MAGIC case when `Sewer Network` = 'N/A' then null else `Sewer Network` end AS sewerNetwork,
# MAGIC case when `Sewer Catchment` = 'N/A' then null else `Sewer Catchment` end AS sewerCatchment,
# MAGIC case when `Sewer SCAMP` = 'N/A' then null else `Sewer SCAMP` end AS sewerScamp,
# MAGIC case when `Recycled Delivery System` = 'N/A' then null else `Recycled Delivery System` end AS recycledDeliverySystem,
# MAGIC case when `Recycled Distribution System` = 'N/A' then null else `Recycled Distribution System` end AS recycledDistributionSystem,
# MAGIC case when `Recycled Supply Zone` = 'N/A' then null else `Recycled Supply Zone` end AS recycledSupplyZone,
# MAGIC case when `Stormwater Catchment` = 'N/A' then null else `Stormwater Catchment` end AS stormwaterCatchment,
# MAGIC areaSizeUnit
# MAGIC from Source1
