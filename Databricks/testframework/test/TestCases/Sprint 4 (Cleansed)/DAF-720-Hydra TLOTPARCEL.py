# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "sadaftest01"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-datalake-key")
container_name = "raw"
file_location = "wasbs://raw@sadaftest01.blob.core.windows.net/landing/hydraarchive/TLotParcel.csv"
file_type = "csv"
print(storage_account_name)
print(file_location)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] loading to a dataframe
 df = spark.read.format("csv").option('delimiter','|').option('header','true').load("wasbs://raw@sadaftest01.blob.core.windows.net/landing/hydraarchive/TLotParcel.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.createOrReplaceTempView("Source")

# COMMAND ----------

# DBTITLE 1,[Source] displaying records
# MAGIC %sql
# MAGIC select * from Source

# COMMAND ----------

# DBTITLE 1,[Source] displaying records with mapping
# MAGIC %sql
# MAGIC select 
# MAGIC 
# MAGIC case when `Property Number` = 'N/A' then null else `Property Number` end AS propertyNumber,
# MAGIC case when LGA = 'N/A' then null else LGA end AS LGA,
# MAGIC case when Address = ' ' then null else initcap(Address) end AS propertyAddress,
# MAGIC case when Suburb = 'N/A' then null else initcap(Suburb) end AS suburb,
# MAGIC case when `Land Use` = 'N/A' then null else initcap(`Land Use`) end AS landUse,
# MAGIC case when `Superior Land Use` = 'N/A' then null else initcap(`Superior Land Use`) end AS superiorLandUse,
# MAGIC case when `Area m2` = 'N/A' then null else initcap(`Area m2`) end AS areaSize,
# MAGIC case when Lon = 'N/A' then null else cast(Lon as float) end AS longitude,
# MAGIC case when Lat = 'N/A' then null else cast(Lat as float) end AS latitude,
# MAGIC case when `MGA56 X` = 'N/A' then null else cast(`MGA56 X` as float) end AS x_coordinate_MGA56,
# MAGIC case when ` MGA56 Y` = 'N/A' then null else cast(` MGA56 Y` as float) end AS y_coordinate_MGA56, 
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
# MAGIC case when `Stormwater Catchment` = 'N/A' then null else `Stormwater Catchment` end AS stormwaterCatchment
# MAGIC 
# MAGIC 
# MAGIC from Source

# COMMAND ----------

# DBTITLE 1,[Additional checks]
# MAGIC %sql
# MAGIC select 
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
# MAGIC stormwaterCatchment
# MAGIC from cleansed.t_hydra_tlotparcel where propertyAddress = 'Burragorang Rd Oakdale'

# COMMAND ----------

# DBTITLE 1,[Additional checks]
# MAGIC %sql
# MAGIC select 
# MAGIC 
# MAGIC case when `Property Number` = 'N/A' then null else `Property Number` end AS propertyNumber,
# MAGIC case when LGA = 'N/A' then null else LGA end AS LGA,
# MAGIC case when Address = ' ' then null else initcap(Address) end AS propertyAddress,
# MAGIC case when Suburb = 'N/A' then null else initcap(Suburb) end AS suburb,
# MAGIC case when `Land Use` = 'N/A' then null else initcap(`Land Use`) end AS landUse,
# MAGIC case when `Superior Land Use` = 'N/A' then null else initcap(`Superior Land Use`) end AS superiorLandUse,
# MAGIC case when `Area m2` = 'N/A' then null else initcap(`Area m2`) end AS areaSize,
# MAGIC case when Lon = 'N/A' then null else cast(Lon as float) end AS longitude,
# MAGIC case when Lat = 'N/A' then null else cast(Lat as float) end AS latitude,
# MAGIC case when `MGA56 X` = 'N/A' then null else cast(`MGA56 X` as float) end AS x_coordinate_MGA56,
# MAGIC case when ` MGA56 Y` = 'N/A' then null else cast(` MGA56 Y` as float) end AS y_coordinate_MGA56, 
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
# MAGIC case when `Stormwater Catchment` = 'N/A' then null else `Stormwater Catchment` end AS stormwaterCatchment
# MAGIC 
# MAGIC 
# MAGIC from Source where Address = 'Burragorang Rd Oakdale'

# COMMAND ----------

# DBTITLE 1,[Source] displaying records after mapping
# MAGIC %sql
# MAGIC select 
# MAGIC `Property Number` as propertyNumber,
# MAGIC LGA as LGA
# MAGIC ,Address as propertyAddress,
# MAGIC Suburb as suburb,
# MAGIC `Land Use` as landUse,
# MAGIC `Superior Land Use` as superiorLandUse,
# MAGIC `Area m2` as areaSize,
# MAGIC Lon as longitude,
# MAGIC Lat as latitude,
# MAGIC `MGA56 X` as x_coordinate_MGA56,
# MAGIC ` MGA56 Y` as y_coordinate_MGA56,
# MAGIC `Water Delivery System` as waterDeliverySystem,
# MAGIC `Water Distribution System` as waterDistributionSystem,
# MAGIC `Water Supply Zone` as waterSupplyZone,
# MAGIC `Water Pressure Zone` as waterPressureZone,
# MAGIC `Sewer Network` as sewerNetwork,
# MAGIC `Sewer Catchment` as sewerCatchment,
# MAGIC `Sewer SCAMP` as sewerScamp,
# MAGIC `Recycled Delivery System` as recycledDeliverySystem,
# MAGIC `Recycled Distribution System` as recycledDistributionSystem,
# MAGIC `Recycled Supply Zone` as recycledSupplyZone,
# MAGIC `Stormwater Catchment` as stormwaterCatchment
# MAGIC 
# MAGIC from Source

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

# DBTITLE 1,[Verification] Duplicate checks
# MAGIC %sql
# MAGIC SELECT meterClassCode, COUNT (*) as count
# MAGIC FROM cleansed.t_hydra_tlotparcel
# MAGIC GROUP BY meterClassCode
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
# MAGIC case when `Property Number` = 'N/A' then null else `Property Number` end AS propertyNumber,
# MAGIC case when LGA = 'N/A' then null else LGA end AS LGA,
# MAGIC --case when Address = ' ' then null else initcap(Address) end AS propertyAddress,
# MAGIC case when Suburb = 'N/A' then null else initcap(Suburb) end AS suburb,
# MAGIC case when `Land Use` = 'N/A' then null else initcap(`Land Use`) end AS landUse,
# MAGIC case when `Superior Land Use` = 'N/A' then null else initcap(`Superior Land Use`) end AS superiorLandUse,
# MAGIC case when `Area m2` = 'N/A' then null else initcap(`Area m2`) end AS areaSize,
# MAGIC case when Lon = 'N/A' then null else cast(Lon as float) end AS longitude,
# MAGIC case when Lat = 'N/A' then null else cast(Lat as float) end AS latitude,
# MAGIC case when `MGA56 X` = 'N/A' then null else cast(`MGA56 X` as float) end AS x_coordinate_MGA56,
# MAGIC case when ` MGA56 Y` = 'N/A' then null else cast(` MGA56 Y` as float) end AS y_coordinate_MGA56, 
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
# MAGIC case when `Stormwater Catchment` = 'N/A' then null else `Stormwater Catchment` end AS stormwaterCatchment
# MAGIC 
# MAGIC 
# MAGIC from Source
# MAGIC except
# MAGIC 
# MAGIC select 
# MAGIC propertyNumber,
# MAGIC LGA,
# MAGIC --propertyAddress,
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
# MAGIC stormwaterCatchment
# MAGIC from cleansed.t_hydra_tlotparcel

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select 
# MAGIC meterClassCode
# MAGIC ,UPPER(meterClass)
# MAGIC ,meterClassAbbreviation
# MAGIC ,meterClassEffectiveDate
# MAGIC ,meterClassCancelledDate
# MAGIC ,meterTypeCode
# MAGIC from cleansed.t_access_z309_tmeterclass
# MAGIC except
# MAGIC select 
# MAGIC C_METE_CLAS as meterClassCode
# MAGIC ,T_METE_CLAS as meterClass
# MAGIC ,T_METE_CLAS_ABBR as meterClassAbbreviation,
# MAGIC case when D_METE_CLAS_EFFE <> 'null' then 
# MAGIC CONCAT(LEFT(D_METE_CLAS_EFFE,4),'-',SUBSTRING(D_METE_CLAS_EFFE,5,2),'-',RIGHT(D_METE_CLAS_EFFE,2))
# MAGIC else D_METE_CLAS_EFFE end as meterClassEffectiveDate,
# MAGIC case when D_METE_CLAS_CANC <> 'null' then 
# MAGIC CONCAT(LEFT(D_METE_CLAS_CANC,4),'-',SUBSTRING(D_METE_CLAS_CANC,5,2),'-',RIGHT(D_METE_CLAS_CANC,2))
# MAGIC else D_METE_CLAS_CANC end as meterClassCancelledDate,
# MAGIC C_WATE_METE_TYPE as meterTypeCode
# MAGIC from Source
