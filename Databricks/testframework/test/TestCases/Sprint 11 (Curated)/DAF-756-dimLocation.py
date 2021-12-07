# Databricks notebook source
# MAGIC %sql
# MAGIC select 
# MAGIC * from 
# MAGIC cleansed.hydra_tlotparcel where propertynumber = '4858656'

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC 
# MAGIC rtrim(ltrim(propertyNumber)) as LocationId,
# MAGIC propertyAddress as formattedAddress,
# MAGIC null as streetName,
# MAGIC null as streetType,
# MAGIC LGA as LGA,
# MAGIC suburb as suburb,
# MAGIC 'NSW' as state,
# MAGIC first(latitude) as latitude, 
# MAGIC first(longitude) as longitude
# MAGIC from cleansed.hydra_tlotparcel  group by LocationId having propertyNumber is not null and  propertynumber = '4858656'

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC rtrim(ltrim(propertyNumber)) as LocationId,
# MAGIC first(propertyAddress) as formattedAddress,
# MAGIC null as streetName,
# MAGIC null as streetType,
# MAGIC first(LGA) as LGA,
# MAGIC first(suburb) as suburb,
# MAGIC 'NSW' as state,
# MAGIC first(latitude) as latitude, 
# MAGIC first(longitude) as longitude
# MAGIC from cleansed.hydra_tlotparcel where propertyNumber is not null and  propertynumber = '4858656' group by propertyNumber 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.dimlocation where locationID = '4858656'

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC rtrim(ltrim(propertyNumber)) as LocationId,
# MAGIC first(propertyAddress) as formattedAddress,
# MAGIC null as streetName,
# MAGIC null as streetType,
# MAGIC first(LGA) as LGA,
# MAGIC first(suburb) as suburb,
# MAGIC 'NSW' as state,
# MAGIC first(latitude) as latitude, 
# MAGIC first(longitude) as longitude
# MAGIC from cleansed.hydra_tlotparcel where propertyNumber is not null and  propertynumber = '3100016' group by propertyNumber 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.dimlocation where locationID = '3100016'

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC rtrim(ltrim(propertyNumber)) as LocationId,
# MAGIC first(propertyAddress) as formattedAddress,
# MAGIC null as streetName,
# MAGIC null as streetType,
# MAGIC first(LGA) as LGA,
# MAGIC first(suburb) as suburb,
# MAGIC 'NSW' as state,
# MAGIC first(latitude) as latitude, 
# MAGIC first(longitude) as longitude
# MAGIC from cleansed.hydra_tlotparcel where propertyNumber is not null and  propertynumber = '4858656' group by propertyNumber 

# COMMAND ----------

# DBTITLE 1,[Mapping Rule]
# MAGIC %sql
# MAGIC select 
# MAGIC rtrim(ltrim(propertyNumber)) as LocationId,
# MAGIC propertyAddress as formattedAddress,
# MAGIC null as streetName,
# MAGIC null as streetType,
# MAGIC --LGA,
# MAGIC --suburb,
# MAGIC 'NSW' as state,
# MAGIC first(latitude) as latitude, 
# MAGIC first(longitude) as longitude
# MAGIC from cleansed.hydra_tlotparcel where propertyNumber is not null and  propertynumber = '4858656' group by propertyNumber 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.dimlocation where locationID = '4858656'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (
# MAGIC select propertyNumber, count(propertyNumber) as testcount from
# MAGIC cleansed.hydra_tlotparcel group by propertyNumber)a  where a.testcount > 1 

# COMMAND ----------

# DBTITLE 1,Apply Transformation
# MAGIC %sql
# MAGIC select 
# MAGIC LocationId,
# MAGIC formattedAddress,
# MAGIC streetName,
# MAGIC streetType,
# MAGIC LGA,
# MAGIC suburb,
# MAGIC state,
# MAGIC latitude,
# MAGIC longitude from (
# MAGIC select
# MAGIC systemKey,
# MAGIC rtrim(ltrim(propertyNumber)) as LocationId,
# MAGIC propertyAddress as formattedAddress,
# MAGIC null as streetName,
# MAGIC null as streetType,
# MAGIC LGA,
# MAGIC suburb as suburb,
# MAGIC 'NSW' as state,
# MAGIC first(latitude) as latitude, 
# MAGIC first(longitude) as longitude,
# MAGIC row_number() over (partition by propertyNumber order by systemKey desc) rn
# MAGIC from 
# MAGIC cleansed.hydra_tlotparcel 
# MAGIC where propertyNumber is not null )a where a.rn = 1
# MAGIC --)a
# MAGIC union all
# MAGIC select * from(
# MAGIC select 
# MAGIC '-1' as LocationID
# MAGIC ,'Unknown' as formattedAddress
# MAGIC ,'null' as streetName
# MAGIC ,'null' as streetType
# MAGIC ,'null' as LGA
# MAGIC ,'null' as suburb
# MAGIC ,'null' as state
# MAGIC ,'null' as latitude
# MAGIC ,'null' as longitude
# MAGIC from  cleansed.hydra_tlotparcel limit 1)b

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.hydra_tlotparcel

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC rtrim(ltrim(propertyNumber)) as LocationId,
# MAGIC propertyAddress as formattedAddress,
# MAGIC null as streetName,
# MAGIC null as streetType,
# MAGIC LGA,
# MAGIC suburb as suburb,
# MAGIC 'NSW' as state,
# MAGIC first(latitude),
# MAGIC first(longitude)
# MAGIC from 
# MAGIC cleansed.hydra_tlotparcel group by LocationId

# COMMAND ----------

# DBTITLE 1,[additional check]
# MAGIC %sql
# MAGIC select 
# MAGIC LocationId,
# MAGIC formattedAddress,
# MAGIC streetName,
# MAGIC streetType,
# MAGIC LGA,
# MAGIC suburb,
# MAGIC state,
# MAGIC latitude,
# MAGIC longitude from (
# MAGIC select
# MAGIC rtrim(ltrim(propertyNumber)) as LocationId,
# MAGIC propertyAddress as formattedAddress,
# MAGIC null as streetName,
# MAGIC null as streetType,
# MAGIC LGA,
# MAGIC suburb as suburb,
# MAGIC 'NSW' as state,
# MAGIC first(latitude),
# MAGIC first(longitude)
# MAGIC from 
# MAGIC cleansed.hydra_tlotparcel
# MAGIC where propertyNumber is not null )a  group by LocationId
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select * from(
# MAGIC select 
# MAGIC '-1' as LocationID
# MAGIC ,'Unknown' as formattedAddress
# MAGIC ,'null' as streetName
# MAGIC ,'null' as streetType
# MAGIC ,'null' as LGA
# MAGIC ,'null' as suburb
# MAGIC ,'null' as state
# MAGIC ,'null' as latitude
# MAGIC ,'null' as longitude
# MAGIC from  cleansed.hydra_tlotparcel limit 1)b

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.dimlocation where locationID = '-1'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.dimlocation where locationID = '-1'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from
# MAGIC curated.dimlocation

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (
# MAGIC select locationid, count(locationid) as testcount from
# MAGIC curated.dimlocation group by LocationID)a  where a.testcount > 1 

# COMMAND ----------

lakedftarget = spark.sql("select * from curated.dimlocation")
display(lakedftarget)

# COMMAND ----------

lakedftarget.printSchema()

# COMMAND ----------

# DBTITLE 1,[Verification] Count Checks
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Source' as TableName from(
# MAGIC select 
# MAGIC LocationId,
# MAGIC formattedAddress,
# MAGIC streetName,
# MAGIC streetType,
# MAGIC LGA,
# MAGIC suburb,
# MAGIC state,
# MAGIC latitude,
# MAGIC longitude from (
# MAGIC select
# MAGIC systemKey,
# MAGIC rtrim(ltrim(propertyNumber)) as LocationId,
# MAGIC propertyAddress as formattedAddress,
# MAGIC null as streetName,
# MAGIC null as streetType,
# MAGIC LGA,
# MAGIC suburb as suburb,
# MAGIC 'NSW' as state,
# MAGIC latitude,
# MAGIC longitude,
# MAGIC row_number() over (partition by propertyNumber order by systemKey desc) as rn
# MAGIC from 
# MAGIC cleansed.hydra_tlotparcel 
# MAGIC where propertyNumber is not null )a where a.rn = 1
# MAGIC --)a
# MAGIC union all
# MAGIC select * from(
# MAGIC select 
# MAGIC '-1' as LocationID
# MAGIC ,'Unknown' as formattedAddress
# MAGIC ,'null' as streetName
# MAGIC ,'null' as streetType
# MAGIC ,'null' as LGA
# MAGIC ,'null' as suburb
# MAGIC ,'null' as state
# MAGIC ,'null' as latitude
# MAGIC ,'null' as longitude
# MAGIC from  cleansed.hydra_tlotparcel limit 1)b)c
# MAGIC 
# MAGIC union all
# MAGIC select count (*) as RecordCount,'Target' as TableName from curated.dimlocation

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (*) as RecordCount from cleansed.hydra_tlotparcel  where propertyNumber is '3209771'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (*) as RecordCount from curated.dimlocation  where LocationID is null

# COMMAND ----------

# DBTITLE 1,[Verification] Auto Generate field check
# MAGIC %sql
# MAGIC select DimLocationSK from curated.dimlocation where DimLocationSK in (null,'',' ')

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Check
# MAGIC %sql
# MAGIC SELECT LocationID, COUNT (*) as count
# MAGIC FROM curated.dimlocation
# MAGIC GROUP BY LocationID
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Source vs Target check]
# MAGIC %sql
# MAGIC select * from(
# MAGIC select 
# MAGIC LocationId,
# MAGIC formattedAddress,
# MAGIC streetName,
# MAGIC streetType,
# MAGIC LGA,
# MAGIC suburb,
# MAGIC state
# MAGIC latitude,
# MAGIC longitude
# MAGIC from (
# MAGIC select
# MAGIC --systemKey,
# MAGIC rtrim(ltrim(propertyNumber)) as LocationId,
# MAGIC propertyAddress as formattedAddress,
# MAGIC null as streetName,
# MAGIC null as streetType,
# MAGIC LGA,
# MAGIC suburb as suburb,
# MAGIC 'NSW' as state,
# MAGIC latitude,
# MAGIC longitude,
# MAGIC row_number() over (partition by propertyNumber order by systemKey desc) as rn
# MAGIC from 
# MAGIC cleansed.hydra_tlotparcel 
# MAGIC where propertyNumber is not null )a where a.rn = 1
# MAGIC --)a
# MAGIC union all
# MAGIC select * from(
# MAGIC select 
# MAGIC '-1' as LocationID
# MAGIC ,'Unknown' as formattedAddress
# MAGIC ,'null' as streetName
# MAGIC ,'null' as streetType
# MAGIC ,'null' as LGA
# MAGIC ,'null' as suburb
# MAGIC ,'null' as state
# MAGIC ,'null' as latitude
# MAGIC ,'null' as longitude
# MAGIC from  cleansed.hydra_tlotparcel limit 1)b)c
# MAGIC 
# MAGIC except
# MAGIC 
# MAGIC select 
# MAGIC LocationID,
# MAGIC formattedAddress,
# MAGIC streetName,
# MAGIC streetType,
# MAGIC LGA,
# MAGIC suburb,
# MAGIC state,
# MAGIC latitude,
# MAGIC longitude
# MAGIC from
# MAGIC curated.dimlocation 

# COMMAND ----------

# DBTITLE 1,[Target vs Source]
# MAGIC %sql
# MAGIC select 
# MAGIC LocationID,
# MAGIC formattedAddress,
# MAGIC streetName,
# MAGIC streetType,
# MAGIC --LGA,
# MAGIC --suburb,
# MAGIC state
# MAGIC --latitude
# MAGIC --longitude
# MAGIC from
# MAGIC curated.dimlocation
# MAGIC except
# MAGIC select * from(
# MAGIC select 
# MAGIC LocationId,
# MAGIC formattedAddress,
# MAGIC streetName,
# MAGIC streetType,
# MAGIC --LGA,
# MAGIC --suburb,
# MAGIC state
# MAGIC --latitude,
# MAGIC --longitude
# MAGIC from (
# MAGIC select
# MAGIC systemKey,
# MAGIC rtrim(ltrim(propertyNumber)) as LocationId,
# MAGIC propertyAddress as formattedAddress,
# MAGIC null as streetName,
# MAGIC null as streetType,
# MAGIC LGA,
# MAGIC suburb as suburb,
# MAGIC 'NSW' as state,
# MAGIC latitude,
# MAGIC longitude,
# MAGIC row_number() over (partition by propertyNumber order by systemKey desc) rn
# MAGIC from 
# MAGIC cleansed.t_hydra_tlotparcel 
# MAGIC where propertyNumber is not null )a where a.rn = 1
# MAGIC --)a
# MAGIC union all
# MAGIC select * from(
# MAGIC select 
# MAGIC '-1' as LocationID
# MAGIC ,'Unknown' as formattedAddress
# MAGIC ,'null' as streetName
# MAGIC ,'null' as streetType
# MAGIC --,'null' as LGA
# MAGIC --,'null' as suburb
# MAGIC ,'null' as state
# MAGIC --,'null' as latitude
# MAGIC --,'null' as longitude
# MAGIC from  cleansed.hydra_tlotparcel limit 1)b)c

# COMMAND ----------

# DBTITLE 1,S vs T for LGA
# MAGIC %sql
# MAGIC select 
# MAGIC rtrim(ltrim(propertyNumber)) as LocationId,
# MAGIC LGA 
# MAGIC from 
# MAGIC cleansed.hydra_tlotparcel where propertyNumber is not null
# MAGIC 
# MAGIC except
# MAGIC select 
# MAGIC LocationID,
# MAGIC LGA
# MAGIC from
# MAGIC curated.dimlocation

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC systemKey,
# MAGIC rtrim(ltrim(propertyNumber)) as LocationId,
# MAGIC propertyAddress as formattedAddress,
# MAGIC null as streetName,
# MAGIC null as streetType,
# MAGIC LGA,
# MAGIC suburb as suburb,
# MAGIC 'NSW' as state,
# MAGIC latitude,
# MAGIC longitude
# MAGIC from 
# MAGIC cleansed.hydra_tlotparcel
# MAGIC where propertyNumber = '3810499'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.dimlocation where LocationId = '3810499'

# COMMAND ----------

# DBTITLE 1,T vs S for LGA 
# MAGIC %sql
# MAGIC select 
# MAGIC LocationID,
# MAGIC LGA
# MAGIC from
# MAGIC curated.dimlocation
# MAGIC except
# MAGIC select 
# MAGIC rtrim(ltrim(propertyNumber)) as LocationId,
# MAGIC LGA 
# MAGIC from 
# MAGIC cleansed.hydra_tlotparcel where propertyNumber is not null

# COMMAND ----------

# DBTITLE 1,[S vs T for suburb]
# MAGIC %sql
# MAGIC select 
# MAGIC rtrim(ltrim(propertyNumber)) as LocationId,
# MAGIC suburb as suburb
# MAGIC from 
# MAGIC cleansed.hydra_tlotparcel where propertyNumber is not null
# MAGIC 
# MAGIC except
# MAGIC 
# MAGIC select 
# MAGIC LocationID,
# MAGIC suburb
# MAGIC from
# MAGIC curated.dimlocation

# COMMAND ----------

# DBTITLE 1,[T vs S for suburb]
# MAGIC %sql
# MAGIC select 
# MAGIC LocationID,
# MAGIC suburb
# MAGIC from
# MAGIC curated.dimlocation
# MAGIC except
# MAGIC select 
# MAGIC rtrim(ltrim(propertyNumber)) as LocationId,
# MAGIC suburb as suburb
# MAGIC from 
# MAGIC cleansed.hydra_tlotparcel where propertyNumber is not null

# COMMAND ----------

# DBTITLE 1,[S vs T based on new mapping]
# MAGIC %sql
# MAGIC select 
# MAGIC rtrim(ltrim(propertyNumber)) as LocationId,
# MAGIC first(propertyAddress) as formattedAddress,
# MAGIC null as streetName,
# MAGIC null as streetType,
# MAGIC first(LGA) as LGA,
# MAGIC first(suburb) as suburb,
# MAGIC 'NSW' as state,
# MAGIC first(latitude) as latitude, 
# MAGIC first(longitude) as longitude
# MAGIC from cleansed.hydra_tlotparcel where propertyNumber is not null and propertyNumber='4858656'group by propertyNumber 
# MAGIC except
# MAGIC select 
# MAGIC LocationID,
# MAGIC formattedAddress,
# MAGIC streetName,
# MAGIC streetType,
# MAGIC LGA,
# MAGIC suburb,
# MAGIC state,
# MAGIC latitude,
# MAGIC longitude
# MAGIC from
# MAGIC curated.dimlocation where LocationId='4858656'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.hydra_tlotparcel where propertyNumber='4858656'

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC LocationID,
# MAGIC formattedAddress,
# MAGIC streetName,
# MAGIC streetType,
# MAGIC LGA,
# MAGIC suburb,
# MAGIC state,
# MAGIC latitude,
# MAGIC longitude
# MAGIC from
# MAGIC curated.dimlocation where LocationId='4858656'
# MAGIC except
# MAGIC select 
# MAGIC rtrim(ltrim(propertyNumber)) as LocationId,
# MAGIC first(propertyAddress) as formattedAddress,
# MAGIC null as streetName,
# MAGIC null as streetType,
# MAGIC first(LGA) as LGA,
# MAGIC first(suburb) as suburb,
# MAGIC 'NSW' as state,
# MAGIC first(latitude) as latitude, 
# MAGIC first(longitude) as longitude
# MAGIC from cleansed.hydra_tlotparcel where propertyNumber is not null and propertyNumber='4858656'group by propertyNumber 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.dimlocation where LocationId ='4858656'

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC rtrim(ltrim(propertyNumber)) as LocationId,
# MAGIC first(propertyAddress) as formattedAddress,
# MAGIC null as streetName,
# MAGIC null as streetType,
# MAGIC first(LGA) as LGA,
# MAGIC first(suburb) as suburb,
# MAGIC 'NSW' as state,
# MAGIC first(latitude) as latitude, 
# MAGIC first(longitude) as longitude
# MAGIC from cleansed.hydra_tlotparcel where propertyNumber is not null and propertyNumber='4858656'group by propertyNumber 
