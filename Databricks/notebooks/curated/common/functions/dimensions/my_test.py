# Databricks notebook source
df = spark.sql("select * from stage.dimmeter")
display(df)
print(df.count())
df = spark.sql("select sourceSystemCode, meterID, meterSize, count(*) from stage.dimmeter group by sourceSystemCode, meterID,meterSize having count(*) > 1")
display(df)
print(df.count())

df = spark.sql("select * from stage.dimmeter where sourceSystemCode = 'Access' and meterID = 'BDRU0453'")
display(df)
print(df.count())

accessZ309TpropmeterDf = spark.sql("select 'Access' as sourceSystemCode, meterMakerNumber as meterId, meterSize, waterType \
                                      from cleansed.t_access_z309_tpropmeter \
                                      where _RecordCurrent = 1 and _RecordDeleted = 0")
accessZ309TpropmeterDf = accessZ309TpropmeterDf.dropDuplicates() #Please remove once upstream data is fixed
display(accessZ309TpropmeterDf)
print(accessZ309TpropmeterDf.count())

accessZ309TpropmeterDf = spark.sql("select 'Access' as sourceSystemCode, meterMakerNumber as meterId, meterSize, waterType \
                                      from cleansed.t_access_z309_tpropmeter")
print(accessZ309TpropmeterDf.count())
accessZ309TpropmeterDf = accessZ309TpropmeterDf.dropDuplicates() #Please remove once upstream data is fixed
display(accessZ309TpropmeterDf)
print(accessZ309TpropmeterDf.count())

df = spark.sql("select * from stage.dimmeter where sourceSystemCode = 'Access' and meterID = 'BDRU0453'")
display(df)
print(df.count())

# COMMAND ----------

df = spark.sql("select * from curated.dimmeter where _RecordCurrent=1 and sourceSystemCode = 'Access' and meterID = 'BDRU0453'")
df = spark.sql("select * from curated.dimmeter")
#df = spark.sql("select * from curated.dimmeter where _RecordDeleted=1")
display(df)
print(df.count())

#df = spark.sql("select * from stage.dimmeter")
df = spark.sql("select * from stage.dimmeter where meterID is null")

display(df)
print(df.count())

# COMMAND ----------



df = spark.sql("SELECT TGT.* FROM curated.DimMeter TGT where meterID is null")
display(df)
print(df.count())

accessZ309TpropmeterDf = spark.sql("select 'Access' as sourceSystemCode, meterMakerNumber as meterId, meterSize, waterType \
                                      from cleansed.t_access_z309_tpropmeter \
                                      where _RecordCurrent = 1 and _RecordDeleted = 0 and meterMakerNumber is null")
accessZ309TpropmeterDf = accessZ309TpropmeterDf.dropDuplicates() #Please remove once upstream data is fixed
display(accessZ309TpropmeterDf)
print(accessZ309TpropmeterDf.count())




# COMMAND ----------

df = spark.sql("SELECT CONCAT(TGT.`meterId`, '|', TGT.`sourceSystemCode`) AS merge_key, TGT.* FROM curated.DimMeter TGT \
	LEFT JOIN stage.DimMeter SRC ON SRC.`meterId`  =  TGT.`meterId` AND SRC.`sourceSystemCode`  =  TGT.`sourceSystemCode` \
	WHERE CONCAT(SRC.`meterId`, '|', SRC.`sourceSystemCode`) IS NULL \
	AND TGT._RecordCurrent = 1 AND TGT._RecordDeleted = 0 \
    UNION ALL \
	SELECT NULL AS merge_key, SRC.* FROM SRC" )
display(df)
print(df.count())

# COMMAND ----------

df = spark.sql("SELECT CONCAT(SRC.`meterId`, '|', SRC.`sourceSystemCode`) AS merge_key, SRC.* FROM stage.DimMeter SRC \
union all \
SELECT null as merge_key, SRC.* FROM stage.DimMeter SRC \
LEFT JOIN curated.DimMeter TGT ON SRC.`meterId`  =  TGT.`meterId` AND SRC.`sourceSystemCode`  =  TGT.`sourceSystemCode` \
WHERE 1 = 1 \
	-- The following joins will ensure that when INSERTing new version we only pick records that have changed. \
	AND TGT._RecordCurrent = 1")
display(df)
print(df.count())

# COMMAND ----------



# COMMAND ----------

df= spark.sql("SELECT meterId,sourceSystemCode, _RecordStart, \
                   CAST(ROW_NUMBER() OVER (ORDER BY 1) AS BIGINT) + 0 AS DimMeterSK FROM curated.DimMeter WHERE DimMeterSK IS NULL")
display(df)              

# COMMAND ----------

df = spark.sql("select propertyID from curated.dimproperty where propertyID in ('3100038','3100044')")
display (df)

df = spark.sql("select * from cleansed.t_access_z309_tproperty where propertyNumber in ('3100038','3100044')")
display(df)

