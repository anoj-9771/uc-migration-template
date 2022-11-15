# Databricks notebook source
# MAGIC %run ../../../../atf-common

# COMMAND ----------

# MAGIC %run /build/includes/util-general

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
target_df = spark.sql("select * from curated_v2.dimProperty")
target_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC --'-1' as propertyNumber,
# MAGIC wn.waterNetworkSK as waterNetworkSK_drinkingWater1, wnr.waterNetworkSK as waterNetworkSK_recycledWater1, snw.sewerNetworkSK as sewerNetworkSK1, sw.stormWaterNetworkSK as stormWaterNetworkSK1 
# MAGIC                             from curated_v2.dimWaterNetwork wn, 
# MAGIC                                  curated_v2.dimWaterNetwork wnr, 
# MAGIC                                 curated_v2.dimSewerNetwork snw, 
# MAGIC                                  curated_v2.dimStormWaterNetwork sw 
# MAGIC                             where wn.pressureArea = 'Unknown' 
# MAGIC                             and   wn._RecordCurrent = 1 and wn.isPotableWaterNetwork='Y' 
# MAGIC                             and   wnr.supplyZone = 'Unknown' 
# MAGIC                             and   wnr._RecordCurrent = 1 and wnr.isRecycledWaterNetwork='Y' 
# MAGIC                             and   snw.SCAMP = 'Unknown' 
# MAGIC                             and   snw._RecordCurrent = 1 
# MAGIC                             and   sw.stormWaterCatchment = 'Unknown' 
# MAGIC                             and   sw._RecordCurrent = 1 

# COMMAND ----------

# DBTITLE 1,Source 
sdf=spark.sql(""" with  t5 as ( select propertyNumber,
		wn.waterNetworkSK as potableSK, 
		wnr.waterNetworkSK as recycledSK,
		snw.sewerNetworkSK as sewerNetworkSK,
		sw.stormWaterNetworkSK as stormWaterNetworkSK, 
		row_number() over (partition by propertyNumber order by lp.waterPressureZone desc, lp.recycledSupplyZone desc, lp.sewerScamp desc, lp.stormWaterCatchment desc) as rnk
        from cleansed.hydra_TLotParcel lp 
        left outer join curated_v2.dimWaterNetwork wn
		on lp.waterPressureZone = wn.pressureArea and wn._RecordCurrent = 1 
        left outer join curated_v2.dimWaterNetwork wnr
		on lp.recycledSupplyZone = wnr.supplyZone and wnr._RecordCurrent = 1 
        left outer join curated_v2.dimSewerNetwork snw 
		on lp.sewerScamp = snw.SCAMP and snw._RecordCurrent = 1 
        left outer join curated_v2.dimStormWaterNetwork sw 
		on lp.stormWaterCatchment = sw.stormWaterCatchment and sw._RecordCurrent = 1
        where propertyNumber is not null 
        and lp._RecordCurrent = 1 ), 
							
systemareas as (select * 
                from t5
                where rnk = 1 ),
dummy as (
select 
--'-1' as propertyNumber,
wn.waterNetworkSK as waterNetworkSK_drinkingWater1, wnr.waterNetworkSK as waterNetworkSK_recycledWater1, snw.sewerNetworkSK as sewerNetworkSK1, sw.stormWaterNetworkSK as stormWaterNetworkSK1 
                            from curated_v2.dimWaterNetwork wn, 
                                 curated_v2.dimWaterNetwork wnr, 
                                curated_v2.dimSewerNetwork snw, 
                                 curated_v2.dimStormWaterNetwork sw 
                            where wn.pressureArea = 'Unknown' 
                            and   wn._RecordCurrent = 1 and wn.isPotableWaterNetwork='Y' 
                            and   wnr.supplyZone = 'Unknown' 
                            and   wnr._RecordCurrent = 1 and wnr.isRecycledWaterNetwork='Y' 
                            and   snw.SCAMP = 'Unknown' 
                            and   snw._RecordCurrent = 1 
                            and   sw.stormWaterCatchment = 'Unknown' 
                            and   sw._RecordCurrent = 1 
		),
		
  
									 
ISU as ( select con.propertyNumber as propertyNumber, 
                                coalesce(pre.premise,'-1') as premise, 
                                potableSK as waterNetworkSK_drinkingWater, 
                                recycledSK as waterNetworkSK_recycledWater, 
                                sewerNetworkSK, 
                                stormWaterNetworkSK, 
                                con.objectNumber as objectNumber, 
                                con.propertyInfo as propertyInfo, 
                                con.buildingFeeDate as buildingFeeDate, 
                                con.validFromDate as connectionValidFromDate , 
                                con.CRMConnectionObjectGUID as CRMConnectionObjectGUID, 
                                vn.architecturalObjectNumber as architecturalObjectNumber, 
                                vn.architecturalObjectTypeCode as architecturalObjectTypeCode, 
                                vn.architecturalObjectType as architecturalObjectType,
                                vn.parentArchitecturalObjectTypeCode as parentArchitecturalObjectTypeCode,
                                vn.parentArchitecturalObjectType as parentArchitecturalObjectType,
                                vn.parentArchitecturalObjectNumber as parentArchitecturalObjectNumber,
                                vd.hydraBand as hydraBand,
                                coalesce(vd.hydraCalculatedArea,-1) as hydraCalculatedArea,
                                coalesce(vd.hydraAreaUnit,'Unknown') as hydraAreaUnit,
                                vd.overrideArea as overrideArea,
                                vd.overrideAreaUnit as overrideAreaUnit,
                                coalesce(vd.stormWaterAssessmentFlag,'N') as stormWaterAssessmentFlag,
                                coalesce(vd.hydraAreaFlag,'N') as hydraAreaFlag,
                                vd.comments as comments,
                                con._RecordCurrent as _RecordCurrent,
                                con._recordDeleted as _recordDeleted
                                
                        from cleansed.isu_0uc_connobj_attr_2 con
left outer join cleansed.isu_vibdnode vn on vn.architecturalObjectNumber = con.propertyNumber and vn._RecordDeleted = 0 and   vn._RecordCurrent = 1
left outer join cleansed.isu_vibdao vd on vd.architecturalObjectInternalId = vn.architecturalObjectInternalId and vd._RecordDeleted = 0 and vd._RecordCurrent = 1 
left outer join cleansed.isu_0ucpremise_attr_2 pre on pre.propertyNumber = vn.architecturalObjectNumber
 left outer join systemAreas sa 
                              on sa.propertyNumber = coalesce(int(vn.parentArchitecturalObjectNumber),int(con.propertyNumber)) 
                         where con.propertyNumber <> '' 
                         and   con._RecordDeleted = 0 
                         and   con._RecordCurrent = 1),			
		

				
src as (
		select propertyNumber, 
        premise,
        coalesce(a.waterNetworkSK_drinkingWater, b.waterNetworkSK_drinkingWater1) as waterNetworkSK_drinkingWater, 
		coalesce(a.waterNetworkSK_recycledWater, b.waterNetworkSK_recycledWater1) as waterNetworkSK_recycledWater,
		coalesce(a.sewerNetworkSK, b.sewerNetworkSK1) as sewerNetworkSK,
		coalesce(a.stormWaterNetworkSK, b.stormWaterNetworkSK1) as stormWaterNetworkSK	
        ,objectNumber
        ,propertyInfo
        ,buildingFeeDate
        ,connectionValidFromDate
        ,CRMConnectionObjectGUID
        ,architecturalObjectNumber
        ,architecturalObjectTypeCode
        ,architecturalObjectType
        ,parentArchitecturalObjectNumber
        ,parentArchitecturalObjectTypeCode
        ,parentArchitecturalObjectType
        ,hydraBand
        ,hydraCalculatedArea
        ,case when hydraAreaUnit =' '  then 'Unknown' else hydraAreaUnit end as hydraAreaUnit
        ,overrideArea
        ,overrideAreaUnit
        ,stormWaterAssessmentFlag
        ,hydraAreaFlag
        ,comments
        ,_RecordCurrent
        ,_RecordDeleted
		from ISU a,
		dummy as b 
		)

		Select * from src """)


# COMMAND ----------

sdf.createOrReplaceTempView("source_view")
#display(source_isu)
#source_isu.count()

src_a = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=1 and _recordDeleted=0 ")
src_d = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=0 and _recordDeleted=1 ")
src_a.createOrReplaceTempView("src_a")
src_d.createOrReplaceTempView("src_d")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from source_view

# COMMAND ----------

# DBTITLE 1,Define Variables for ATF
keyColumns = 'propertyNumber'
mandatoryColumns = 'propertyNumber,premise,hydraCalculatedArea,hydraAreaUnit'

columns = ("""
propertyNumber
,sourceSystemCode
,premise
,waterNetworkSK_drinkingWater
,waterNetworkSK_recycledWater
,sewerNetworkSK
,stormWaterNetworkSK
,objectNumber
,propertyInfo
,buildingFeeDate
,connectionValidFromDate
,CRMConnectionObjectGUID
,architecturalObjectNumber
,architecturalObjectTypeCode
,architecturalObjectType
,parentArchitecturalObjectNumber
,parentArchitecturalObjectTypeCode
,parentArchitecturalObjectType
,hydraBand
,hydraCalculatedArea
,hydraAreaUnit
,overrideArea
,overrideAreaUnit
,stormWaterAssessmentFlag
,hydraAreaFlag
,comments
""")

#source_a = spark.sql(f"""
#--Select {columns}
#--From src_a
#""")

#--source_d = spark.sql(f"""
#--Select {columns}
#--From src_d
#""")
#

# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()

# COMMAND ----------

  target_isu= spark.sql("""
select
propertyNumber
--,sourceSystemCode
,premise
,waterNetworkSK_drinkingWater
,waterNetworkSK_recycledWater
,sewerNetworkSK
,stormWaterNetworkSK
,objectNumber
,propertyInfo
,buildingFeeDate
,connectionValidFromDate
,CRMConnectionObjectGUID
,architecturalObjectNumber
,architecturalObjectTypeCode
,architecturalObjectType
,parentArchitecturalObjectNumber
,parentArchitecturalObjectTypeCode
,parentArchitecturalObjectType
,hydraBand
,hydraCalculatedArea
,hydraAreaUnit
,overrideArea
,overrideAreaUnit
,stormWaterAssessmentFlag
,hydraAreaFlag
,comments
,_RecordCurrent
,_RecordDeleted
from
curated_v2.dimProperty
 WHERE sourceSystemCode='ISU'""")
    
target_isu.createOrReplaceTempView("target_view")
target_a = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from target_view where _RecordCurrent=1 and _recordDeleted=0 ")
target_d = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from target_view where _RecordCurrent=0 and _recordDeleted=1 ")
target_a.createOrReplaceTempView("target_a")
target_d.createOrReplaceTempView("target_d")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from src_d

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from target_d

# COMMAND ----------

src_act=spark.sql("select * from src_a")
tgt_act=spark.sql("select * from target_a ") 
print("Source Count:",src_act.count())
print("Target Count:",tgt_act.count())
diff1=src_act.subtract(tgt_act)
diff2=tgt_act.subtract(src_act)

# COMMAND ----------

print("Source-Target count is : ", diff1.count())
display(diff1)
print("Target-Source count is : ", diff2.count())
display(diff2)

# COMMAND ----------

# DBTITLE 1,S vs T
# MAGIC %sql
# MAGIC select
# MAGIC propertyNumber
# MAGIC --,sourceSystemCode
# MAGIC ,premise
# MAGIC ,waterNetworkSK_drinkingWater
# MAGIC ,waterNetworkSK_recycledWater
# MAGIC ,sewerNetworkSK
# MAGIC ,stormWaterNetworkSK
# MAGIC ,objectNumber
# MAGIC ,propertyInfo
# MAGIC ,buildingFeeDate
# MAGIC ,connectionValidFromDate
# MAGIC ,CRMConnectionObjectGUID
# MAGIC ,architecturalObjectNumber
# MAGIC ,architecturalObjectTypeCode
# MAGIC ,architecturalObjectType
# MAGIC ,parentArchitecturalObjectNumber
# MAGIC ,parentArchitecturalObjectTypeCode
# MAGIC ,parentArchitecturalObjectType
# MAGIC ,hydraBand
# MAGIC ,hydraCalculatedArea
# MAGIC ,case when hydraAreaUnit =' '  then 'Unknown' else hydraAreaUnit end as hydraAreaUnit
# MAGIC ,overrideArea
# MAGIC ,overrideAreaUnit
# MAGIC ,stormWaterAssessmentFlag
# MAGIC ,hydraAreaFlag
# MAGIC ,comments
# MAGIC 
# MAGIC from
# MAGIC src_a
# MAGIC except
# MAGIC select
# MAGIC propertyNumber
# MAGIC --,sourceSystemCode
# MAGIC ,premise
# MAGIC ,waterNetworkSK_drinkingWater
# MAGIC ,waterNetworkSK_recycledWater
# MAGIC ,sewerNetworkSK
# MAGIC ,stormWaterNetworkSK
# MAGIC ,objectNumber
# MAGIC ,propertyInfo
# MAGIC ,buildingFeeDate
# MAGIC ,connectionValidFromDate
# MAGIC ,CRMConnectionObjectGUID
# MAGIC ,architecturalObjectNumber
# MAGIC ,architecturalObjectTypeCode
# MAGIC ,architecturalObjectType
# MAGIC ,parentArchitecturalObjectNumber
# MAGIC ,parentArchitecturalObjectTypeCode
# MAGIC ,parentArchitecturalObjectType
# MAGIC ,hydraBand
# MAGIC ,hydraCalculatedArea
# MAGIC ,hydraAreaUnit
# MAGIC ,overrideArea
# MAGIC ,overrideAreaUnit
# MAGIC ,stormWaterAssessmentFlag
# MAGIC ,hydraAreaFlag
# MAGIC ,comments
# MAGIC 
# MAGIC from
# MAGIC curated_v2.dimProperty
# MAGIC  WHERE sourceSystemCode='ISU'

# COMMAND ----------

# DBTITLE 1,T vs S
# MAGIC %sql
# MAGIC select
# MAGIC propertyNumber
# MAGIC --,sourceSystemCode
# MAGIC ,premise
# MAGIC ,waterNetworkSK_drinkingWater
# MAGIC ,waterNetworkSK_recycledWater
# MAGIC ,sewerNetworkSK
# MAGIC ,stormWaterNetworkSK
# MAGIC ,objectNumber
# MAGIC ,propertyInfo
# MAGIC ,buildingFeeDate
# MAGIC ,connectionValidFromDate
# MAGIC ,CRMConnectionObjectGUID
# MAGIC ,architecturalObjectNumber
# MAGIC ,architecturalObjectTypeCode
# MAGIC ,architecturalObjectType
# MAGIC ,parentArchitecturalObjectNumber
# MAGIC ,parentArchitecturalObjectTypeCode
# MAGIC ,parentArchitecturalObjectType
# MAGIC ,hydraBand
# MAGIC ,hydraCalculatedArea
# MAGIC ,hydraAreaUnit
# MAGIC ,overrideArea
# MAGIC ,overrideAreaUnit
# MAGIC ,stormWaterAssessmentFlag
# MAGIC ,hydraAreaFlag
# MAGIC ,comments
# MAGIC 
# MAGIC from
# MAGIC curated_v2.dimProperty
# MAGIC  WHERE sourceSystemCode='ISU'
# MAGIC  except
# MAGIC  select
# MAGIC propertyNumber
# MAGIC --,sourceSystemCode
# MAGIC ,premise
# MAGIC ,waterNetworkSK_drinkingWater
# MAGIC ,waterNetworkSK_recycledWater
# MAGIC ,sewerNetworkSK
# MAGIC ,stormWaterNetworkSK
# MAGIC ,objectNumber
# MAGIC ,propertyInfo
# MAGIC ,buildingFeeDate
# MAGIC ,connectionValidFromDate
# MAGIC ,CRMConnectionObjectGUID
# MAGIC ,architecturalObjectNumber
# MAGIC ,architecturalObjectTypeCode
# MAGIC ,architecturalObjectType
# MAGIC ,parentArchitecturalObjectNumber
# MAGIC ,parentArchitecturalObjectTypeCode
# MAGIC ,parentArchitecturalObjectType
# MAGIC ,hydraBand
# MAGIC ,hydraCalculatedArea
# MAGIC ,case when hydraAreaUnit =' '  then 'Unknown' else hydraAreaUnit end as hydraAreaUnit
# MAGIC ,overrideArea
# MAGIC ,overrideAreaUnit
# MAGIC ,stormWaterAssessmentFlag
# MAGIC ,hydraAreaFlag
# MAGIC ,comments
# MAGIC from
# MAGIC src_a

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.isu_0ucpremise_attr_2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT TRIM(leading '0' from premise) as premise FROM cleansed.isu_0ucpremise_attr_2

# COMMAND ----------

# MAGIC %sql
# MAGIC select architecturalObjectNumber from cleansed.isu_vibdnode

# COMMAND ----------

# MAGIC %sql
# MAGIC select architecturalObjectNumber from curated_v2.dimProperty
