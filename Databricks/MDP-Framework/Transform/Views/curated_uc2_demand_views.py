# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

db = DEFAULT_TARGET

# COMMAND ----------

def CreateView(db: str, view: str, sql_: str):
    table_namespace = get_table_namespace(db, view)
    schema_name = '.'.join(table_namespace.split('.')[:-1])
    object_name = table_namespace.split('.')[-1]

    if len(table_namespace.split('.')) > 2:
        catalog_name = schema_name.split('.')[0]
        spark.sql(f"USE CATALOG {catalog_name}")

    if spark.sql(f"SHOW VIEWS FROM {schema_name} LIKE '{object_name}'").count() == 1:
        sqlLines = f"ALTER VIEW {table_namespace} AS"
    else:
        sqlLines = f"CREATE VIEW {table_namespace} AS"        

    sqlLines += sql_
    print(sqlLines)
    spark.sql(sqlLines)    

# COMMAND ----------

# DBTITLE 1,viewManualDemand
view = "viewManualDemand"
sql_ = f"""
SELECT   adj.reportDate
        ,adj.deliverySystem
        ,adj.demandQuantity
        ,adj.calendarDate
        ,adj.createdDate
        ,user.Name createdByName
        ,adj.commentText
FROM {get_table_namespace(SOURCE, 'sharepointlistedp_demandcalculationadjustment')} adj
JOIN {get_table_namespace(SOURCE, 'sharepointlistedp_userinformationlist')} user
ON adj.createdById = user.Id
"""

CreateView(db, view, sql_)

# COMMAND ----------

# DBTITLE 1,viewDemandWaterNetwork
view = "viewDemandWaterNetwork"
sql_ = f"""
select distinct MD5(supplyZone) demandWaterNetworkSK,deliverySystem,distributionSystem,supplyZone,null as pressureArea ,null as SystemExport ,  'Supply Zone' as networkTypeCode
from {get_table_namespace(DEFAULT_TARGET, 'dimwaternetwork')} 
where _RecordCurrent =1 

UNION 
select MD5(pressureArea),deliverySystem,distributionSystem,supplyZone,pressureArea ,null as SystemExport, 'Pressure Area' as networkTypeCode
from {get_table_namespace(DEFAULT_TARGET, 'dimwaternetwork')}
where _RecordCurrent =1 
and pressureArea  is not null
UNION  
select distinct MD5(distributionSystem), deliverySystem,  distributionSystem,null supplyZone,null as pressureArea,null as SystemExport, 'Distribution System' as networkTypeCode
from {get_table_namespace(DEFAULT_TARGET, 'dimwaternetwork')}
where _RecordCurrent =1
UNION 
select distinct MD5(deliverySystem),deliverySystem, null as distributionSystem,null supplyZone,null as pressureArea,null as SystemExport, 'Delivery System' as networkTypeCode
from {get_table_namespace(DEFAULT_TARGET, 'dimwaternetwork')}
where _RecordCurrent =1  
UNION  ALL
 select distinct MD5('DEL_CASCADE + DEL_ORCHARD_HILLS')   ,'DEL_CASCADE + DEL_ORCHARD_HILLS' as deliverySystem,null as distributionSystem,null supplyZone,null as pressureArea, null as SystemExport,'Delivery System Combined' as networkTypeCode

UNION ALL

 select distinct MD5('DEL_POTTS_HILL + DEL_PROSPECT_EAST')  ,'DEL_POTTS_HILL + DEL_PROSPECT_EAST' as deliverySystem,null as  distributionSystem,null supplyZone,null as pressureArea, null as SystemExport,'Delivery System Combined' as networkTypeCode

UNION ALL
 select distinct MD5('DESALINATION PLANT')  ,'DESALINATION PLANT' as deliverySystem,null as distributionSystem,null supplyZone,null as pressureArea,null as SystemExport,'Desalination Plant' as networkTypeCode

 UNION ALL
  select distinct MD5('Warragamba Backwash')  ,'DEL_WARRAGAMBA' as deliverySystem,null as distributionSystem,null supplyZone,null as pressureArea , 'Warragamba Backwash' as SystemExport,'System Export' as networkTypeCode

UNION  ALL
  select distinct MD5('Port Kembla Coal Terminal Recycle Water Top-up')  ,'DEL_ILLAWARRA' as deliverySystem,null as distributionSystem,null supplyZone,null as pressureArea , 'Port Kembla Coal Terminal Recycle Water Top-up' as SystemExport,'System Export' as networkTypeCode

UNION ALL
  select distinct MD5('Rouse Hill Recycled Water - Potable Top Up') ,'DEL_PROSPECT_NORTH' as deliverySystem,null as distributionSystem,null supplyZone,null as pressureArea , 'Rouse Hill Recycled Water - Potable Top Up' as SystemExport,'System Export' as networkTypeCode
"""

CreateView(db, view, sql_)

# COMMAND ----------

# DBTITLE 1,viewFactWaterNetworkDemandWithExports
view = "viewFactWaterNetworkDemandWithExports"
sql_ = f"""
WITH md AS
( 
  SELECT  *
          ,Row_Number () over ( partition BY deliverySystem,reportDate ORDER BY calendarDate desc ) AS ROW_NO
  FROM {get_table_namespace(DEFAULT_TARGET, 'viewManualDemand')}
  qualify row_no = 1
)
SELECT  * except(chooseAlternateFlag)
FROM
(
	SELECT  fd.reportDate
	       ,wn.deliverySystem
	       ,wn.distributionSystem
	       ,wn.supplyZone
	       ,wn.pressureArea
	       ,wn.networkTypeCode
	       ,nvl2(md.reportDate,demandQuantity,fd.totalDemandQuantity) demandQuantity
	       ,md.calendarDate manualInputDate
         ,nvl2(md.reportDate,'NA',fd.alternateFlag) alternateFlag
	       ,nvl2(md.reportDate,'Y','NA') manualDemandFlag
	       ,nvl2(md.reportDate,null,fd.demandErrorText) demandErrorText
	       ,nvl2(md.reportDate,null,fd.flowmeterErrorText) flowmeterErrorText
	       ,nvl2(md.reportDate,null,fd.reservoirErrorText) reservoirErrorText
	       ,nvl2(md.reportDate,null,fd.flowmeterWarningText) flowmeterWarningText
	       ,nvl2(md.reportDate,null,fd.reservoirWarningText) reservoirWarningText
	       ,nvl2(md.reportDate,0,fd.flowmeterErrorCount) flowmeterErrorCount
	       ,nvl2(md.reportDate,0,fd.reservoirErrorCount) reservoirErrorCount
	       ,nvl2(md.reportDate,0,fd.flowmeterWarningCount) flowmeterWarningCount
	       ,nvl2(md.reportDate,0,fd.reservoirWarningCount) reservoirWarningCount
          ,if(
              sum(
                if(fd.alternateFlag = 'N'
                  --,flowmeterErrorCount + reservoirErrorCount + nvl2(demandErrorText,1,0),0))
                  ,flowmeterErrorCount,0))
                over waterNetworkPerDay > 0
              and
              sum(
                if(fd.alternateFlag = 'Y'
                  --,flowmeterErrorCount + reservoirErrorCount + nvl2(demandErrorText,1,0),0))
                  ,flowmeterErrorCount,0))                  
                over waterNetworkPerDay = 0
			        and
              sum(
                if(fd.alternateFlag = 'Y',1,0))
                over waterNetworkPerDay > 0    				
              , 'Y', 'N') chooseAlternateFlag
  FROM {get_table_namespace(DEFAULT_TARGET, 'factdemand')} fd
  JOIN {get_table_namespace(DEFAULT_TARGET, 'viewDemandWaterNetwork')} wn
  ON wn.demandWaterNetworkSK = fd.demandWaterNetworkSK
  LEFT JOIN md
  ON   (wn.deliverySystem = md.deliverySystem
    AND fd.reportDate = md.reportDate
    AND wn.networkTypeCode IN ('Delivery System', 'Delivery System Combined'))
  WHERE wn.networkTypeCode <> 'System Export'  
  WINDOW waterNetworkPerDay as (
    partition BY fd.demandWaterNetworkSK, fd.reportDate
  )
  qualify chooseAlternateFlag = alternateFlag
);
"""

CreateView(db, view, sql_)

# COMMAND ----------

# DBTITLE 1,viewSystemExportVolume
view = "viewSystemExportVolume"
sql_ = f"""
WITH md AS
( 
  SELECT  *
          ,Row_Number () over ( partition BY deliverySystem,reportDate ORDER BY calendarDate desc ) AS ROW_NO
  FROM {get_table_namespace(DEFAULT_TARGET, 'viewManualDemand')}
  qualify row_no = 1
)
SELECT * 
FROM
(
	SELECT  fd.reportDate
	       ,wn.SystemExport
	       ,nvl2(md.reportDate,demandQuantity,fd.totalDemandQuantity) exportQuantity
	       ,md.calendarDate manualInputDate
	       ,nvl2(md.reportDate,'Y','NA') manualDemandFlag
         ,nvl2(md.reportDate,null,fd.demandErrorText) exportErrorText
         ,nvl2(md.reportDate,null,fd.flowmeterErrorText) flowmeterErrorText
         ,nvl2(md.reportDate,null,fd.flowmeterWarningText) flowmeterWarningText
         ,nvl2(md.reportDate,0,fd.flowmeterErrorCount) flowmeterErrorCount
         ,nvl2(md.reportDate,0,fd.flowmeterWarningCount) flowmeterWarningCount        	       
  FROM {get_table_namespace(DEFAULT_TARGET, 'factdemand')} fd
  JOIN {get_table_namespace(DEFAULT_TARGET, 'viewDemandWaterNetwork')} wn
  ON wn.demandWaterNetworkSK = fd.demandWaterNetworkSK
  LEFT JOIN md
  ON   (wn.SystemExport = md.deliverySystem
    AND fd.reportDate = md.reportDate
    AND wn.networkTypeCode IN ('System Export'))
  WHERE wn.networkTypeCode = 'System Export'
);
"""

CreateView(db, view, sql_)

# COMMAND ----------

# DBTITLE 1,viewInterimFactWaterNetworkDemand
view = "viewInterimFactWaterNetworkDemand"
sql_ = f"""
WITH v AS
(
  SELECT  fd.reportDate
          ,fd.deliverySystem
          ,fd.distributionSystem
          ,fd.supplyZone
          ,fd.pressureArea
          -- ,nvl2(sev.SystemExport,'Delivery System Export Compensated',fd.networkTypeCode) networkTypeCode
          ,fd.networkTypeCode
          ,fd.demandQuantity - nvl2(sev.SystemExport,sev.exportQuantity,0) demandQuantity
          ,greatest(fd.manualInputDate, sev.manualInputDate) manualInputDate
          ,fd.alternateFlag
          ,greatest(fd.manualDemandFlag, sev.manualDemandFlag) manualDemandFlag
          ,array_remove(array(
              if(fd.manualDemandFlag='Y'
                ,case when fd.networkTypeCode in ('Delivery System','Delivery System Combined','Desalination Plant') 
                        then fd.deliverySystem
                      when fd.networkTypeCode in ('Distribution System')    
                        then fd.distributionSystem                                     
                      when fd.networkTypeCode in ('Supply Zone')
                        then fd.supplyZone                                     
                      when fd.networkTypeCode in ('Pressure Area')    
                        then fd.pressureArea   
                      else ''                             
                  end,'')
              ,if(sev.manualDemandFlag='Y',sev.SystemExport,'')
              ),'') manualOverrides
          ,array_remove(array(
              if(fd.flowmeterErrorCount + fd.reservoirErrorCount + nvl2(fd.demandErrorText,1,0) > 0
                ,case when fd.networkTypeCode in ('Delivery System','Delivery System Combined','Desalination Plant') 
                        then fd.deliverySystem
                      when fd.networkTypeCode in ('Distribution System')    
                        then fd.distributionSystem                                     
                      when fd.networkTypeCode in ('Supply Zone')
                        then fd.supplyZone                                     
                      when fd.networkTypeCode in ('Pressure Area')    
                        then fd.pressureArea   
                      else ''                             
                  end,'')
              ,if(sev.flowmeterErrorCount + nvl2(sev.exportErrorText,1,0) > 0,sev.SystemExport,'')
              ),'') falseReadings              
          ,array_union(coalesce(fd.demandErrorText,array()),coalesce(sev.exportErrorText,array())) demandErrorText
          ,array_union(coalesce(fd.flowmeterErrorText,array()),coalesce(sev.flowmeterErrorText,array())) flowmeterErrorText
          ,fd.reservoirErrorText
          ,array_union(coalesce(fd.flowmeterWarningText,array()),coalesce(sev.flowmeterWarningText,array())) flowmeterWarningText
          ,fd.reservoirWarningText
          ,fd.flowmeterErrorCount + nvl2(sev.SystemExport,sev.flowmeterErrorCount,0) flowmeterErrorCount
          ,fd.reservoirErrorCount
          ,fd.flowmeterWarningCount + nvl2(sev.SystemExport,sev.flowmeterWarningCount,0) flowmeterWarningCount
          ,fd.reservoirWarningCount
          ,nvl2(sev.SystemExport,'Y','NA') exportCompensatedFlag
  FROM {get_table_namespace(DEFAULT_TARGET, 'viewFactWaterNetworkDemandWithExports')} fd
  LEFT JOIN {get_table_namespace(DEFAULT_TARGET, 'viewDemandWaterNetwork')} dwn
  ON   (fd.deliverySystem = dwn.deliverySystem  
    AND dwn.networkTypeCode = 'System Export')
  LEFT JOIN {get_table_namespace(DEFAULT_TARGET, 'viewSystemExportVolume')} sev
  ON   (dwn.SystemExport = sev.SystemExport
    AND fd.reportDate = sev.reportDate
    AND fd.networkTypeCode IN ('Delivery System', 'Delivery System Combined'))
)
SELECT reportDate
       ,deliverySystem
       ,distributionSystem
       ,supplyZone
       ,pressureArea
       ,networkTypeCode
       ,demandQuantity
       ,manualInputDate
       ,alternateFlag
       ,manualDemandFlag
       ,if(size(manualOverrides)>0,manualOverrides,null) manualOverrides
       ,if(size(falseReadings)>0,falseReadings,null) falseReadings
       ,if(size(demandErrorText)>0,demandErrorText,null) demandErrorText
       ,if(size(flowmeterErrorText)>0,flowmeterErrorText,null) flowmeterErrorText
       ,reservoirErrorText
       ,if(size(flowmeterWarningText)>0,flowmeterWarningText,null) flowmeterWarningText
       ,reservoirWarningText
       ,flowmeterErrorCount
       ,reservoirErrorCount
       ,flowmeterWarningCount
       ,reservoirWarningCount
       ,exportCompensatedFlag
FROM v;
"""

CreateView(db, view, sql_)

# COMMAND ----------

# DBTITLE 1,viewFactWaterNetworkDemand
view = "viewFactWaterNetworkDemand"
sql_ = f"""
SELECT reportDate
       ,deliverySystem
       ,distributionSystem
       ,supplyZone
       ,pressureArea
       ,networkTypeCode
       ,demandQuantity
       ,manualInputDate
       ,alternateFlag
       ,manualDemandFlag
       ,demandErrorText
       ,flowmeterErrorText
       ,reservoirErrorText
       ,flowmeterWarningText
       ,reservoirWarningText
       ,flowmeterErrorCount
       ,reservoirErrorCount
       ,flowmeterWarningCount
       ,reservoirWarningCount
       ,exportCompensatedFlag
FROM {get_table_namespace(DEFAULT_TARGET, 'viewInterimFactWaterNetworkDemand')};
"""

CreateView(db, view, sql_)

# COMMAND ----------

# DBTITLE 1,viewSWCDemand
view = "viewSWCDemand"
sql_ = f"""
WITH v AS
( 
  -- SELECT  reportDate
  --       ,SUM(demandQuantity) demandQuantity
  --       ,collect_set(case when manualDemandFlag = 'Y'
  --                           then deliverySystem||' => Overwritten by Manual process'
  --                         when flowmeterErrorCount + reservoirErrorCount + nvl2(demandErrorText,1,0) > 0
  --                           then deliverySystem||' => False Reading'
  --                     end
  --                   ) errorText       
  -- FROM curated.viewFactWaterNetworkDemand
  -- WHERE networkTypeCode IN ('Delivery System', 'Delivery System Combined', 'Delivery System Export Compensated')
  -- GROUP BY  reportDate
  SELECT  reportDate
        ,SUM(demandQuantity) demandQuantity
        ,array_sort(flatten(collect_set(array_union(
          coalesce(transform(manualOverrides,x -> if(x<>'',x||' => Overwritten by Manual process',x)),array())
          ,coalesce(transform(falseReadings,x -> if(x<>'',x||' => False Reading',x)),array())
        )))) errorText
  FROM {get_table_namespace(DEFAULT_TARGET, 'viewInterimFactWaterNetworkDemand')}
  WHERE networkTypeCode IN ('Delivery System', 'Delivery System Combined')
    AND deliverySystem NOT IN ('DEL_CASCADE','DEL_ORCHARD_HILLS')
  GROUP BY  reportDate    
)
SELECT * except(errorText)
       ,if(size(errorText)>0,errorText,null) errorText
FROM v;
"""

CreateView(db, view, sql_)

# COMMAND ----------

# DBTITLE 1,manualDemandHistorical
spark.sql(f"""
    create or replace view {get_env()}curated.water_balance.manualdemandhistorical as ( 
    select * from {get_env()}cleansed.iicats.manualdemandhistorical
    )
    """)
