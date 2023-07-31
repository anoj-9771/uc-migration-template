# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET_TABLE=_.Destination

# COMMAND ----------

# DBTITLE 1,Create table upfront as the main query needs it
spark.sql(f"""
	CREATE TABLE IF NOT EXISTS {TARGET_TABLE}
	(
		assetDemandValueSK STRING NOT NULL,
		sourceSystemCode STRING NOT NULL,
		assetHierarchystring STRING  NOT NULL ,
		reportDate           DATE  NOT NULL ,
		priorityNumber       INTEGER  NOT NULL ,
		assetTypeCode        STRING  NOT NULL ,
		assetCode            STRING  NOT NULL ,
		resultValueNumber          DECIMAL(18,10),
		readValueNumber            DECIMAL(18,10),
		reservoirCapacityNumber    DECIMAL(18,10),
		reservoirPercentage  DECIMAL(18,10),
		errorText            ARRAY<STRING>,
		warningText          ARRAY<STRING>,
		_BusinessKey		 STRING NOT NULL,
		_DLCuratedZoneTimeStamp TIMESTAMP NOT NULL,
		_recordEnd			 TIMESTAMP NOT NULL,
		_recordCurrent		 INTEGER NOT NULL,
		_recordDeleted		 INTEGER NOT NULL,
  		_recordStart		 TIMESTAMP NOT NULL
	)
 	{f"USING DELTA LOCATION '{_.DataLakePath}'" if len(TARGET_TABLE.split('.'))<=2 else ''}
"""
)

# COMMAND ----------

df = spark.sql(f"""
WITH DEVICE_ERROR_CONFIG AS
( 
	  --only include entries here when there's no record for the combination assetTypeCode, ObjectName in curated.viewDemandCalculatePriorityConfig
    SELECT 'Flowmeter' assetTypeCode,'Flow' ObjectName,'SN' statisticalTypeCode,'15' tmBaseCode
    UNION ALL
    SELECT 'Flowmeter','Flow 15M CV','SN','15'
    UNION ALL
    SELECT 'FlowmeterReverse','Flow Bidirectional','AV','15'
),
HIER AS
(
	SELECT  *
	FROM
	(
		SELECT  *
		       ,MIN(if(pointName = 'Volume Remaining',objectinternalid,null)) over (partition by assetHierarchyString,assetCd,assetTypeCode) res_capacity_objectinternalid
		FROM
		(
			SELECT  objectinternalid
			       ,effectiveFromDateTime
			       ,Row_Number () over ( partition BY objectinternalid,assetTypeCode ORDER BY effectiveFromDateTime desc ) AS ROW_NO
			       ,pointName
			       ,pointDescription
			       ,siteCd
			       ,siteName
			       ,facilityCd
			       ,assetCd
			       ,facilityAssetCd
			       ,userAssetNumber
			       ,dac.assetTypeCode
			       ,dac.assetHierarchystring
			FROM {get_table_namespace(SOURCE, 'iicats_hierarchy_cnfgn')} hc
			JOIN {get_table_namespace(DEFAULT_TARGET, 'viewDemandAssetConfig')} dac
			ON (hc.siteCd = dac.siteCode AND hc.facilityCd = dac.facilityCode AND (dac.assetCode is null or hc.assetCd = dac.assetCode) )
			WHERE hc.sourceRecordSystemId in(89, 90)
      and hc.objectInternalId < 20000000              
      qualify row_no = 1
    )
    WHERE (assetTypeCode, pointName) in (
                                                select replace(assetTypeCode,'ReservoirCapacity', 'Reservoir'), ObjectName
                                                from {get_table_namespace(DEFAULT_TARGET, 'viewDemandCalculatePriorityConfig')}
                                                union all
                                                select assetTypeCode, ObjectName
                                                from device_error_config                                            
                                              )      
  )
  WHERE pointName <> 'Volume Remaining'
),
REQUIRED_DATES as
(
	SELECT  explode(sequence(minDate,maxDate)) reportDate
	FROM
	(        
        -- select date'2022-06-30' minDate,
        --     date'2023-07-03' maxDate

        select 
        coalesce((select max(reportDate) + 1 from {TARGET_TABLE}),
                min(measurementResultAESTDateTime)::DATE
        ) minDate
        , nvl2(max(measurementResultAESTDateTime)::DATE,least(max(measurementResultAESTDateTime)::DATE,current_date()-1),null) maxDate
        From {get_table_namespace(SOURCE, 'iicats_tsv')}
  )
  WHERE minDate <= maxDate
),
PLMT as
(
	SELECT  pointInternalId
	       ,effectiveFromDateTime
	       ,pointLimitTypeCd
	       ,pointAlarmTypeCd
	       ,limitValue
	       ,sourceRecordUpsertLogic
	       ,sourceRecordCreationDateTime
	       ,Row_Number () over ( partition BY pointInternalId ,pointLimitTypeCd ORDER BY effectiveFromDateTime desc ) AS ROW_NO
	FROM {get_table_namespace(SOURCE, 'iicats_point_limit')}
  -- Using clone for testing
  -- FROM default.iicats_point_limit_clone
	WHERE pointLimitTypeCd IN (
                        select distinct reservoirLimitcode
                        from {get_table_namespace(DEFAULT_TARGET, 'viewDemandCalculatePriorityConfig')}
                        where objectName = 'Volume Remaining'
                      )
  qualify row_no = 1                
),
HIER_WITH_RES_CAPACITY AS
(
	SELECT  * except(priorityNumber,minPriorityNumber)
	FROM
	(
		SELECT  HIER.*
		       ,plmt.limitValue reservoirCapacityNumber
		       ,plmt.pointLimitTypeCd
		       ,priorityNumber
		       ,MIN(dcpc.priorityNumber) over ( partition BY HIER.assetHierarchystring,HIER.assetCd,HIER.assetTypeCode ) minPriorityNumber
		FROM hier
		LEFT JOIN plmt
		ON hier.res_capacity_objectinternalid = plmt.pointInternalId
		LEFT JOIN {get_table_namespace(DEFAULT_TARGET, 'viewDemandCalculatePriorityConfig')} dcpc
		ON ('Volume Remaining' = dcpc.ObjectName 
        AND 'ReservoirCapacity' = dcpc.assetTypeCode 
        AND plmt.pointLimitTypeCd = dcpc.reservoirLimitcode )
	)
	WHERE priorityNumber = minPriorityNumber or priorityNumber is null
),
ASSET_DATES as
(
	SELECT  *
	FROM
	(
		SELECT  distinct assetHierarchyString
		       ,assetTypeCode
		       ,assetCd
		       ,reservoirCapacityNumber
		FROM hier_with_res_capacity
	)
	CROSS JOIN required_dates
),
PCNF as
(
	SELECT  objectInternalId
	       ,effectiveFromDateTime
	      --  ,Row_Number () over ( partition BY objectInternalId,pointStatisticTypeCd ,timeBaseCd ORDER BY effectiveFromDateTime desc ) AS ROW_NO
         ,Row_Number () over ( partition BY objectInternalId ORDER BY effectiveFromDateTime desc ) AS ROW_NO
	       ,pointStatisticTypeCd
	       ,timeBaseCd
	       ,pointTimeBaseinSeconds
	       ,pointInternalId
	FROM {get_table_namespace(SOURCE, 'iicats_tsv_point_cnfgn')}
	WHERE sourceRecordSystemId in(89, 90) 
  qualify row_no = 1
),
TSV as
(
	SELECT  T.objectInternalId
	       ,T.measurementResultAESTDateTime
        --Temporary for Nov data. Uncomment when generating march, and apr data
        --  ,from_utc_timestamp(to_utc_timestamp(T.measurementResultAESTDateTime, 'Australia/Sydney'), 'UTC+10') measurementResultAESTDateTime
	       ,T.statisticTypeCd
	       ,T.timeBaseCd
	       ,T.acquisitionMethodId
	       ,T.measurementResultDateTime
	       ,T.tsvMnemonicCd
	       ,T.measurementResultValue
 				 ,T.pointFailedIndicator
				 ,T.pointOutofServiceIndicator
				 ,T.opcQualityIndicator
				 ,T.badOrSuspectIndicator
				 ,T.maintenanceIndicator         
	       ,Row_Number () over ( partition BY objectInternalId,measurementResultAESTDateTime,statisticTypeCd,timeBaseCd,sourceRecordUpsertLogic ORDER BY sourceRecordCreationDateTime desc ) AS ROW_NO
        --Temporary for Nov data. Uncomment when generating march, and apr data
        -- ,Row_Number () over ( partition BY objectInternalId,from_utc_timestamp(to_utc_timestamp(T.measurementResultAESTDateTime, 'Australia/Sydney'), 'UTC+10'),statisticTypeCd,timeBaseCd,sourceRecordUpsertLogic ORDER BY sourceRecordCreationDateTime desc ) AS ROW_NO
	FROM {get_table_namespace(SOURCE, 'iicats_tsv')} T
  --Using clone for testing
  -- FROM default.iicats_tsv_clone T
	-- WHERE measurementResultAESTDateTime >= (SELECT  MIN(reportDate) FROM required_dates) 
  --Min(reportDate) - 1 is to read last 24 hours of device maintenance reading
  WHERE measurementResultAESTDateTime >= (SELECT  MIN(reportDate)-1 FROM required_dates) 
  AND measurementResultAESTDateTime < (SELECT  MAX(reportDate)+1 FROM required_dates)
  AND sourceRecordSystemId in(1, 2)
	--test missing tsv values
	-- AND measurementResultAESTDateTime::DATE <> date'2022-12-01' 
  qualify row_no = 1
),
ASSET_CALC as
(
	SELECT  assetHierarchystring
	       ,reportDate
	       ,priorityNumber
	       ,assetTypeCode
	       ,assetCode
	      --  ,readValueNumber - prevReadValueNumber resultValueNumber
           ,if(assetTypeCode = 'Reservoir'
              ,readValueNumber - prevReadValueNumber
              ,if(readValueNumber - prevReadValueNumber < 0.00
                 ,if(readValueNumber > 300.00,0,readValueNumber)
                 ,readValueNumber - prevReadValueNumber)) resultValueNumber        
	       ,readValueNumber
	       ,transform(array_remove(array(
            flowmeterDeviceErrorText
            ,reservoirDeviceErrorText
            -- ,nullif(completeAssetHierarchyString||'.'||assetTypeCode||' => '||
            ,trim(
                  CASE 
                      WHEN assetTypeCode = 'Reservoir' THEN
                        CASE
                          --  WHEN reservoirCapacityNumber is null THEN 'Reservoir capacity is not available.'
                          WHEN prevReadValueNumber is null and readValueNumber is null THEN 'Possible data loss on '||reportDate||' and '||prevReportDate||'.'
                          WHEN prevReadValueNumber is null THEN 'Possible data loss on '||prevReportDate||'.'
                          WHEN readValueNumber is null THEN 'Possible data loss on '||reportDate||'.'
                          WHEN prevReservoirPercentage < 0.00 and reservoirPercentage < 0.00 THEN 'Reservoir level percentage is negative on '||reportDate||' and '||prevReportDate||'.'
                          WHEN prevReservoirPercentage < 0.00 THEN 'Reservoir level percentage is negative on '||prevReportDate||'.'
                          WHEN reservoirPercentage < 0.00 THEN 'Reservoir level percentage is negative on '||reportDate||'.'
                          -- WHEN prevReservoirPercentage < 0.00
                          --   or reservoirPercentage < 0.00
                          --   THEN 
                          --     -- if(prevReservoirPercentage > 100.00, 'Reservoir percentage is greater than 100 on '||prevReportDate||'.', '')
                          --     if(prevReservoirPercentage < 0.00, ' Reservoir level percentage is negative on '||prevReportDate||'.', '')
                          --     -- ||if(reservoirPercentage > 100.00, ' Reservoir percentage is greater than 100 on '||reportDate||'.', '')
                          --     ||if(reservoirPercentage < 0.00, ' Reservoir level percentage is negative on '||reportDate||'.', '')
                          -- --  WHEN reservoirPercentage > 100.00 THEN 'Reservoir level percentage is greater than 100 on '||reportDate||'.'
                          -- --  WHEN reservoirPercentage < 0.00 THEN 'Reservoir level percentage is negative on '||reportDate||'.'
                          ELSE ''
                        END
                        --  || ' ' 
                        --  || if(prevReservoirPercentage > 100.00, 'Reservoir percentage is greater than 100 on '||prevReportDate||'.', '')
                        --  || if(prevReservoirPercentage < 0.00, 'Reservoir percentage is negative on '||prevReportDate||'.', '')
                      ELSE --Flowmeter,FlowmeterReverse,TotalUse
                        CASE
                          WHEN prevReadValueNumber is null and readValueNumber is null THEN 'Possible data loss on '||reportDate||' and '||prevReportDate||'.'
                          WHEN prevReadValueNumber is null THEN 'Possible data loss on '||prevReportDate||'.'
                          WHEN readValueNumber is null THEN 'Possible data loss on '||reportDate||'.'
                          WHEN readValueNumber - prevreadValueNumber < 0.00 THEN 'Possible Flow Meter Reset in last 24 hours'
                          ELSE ''
                        END             
                  END 
                 )
            -- ,completeAssetHierarchyString||'.'||assetTypeCode||' => ') 
            ),''),x -> if(x<>'',completeAssetHierarchyString||'.'||assetTypeCode||' => '||x,x)) errorText
         ,reservoirCapacityNumber
         ,reservoirPercentage
        --  case 
        --      when prevAVWarningCode in ('AVC','AVA') and AVWarningCode = 'AVC' then 'AVA'
        --      when prevAVWarningCode in ('AVC','AVA') then 'AVP'
        --      else AVWarningCode
        --  end AVWarningCode,
	       ,transform(array_remove(array(
            flowmeterDeviceWarningText
            ,reservoirDeviceWarningText        
        --  nullif(completeAssetHierarchyString||'.'||assetTypeCode||' => '||
            ,trim(
                -- case 
                --     when prevAVWarningCode in ('AVC','AVA') and AVWarningCode = 'AVC' then 'Snap readings on '||reportDate||' and '||prevReportDate||' are not available, used average reading to compute demand.'
                --     when prevAVWarningCode in ('AVC','AVA') then 'Snap reading on '||prevReportDate||' is not available, used average reading to compute demand.'
                --     when AVWarningCode = 'AVC' then 'Snap reading on '||reportDate||' is not available, used average reading to compute demand.'
                -- end 
                -- || 
                -- if(assetTypeCode <> 'Reservoir' and readValueNumber - prevReadValueNumber = 0.00, ' Review Flowmeter volume on '||reportDate||'.', '')           
                  CASE 
                      WHEN assetTypeCode = 'Reservoir' THEN
                        CASE
                          WHEN prevReservoirPercentage > 100.00 and reservoirPercentage > 100.00 THEN 'Probable reservoir overflow on '||reportDate||' and '||prevReportDate||'.'
                          WHEN prevReservoirPercentage > 100.00 THEN 'Probable reservoir overflow on '||prevReportDate||'.'
                          WHEN reservoirPercentage > 100.00 THEN 'Probable reservoir overflow on '||reportDate||'.'
                          ELSE ''
                        END
                      ELSE --Flowmeter,FlowmeterReverse,TotalUse
                        CASE
                          WHEN readValueNumber - prevReadValueNumber = 0.00 THEN ' Review Flowmeter volume on '||reportDate||'.'
                          ELSE ''
                        END             
                  END 
            )
          -- ,completeAssetHierarchyString||'.'||assetTypeCode||' => ') warningText
            ),''),x -> if(x<>'',completeAssetHierarchyString||'.'||assetTypeCode||' => '||x,x)) warningText          
  FROM
  (
    SELECT *
          -- ,AVWarningCode
          ,lag(readValueNumber) over (partition by assetHierarchystring,assetCode,assetTypeCode ORDER BY reportDate) prevReadValueNumber
          -- ,lag(AVWarningCode) over (partition by assetHierarchystring,assetCode,assetTypeCode ORDER BY reportDate) prevAVWarningCode
          ,lag(reservoirPercentage) over (partition by assetHierarchystring,assetCode,assetTypeCode ORDER BY reportDate) prevReservoirPercentage
          ,lag(reportDate,1,reportDate - 1) over (partition by assetHierarchystring,assetCode,assetTypeCode ORDER BY reportDate) prevReportDate
          ,regexp_replace(assetHierarchyString,'\\.'||assetCode||'$','')||'.'||assetCode completeAssetHierarchyString
    FROM
    (
      SELECT  
              ad.assetHierarchystring
              ,ad.reportDate
              ,coalesce(priorityNumber,0) priorityNumber
              ,ad.assetTypeCode
              ,ad.assetCd assetCode
              ,if(pre.assetTypeCode = 'Reservoir',nvl2(pre.measurementResultValue,least(greatest(pre.measurementResultValue::DECIMAL(18,10),0.00),100.00),pre.measurementResultValue) * coalesce(pre.reservoirCapacityNumber,0.00) / 100.00,pre.measurementResultValue) readValueNumber
              ,ad.reservoirCapacityNumber
              ,if(pre.assetTypeCode = 'Reservoir',measurementResultValue,null) reservoirPercentage
              ,if(size(flowmeterDeviceErrorList)>0,
                'Possible device failure on '||array_join(flowmeterDeviceErrorList, ' and ')||'.'
                ,if(deviceMaintenanceReadingCount=0 and pre.assetTypeCode not in ('Reservoir','TotalUse')
                  ,'Device maintenance reading such as Flow is unavailable.'
                  ,'')) flowmeterDeviceErrorText
              ,if(size(flowmeterDeviceWarningList)>0,
                'Possible device failure on '||array_join(flowmeterDeviceWarningList, ' and ')||'.','') flowmeterDeviceWarningText
              ,if(size(reservoirDeviceErrorList)>0,
                'Possible device failure on '||array_join(reservoirDeviceErrorList, ' and ')||'.','') reservoirDeviceErrorText
              ,if(size(reservoirDeviceWarningList)>0,
                'Possible device failure on '||array_join(reservoirDeviceWarningList, ' and ')||'.','') reservoirDeviceWarningText
              -- ,if(TSV.statisticTypeCd = 'AV','AVC',null) AVWarningCode
              ,'N' existingRecord
      FROM
      (
        SELECT *
          ,MIN(priorityNumber) over ( partition BY assetHierarchystring,assetCd,assetTypeCode,reportDate ) minPriorityNumber
        FROM
        (
          SELECT * except(measurementResultAESTDateTime)
              ,measurementResultAESTDateTime::DATE reportDate
          FROM
          (
            SELECT HIER.assetHierarchystring
                ,HIER.assetCd
                ,HIER.assetTypeCode
                ,HIER.pointName
                ,HIER.reservoirCapacityNumber
                ,dcpc.priorityNumber
                ,dcpc.timeCode
                ,TSV.measurementResultValue
                ,TSV.measurementResultAESTDateTime
                ,array_distinct(collect_set(
                  if(dec.ObjectName is not null
                    and (pointFailedIndicator=1 or pointOutofServiceIndicator=1
                    or opcQualityIndicator<192 or badOrSuspectIndicator=1)
                    ,TSV.measurementResultAESTDateTime::DATE,null))
                  over last24hours) flowmeterDeviceErrorList
                ,array_distinct(collect_set(
                  if(dec.ObjectName is not null
                    and (maintenanceIndicator=1)
                    ,TSV.measurementResultAESTDateTime::DATE,null))
                  over last24hours) flowmeterDeviceWarningList
                ,sum(if(dec.ObjectName is not null,1,0))
                  over last24hours deviceMaintenanceReadingCount	
                ,array_distinct(collect_set(
                  if(hier.pointName = 'Level'
                    and date_format(TSV.measurementResultAESTDateTime, 'HH:mm:ss') = lpad(dcpc.timeCode, 2, '0')||':00:00'
                    and (pointFailedIndicator=1 or pointOutofServiceIndicator=1
                      or opcQualityIndicator<192 or badOrSuspectIndicator=1)
                    ,TSV.measurementResultAESTDateTime::DATE,null))
                  over last24hoursByPriority) reservoirDeviceErrorList		
                ,array_distinct(collect_set(
                  if(hier.pointName = 'Level'
                    and date_format(TSV.measurementResultAESTDateTime, 'HH:mm:ss') = lpad(dcpc.timeCode, 2, '0')||':00:00'
                    and (maintenanceIndicator=1)
                    ,TSV.measurementResultAESTDateTime::DATE,null))
                  over last24hoursByPriority) reservoirDeviceWarningList								 
            FROM hier_with_res_capacity hier
            JOIN pcnf
            ON (HIER.objectInternalId = PCNF.pointInternalId)
            LEFT JOIN device_error_config dec
            ON   (HIER.pointName = dec.ObjectName
              AND HIER.assetTypeCode = dec.assetTypeCode)
            LEFT JOIN {get_table_namespace(DEFAULT_TARGET, 'viewDemandCalculatePriorityConfig')} dcpc
            ON   (HIER.pointName = dcpc.ObjectName
              AND HIER.assetTypeCode = dcpc.assetTypeCode)
            JOIN tsv
            ON (TSV.objectInternalId = PCNF.objectInternalId
              AND (
                  TSV.statisticTypeCd = dcpc.statisticalTypeCode
                  AND TSV.timeBaseCd = dcpc.tmBaseCode    
                  AND TSV.acquisitionMethodId = dcpc.acquistionMethodCode
                  OR
                  TSV.statisticTypeCd = dec.statisticalTypeCode
                  AND TSV.timeBaseCd = dec.tmBaseCode    
                )
            )
            WINDOW last24hours as (
              partition BY hier.assetHierarchystring,hier.assetCd,hier.assetTypeCode 
              order by TSV.measurementResultAESTDateTime
              range between interval '24' hours preceding and current row
            ), last24hoursByPriority as (
              partition BY hier.assetHierarchystring,hier.assetCd,hier.assetTypeCode,dcpc.priorityNumber
              order by TSV.measurementResultAESTDateTime
              range between interval '24' hours preceding and current row
            )
          )
          WHERE date_format(measurementResultAESTDateTime, 'HH:mm:ss') = lpad(timeCode, 2, '0')||':00:00'
        ) 
        qualify priorityNumber = minPriorityNumber
      )	pre
      RIGHT JOIN asset_dates ad
        ON   (pre.assetHierarchystring = ad.assetHierarchystring
          AND pre.assetCd = ad.assetCd
          AND pre.assetTypeCode = ad.assetTypeCode
          AND pre.reportDate = ad.reportDate)
      UNION ALL
      SELECT  assetHierarchystring
            ,reportDate
            ,priorityNumber
            ,assetTypeCode
            ,assetCode
            ,readValueNumber
            ,reservoirCapacityNumber
            ,reservoirPercentage
            ,null
            ,null
            ,null
            ,null
            -- ,AVWarningCode
            ,'Y' existingRecord
      FROM {TARGET_TABLE}
      WHERE reportdate = (SELECT  MIN(reportDate) FROM required_dates) - 1		
    )      
  )
  WHERE existingRecord = 'N'
)    
SELECT 'IICATS' sourceSystemCode
       ,assetHierarchystring
       ,reportDate
       ,priorityNumber
       ,assetTypeCode
       ,assetCode
       ,resultValueNumber::DECIMAL(18,10)
       ,readValueNumber::DECIMAL(18,10)
       ,reservoirCapacityNumber::DECIMAL(18,10)
       ,reservoirPercentage::DECIMAL(18,10)
      --  ,AVWarningCode
       ,if(size(errorText)>0,errorText,null) errorText
       ,if(size(warningText)>0,warningText,null) warningText
FROM asset_calc
"""
)

# COMMAND ----------

def Transform():
    # ------------- TABLES ----------------- #
    global df
    # ------------- JOINS ------------------ #
        
    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"assetHierarchystring||'|'||assetTypeCode||'|'||assetCode||'|'||reportDate {BK}"
        ,"sourceSystemCode"
        ,"assetHierarchystring"
        ,"reportDate"
        ,"priorityNumber"
        ,"assetTypeCode"
        ,"assetCode"
        ,"resultValueNumber"        
        ,"readValueNumber"        
        ,"reservoirCapacityNumber"     
        ,"reservoirPercentage"
        ,"errorText"
        ,"warningText"
    ]
    df = df.selectExpr(
        _.Transforms
    )
    # ------------- CLAUSES ---------------- #

    # ------------- SAVE ------------------- #
    # display(df)
    # CleanSelf()
    Save(df, append=True)
    #DisplaySelf()
Transform()
