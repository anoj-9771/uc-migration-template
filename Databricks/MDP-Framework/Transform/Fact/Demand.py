# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

TARGET_TABLE=_.Destination

# COMMAND ----------

# DBTITLE 1,Create table upfront as the main query needs it
spark.sql(f"""
	CREATE TABLE IF NOT EXISTS {TARGET_TABLE}
	(
		demandSK STRING NOT NULL,
		demandWaterNetworkSK STRING NOT NULL ,
		sourceSystemCode STRING NOT NULL,
		reportDate DATE NOT NULL ,
		alternateFlag STRING NOT NULL ,
		totalDemandQuantity DECIMAL(18,10) ,
		flowmeterDemandQuantity DECIMAL(18,10) ,
		reservoirCorrectionQuantity DECIMAL(18,10) ,
		assetCodeList ARRAY<STRUCT<operand: STRING, assetCode: STRING, resultValueNumber: DECIMAL(18,10), readValueNumber: DECIMAL(18,10)>>,
		demandErrorText ARRAY<STRING> ,
		flowmeterErrorText ARRAY<STRING> ,
		reservoirErrorText ARRAY<STRING> ,
		flowmeterWarningText ARRAY<STRING> ,
		reservoirWarningText ARRAY<STRING> ,
		flowmeterErrorCount INTEGER ,
		reservoirErrorCount INTEGER ,
		flowmeterWarningCount INTEGER ,
		reservoirWarningCount INTEGER,
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

import pandas as pd

def try_eval(formula: str):
    try:
        if formula:
            return eval(formula), ''
        else:
            return None, ''    
    except Exception as e:
        return None, f'Error in evaluating the formula "{formula}" {str(e)}'

@pandas_udf("result float, errorText string")
def eval_func(formula: pd.Series) -> pd.DataFrame:
    output = formula.apply(try_eval)
    result = output.apply(lambda x: x[0])
    errorText = output.apply(lambda x: x[1])
    return pd.DataFrame({'result': result, 'errorText': errorText})

df = spark.sql(f"""
WITH OPERANDS as
(
  SELECT  * except(operand)
      ,regexp_replace(operand,'[()]','') operand
      ,regexp_replace(regexp_replace(operand,'[()]',''),'\\.'||slice(split(regexp_replace(operand,'[()]',''),'[.]'),-1,1)[0]||'$','') operandAsset
      ,slice(split(regexp_replace(operand,'[()]',''),'[.]'),-1,1)[0] operandAssetType
  FROM
  (
    SELECT 
            explode(array_union(if(coalesce(flowmeterFormulaString,'')='', array(), split(flowmeterFormulaString,'[-+]')),
                                if(coalesce(reservoirFormulaString,'')='', array(), split(reservoirFormulaString,'[-+]')))
                    ) operand                                 
            , *
    FROM
    (
        SELECT  * except(flowmeterFormulaString,reservoirFormulaString)
                ,regexp_replace(flowmeterFormulaString,'[\\\s"]','') flowmeterFormulaString 
                ,regexp_replace(reservoirFormulaString,'[\\\s"]','') reservoirFormulaString
        FROM {get_table_namespace(DEFAULT_TARGET, 'viewDemandCalculateConfig')}
        WHERE current_date() BETWEEN startDate AND endDate 
    )
  )
),
REQUIRED_DATES as
(
  SELECT  explode(sequence(minDate,maxDate)) reportDate
  FROM
  (            
    SELECT 
          coalesce((select max(reportDate) + 1 from {TARGET_TABLE}),
                          min(ReportDate)
          ) minDate
          , max(ReportDate) maxDate
    FROM {get_table_namespace(DEFAULT_TARGET, 'factAssetDemandValue')}
  )
  WHERE minDate <= maxDate
),
OPERAND_DATES as
(
  SELECT  *
  FROM
  (
  	SELECT  *
	FROM operands
  )
  CROSS JOIN required_dates
),
ADV as
(
  SELECT  regexp_replace(assetHierarchyString,'\\.'||assetCode||'$','') facilityHierarchyString
         ,reportDate
         ,assetCode
         ,assetTypeCode
         ,resultValueNumber
         ,readValueNumber
         ,errorText
         ,warningText
  FROM {get_table_namespace(DEFAULT_TARGET, 'factAssetDemandValue')}
  WHERE reportDate >= (select min(reportDate) from required_dates) 
    AND reportDate <= (select max(reportDate) from required_dates)   
),
OPERAND_VALUES as
(
	SELECT  deliverySystem
	       ,distributionSystem
	       ,supplyZone
	       ,pressureZone
	       ,SystemExport
	       ,flowmeterFormulaString
	       ,reservoirFormulaString
	       ,networkTypeCode
	       ,reportDate
	       ,alternateFlag
           ,array_sort(collect_set(struct(operand, resultValueNumber))
                      --sort by length desc then alphabetically. Sort by length desc ensures that if operand is at assetCd level, that will be replaced
                      -- first before trying to replace facilityCd level operand
                      , (left, right) -> CASE WHEN length(left.operand) > length(right.operand) THEN -1
                                           WHEN length(left.operand) < length(right.operand) THEN 1 
                                           WHEN left.operand > right.operand THEN 1
                                           WHEN left.operand < right.operand THEN -1
                                           ELSE 0 END
                      ) operandList
           ,array_sort(flatten(collect_set(assetCodeList))) assetCodeList                    
           ,array_sort(flatten(collect_set(flowmeterErrorText))) flowmeterErrorText
           ,array_sort(flatten(collect_set(reservoirErrorText))) reservoirErrorText
           ,array_sort(flatten(collect_set(flowmeterWarningText))) flowmeterWarningText
           ,array_sort(flatten(collect_set(reservoirWarningText))) reservoirWarningText           
           ,sum(flowmeterErrorCount)::INT flowmeterErrorCount
           ,sum(reservoirErrorCount)::INT reservoirErrorCount
           ,sum(flowmeterWarningCount)::INT flowmeterWarningCount
           ,sum(reservoirWarningCount)::INT reservoirWarningCount                     
  FROM
  (
    SELECT  od.deliverySystem
          ,od.distributionSystem
          ,od.supplyZone
          ,od.pressureZone
          ,od.SystemExport
          ,od.flowmeterFormulaString
          ,od.reservoirFormulaString
          ,od.networkTypeCode
          ,od.reportDate
          ,od.alternateFlag
          ,od.operand
          ,od.operandAssetType
          ,f.operand _
          ,coalesce(SUM(resultValueNumber),0.00)::DECIMAL(18,10) resultValueNumber
          ,collect_set(struct(od.operand
                               , adv.facilityHierarchyString
                                 ||if(assetCode is not null, '.'||assetCode, '') assetCode
                               , resultValueNumber::DECIMAL(18,10) resultValueNumber
                               , readValueNumber::DECIMAL(18,10) readValueNumber
                               )
                        ) assetCodeList            
          ,array_union(
                        flatten(collect_set(errorText) filter(where assetTypeCode <> 'Reservoir'))
                        , if(f.operand is null and od.operandAssetType <> 'Reservoir'
                            , array(od.operand||" either do not exist in iicats_hierarchy_cnfgn or it has not been setup in {get_table_namespace(DEFAULT_TARGET, 'viewDemandAssetConfig')}.")
                            , array())
                      ) flowmeterErrorText
          ,array_union(
                        flatten(collect_set(errorText) filter(where assetTypeCode = 'Reservoir'))
                        , if(f.operand is null and od.operandAssetType = 'Reservoir'
                            , array(od.operand||" either do not exist in iicats_hierarchy_cnfgn or it has not been setup in {get_table_namespace(DEFAULT_TARGET, 'viewDemandAssetConfig')}.")
                            , array())
                      ) reservoirErrorText             
          ,flatten(collect_set(warningText) filter(where assetTypeCode <> 'Reservoir')) flowmeterWarningText
          ,flatten(collect_set(warningText) filter(where assetTypeCode = 'Reservoir')) reservoirWarningText        
          ,if(f.operand is null and od.operandAssetType = 'Flowmeter', 1, 0) + count(errorText) filter(where assetTypeCode <> 'Reservoir') flowmeterErrorCount
          ,if(f.operand is null and od.operandAssetType = 'Reservoir', 1, 0) + count(errorText) filter(where assetTypeCode = 'Reservoir') reservoirErrorCount
          ,count(warningText) filter(where assetTypeCode <> 'Reservoir') flowmeterWarningCount
          ,count(warningText) filter(where assetTypeCode = 'Reservoir') reservoirWarningCount            
    FROM operands f
    JOIN adv
    on ((f.operandAsset = adv.facilityHierarchyString
        or f.operandAsset = adv.facilityHierarchyString||'.'||assetCode)
        and f.operandAssetType = adv.assetTypeCode
        ) 
    FULL JOIN operand_dates od
    on  ( f.deliverySystem <=> od.deliverySystem
      and f.distributionSystem <=> od.distributionSystem
      and f.supplyZone <=> od.supplyZone
      and f.pressureZone <=> od.pressureZone
      and f.SystemExport <=> od.SystemExport
      and f.flowmeterFormulaString <=> od.flowmeterFormulaString
      and f.reservoirFormulaString <=> od.reservoirFormulaString
      and f.networkTypeCode <=> od.networkTypeCode
      and f.alternateFlag <=> od.alternateFlag
      and f.startDate <=> od.startDate
      and f.endDate <=> od.endDate
      and f.operand = od.operand
      and adv.reportDate = od.reportDate)          
    GROUP BY  od.deliverySystem
        ,od.distributionSystem
        ,od.supplyZone
        ,od.pressureZone
        ,od.SystemExport
        ,od.flowmeterFormulaString
        ,od.reservoirFormulaString
        ,od.networkTypeCode
        ,od.reportDate
        ,od.alternateFlag
        ,od.operand
        ,od.operandAssetType
        ,f.operand
  )
  GROUP BY  deliverySystem
          ,distributionSystem
          ,supplyZone
          ,pressureZone
          ,SystemExport
          ,flowmeterFormulaString
          ,reservoirFormulaString
          ,networkTypeCode
          ,reportDate
          ,alternateFlag
),
DEMAND_CALC as
(
  SELECT  v_dwn.demandWaterNetworkSK
          ,ov.*
  FROM
  (
      SELECT  *
              ,reduce(operandList,flowmeterFormulaString,(acc,x) -> replace(acc,x.operand,x.resultValueNumber)) replacedFlowmeterFormulaString
              ,reduce(operandList,reservoirFormulaString,(acc,x) -> replace(acc,x.operand,x.resultValueNumber)) replacedReservoirFormulaString
      FROM operand_values
  ) ov
  JOIN {get_table_namespace(DEFAULT_TARGET, 'viewDemandWaterNetwork')} v_dwn
  on  ( ov.networkTypeCode = v_dwn.networkTypeCode
    and (ov.deliverySystem = v_dwn.deliverySystem 
    or ov.distributionSystem = v_dwn.distributionSystem
    or ov.supplyZone = v_dwn.supplyZone
    or ov.pressureZone = v_dwn.pressureArea
    or ov.SystemExport = v_dwn.SystemExport)  
  )
)
SELECT  *
FROM demand_calc
"""
)
df = (
    df.withColumn('sourceSystemCode', lit('IICATS'))
      .withColumn('flowmeterDemandStruct', eval_func('replacedFlowmeterFormulaString'))
      .withColumn('reservoirCorrectionStruct', eval_func('replacedReservoirFormulaString'))          
      .withColumn('flowmeterDemandQuantity', col('flowmeterDemandStruct.result').cast("decimal(18,10)"))
      .withColumn('reservoirCorrectionQuantity', col('reservoirCorrectionStruct.result').cast("decimal(18,10)"))
      .withColumn('totalDemandQuantity', expr('coalesce(flowmeterDemandQuantity,0.00) - coalesce(reservoirCorrectionQuantity,0.00)').cast("decimal(18,10)"))
      .withColumn('demandErrorText', 
                    expr("""
                      transform(array_remove(array(
                            if(flowmeterDemandStruct.errorText <> '',
                              'Evaluation of flowmeter formula resulted in error: '||flowmeterDemandStruct.errorText,'')
                            ,if(reservoirCorrectionStruct.errorText <> '',
                              'Evaluation of reservoir formula resulted in error: '||reservoirCorrectionStruct.errorText,'')
                            ,if(networkTypeCode<>'System Export'
                                 and coalesce(flowmeterDemandQuantity,0.00) - coalesce(reservoirCorrectionQuantity,0.00) = 0.00, 
                              'Demand computation is zero on '||reportDate||'.', '')
                            ,if(coalesce(flowmeterDemandQuantity,0.00) - coalesce(reservoirCorrectionQuantity,0.00) < 0.00, 
                              'Demand computation is negative on '||reportDate||'.', '')  
                           ),''),x -> if(x<>'',
                                case when networkTypeCode in ('Delivery System','Delivery System Combined','Desalination Plant') 
                                        then deliverySystem
                                     when networkTypeCode in ('Distribution System')    
                                        then distributionSystem                                     
                                     when networkTypeCode in ('Supply Zone')
                                        then supplyZone                                     
                                     when networkTypeCode in ('Pressure Area')    
                                        then pressureZone   
                                     when networkTypeCode in ('System Export')
                                        then SystemExport   
                                    else ''                             
                                end        
                                ||' => '||x,x))
                    """)
                 )
      .withColumn('demandErrorText',expr('if(size(demandErrorText)>0,demandErrorText,null)'))
      .withColumn('flowmeterErrorText',expr('if(size(flowmeterErrorText)>0,flowmeterErrorText,null)'))
      .withColumn('reservoirErrorText',expr('if(size(reservoirErrorText)>0,reservoirErrorText,null)'))
      .withColumn('flowmeterWarningText',expr('if(size(flowmeterWarningText)>0,flowmeterWarningText,null)'))
      .withColumn('reservoirWarningText',expr('if(size(reservoirWarningText)>0,reservoirWarningText,null)'))                        
      .drop('flowmeterDemandStruct', 'reservoirCorrectionStruct')
)

# COMMAND ----------

def Transform():
    # ------------- TABLES ----------------- #
    global df
    # ------------- JOINS ------------------ #
        
    # ------------- TRANSFORMS ------------- #
    _.Transforms = [
        f"demandWaterNetworkSK||'|'||alternateFlag||'|'||reportDate {BK}"
        ,"demandWaterNetworkSK"
        ,"sourceSystemCode"
        ,"reportDate"
        ,"alternateFlag"
        ,"totalDemandQuantity"
        ,"flowmeterDemandQuantity"
        ,"reservoirCorrectionQuantity"        
        ,"assetCodeList"        
        ,"demandErrorText"     
        ,"flowmeterErrorText"
        ,"reservoirErrorText"
        ,"flowmeterWarningText"
        ,"reservoirWarningText"
        ,"flowmeterErrorCount"
        ,"reservoirErrorCount"
        ,"flowmeterWarningCount"
        ,"reservoirWarningCount"                                             
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
