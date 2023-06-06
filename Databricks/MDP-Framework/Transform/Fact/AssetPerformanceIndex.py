# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ---------- 

# MAGIC %run ../../Common/common-helpers 
# COMMAND ---------- 


# COMMAND ----------

# CleanSelf()

# COMMAND ----------

def monthly_Transform():
    df = spark.sql(f"""SELECT
    assetNumber||'|'||reportingYear||'|'|| reportingMonth||'|'||calculationTypeCode||'|'||orderID {BK}, 
    assetNumber, 
    assetSK as assetFK,
    reportingYear, 
    reportingMonth,
    calculationTypeCode,
    orderID, 
    selectedPeriodStartDate,
    selectedPeriodEndDate,
    comparisonPeriodStartDate,
    comparisonPeriodEndDate,
    selectedPeriodBreakdownMaintenanceCount,
    comparisonPeriodBreakdownMaintenanceCount, 
    yearlyAverageBreakdownMaintenanceCount, 
    selectedPeriodFailedAssetsCount,
    selectedPeriodRepeatedlyFailedAssetsCount,
    CLASSTYPE
    FROM(
    SELECT
        dt.assetNumber,
        dt.assetSK,
        EXTRACT (YEAR FROM DT.SELECTED_PERIOD_END_DATE) reportingYear,
        EXTRACT (MONTH FROM DT.SELECTED_PERIOD_END_DATE) reportingMonth,
        'M' calculationTypeCode,
        DT.ORDER_ID as orderID,
        DT.SELECTED_PERIOD_START_DATE as selectedPeriodStartDate,
        DT.SELECTED_PERIOD_END_DATE as selectedPeriodEndDate,
        DT.COMPARISON_PERIOD_START_DATE as comparisonPeriodStartDate,
        DT.COMPARISON_PERIOD_END_DATE as comparisonPeriodEndDate,
        COUNT( CASE WHEN fwo.workOrderReportedDateTimestamp BETWEEN SELECTED_PERIOD_START_DATE AND SELECTED_PERIOD_END_DATE THEN 1 END ) selectedPeriodBreakdownMaintenanceCount,
        COUNT( CASE WHEN fwo.workOrderReportedDateTimestamp BETWEEN COMPARISON_PERIOD_START_DATE AND COMPARISON_PERIOD_END_DATE THEN 1 END ) comparisonPeriodBreakdownMaintenanceCount,
        COUNT( CASE WHEN fwo.workOrderReportedDateTimestamp BETWEEN COMPARISON_PERIOD_START_DATE AND COMPARISON_PERIOD_END_DATE THEN 1 END )/5 yearlyAverageBreakdownMaintenanceCount,
        CASE WHEN COUNT( CASE WHEN fwo.workOrderReportedDateTimestamp BETWEEN SELECTED_PERIOD_START_DATE AND SELECTED_PERIOD_END_DATE THEN 1 END )>0 THEN 1 ELSE 0 END selectedPeriodFailedAssetsCount,
        CASE WHEN COUNT( CASE WHEN fwo.workOrderReportedDateTimestamp BETWEEN SELECTED_PERIOD_START_DATE AND SELECTED_PERIOD_END_DATE THEN 1 END )>0 AND COUNT( CASE WHEN fwo.workOrderReportedDateTimestamp BETWEEN COMPARISON_PERIOD_START_DATE AND COMPARISON_PERIOD_END_DATE THEN 1 END )>0 THEN 1 ELSE 0 END selectedPeriodRepeatedlyFailedAssetsCount,
        'NONLINEAR' as classType 
        FROM
    ( 
    SELECT
        astloc.assetNumber,
        astloc.assetSK,
        MONTHS.M ORDER_ID,
        (ADD_MONTHS(LAST_DAY(CURRENT_DATE),-MONTHS.M+1-12)+1) SELECTED_PERIOD_START_DATE,
        (ADD_MONTHS(LAST_DAY(CURRENT_DATE),-MONTHS.M+1)) SELECTED_PERIOD_END_DATE,
        (ADD_MONTHS(LAST_DAY(CURRENT_DATE),-MONTHS.M+1-72)+1) COMPARISON_PERIOD_START_DATE,
        (ADD_MONTHS(LAST_DAY(CURRENT_DATE),-MONTHS.M+1-12)) COMPARISON_PERIOD_END_DATE
    FROM
    (
        SELECT DISTINCT da.assetNumber, da.assetSK
        FROM {get_table_namespace('curated', 'dimasset')} da
        inner join {get_table_namespace('curated', 'dimassetlocation')} dal
        on da.assetLocationFK = dal.assetLocationSK
    where da.sourceRecordCurrent = 1
        and dal.sourceRecordCurrent = 1
        and dal.assetLocationTypeCode IN ('SYSAREA','FACILITY','PROCESS','FUNCLOC') 
        and dal.assetLocationStatusDescription  = 'OPERATING'
        and  dal.assetLocationFacilityShortCode   in ('AV','CP','DB','DG','DP','DQ','DR','EG','FM','GE','GG','MV','NC',
            'NT','OO','P0','PR','RF','RK','RM','RN','RP','RQ','RS','RT','RX','SC','SF','SG','SK','SL','SM','SN','SO','SP',
            'SQ','SR','SS','ST','SU','SV','SW','SX','SY','TD','TP','TS','WA','WC','WD','WF','WG','WH','WK','WM','WN','WP','WQ','WS','WT','WU','WX','WZ')	
    ) astloc
    CROSS JOIN
    ( 
        SELECT 1 AS M UNION ALL
        SELECT 2 AS M UNION ALL
        SELECT 3 AS M UNION ALL
        SELECT 4 AS M UNION ALL
        SELECT 5 AS M UNION ALL
        SELECT 6 AS M UNION ALL
        SELECT 7 AS M UNION ALL
        SELECT 8 AS M UNION ALL
        SELECT 9 AS M UNION ALL
        SELECT 10 AS M UNION ALL
        SELECT 11 AS M UNION ALL
        SELECT 12 AS M UNION ALL
        SELECT 13 AS M
    ) MONTHS
    )DT
    INNER JOIN
    ( 
    select *  
    from {get_table_namespace('curated', 'factworkorder')}
    qualify row_number() over(partition by workOrderCreationId order by workOrderChangeDate desc)=1
    ) fwo
    ON dt.assetSK = fwo.assetFK     
    and fwo.workTypeCode = 'BM'
    and fwo.workOrderClassDescription = 'WORKORDER'
    and fwo.workOrderStatusDescription NOT IN ('CAN','CANDUP','DRAFT')
    and serviceTypeCode IN ('M','E','F','C')
    and financialControlIdentifier is NULL

    --and dt.assetNumber = '10640878'

    GROUP BY
    DT.assetNumber,
    DT.assetSK,
    DT.ORDER_ID,
    DT.SELECTED_PERIOD_START_DATE,
    DT.SELECTED_PERIOD_END_DATE,
    DT.COMPARISON_PERIOD_START_DATE,
    DT.COMPARISON_PERIOD_END_DATE

    UNION

    SELECT 
    WORKORDER.ASSET as assetNumber,
    dt.assetSK,
    EXTRACT (YEAR FROM DT.SELECTED_PERIOD_END_DATE) reportingYear,
    EXTRACT (MONTH FROM DT.SELECTED_PERIOD_END_DATE) reportingMonth,
    'M' calculationTypeCode,
    DT.ORDER_ID as orderID,
    DT.SELECTED_PERIOD_START_DATE as selectedPeriodStartDate,
    DT.SELECTED_PERIOD_END_DATE as selectedPeriodEndDate,
    DT.COMPARISON_PERIOD_START_DATE as comparisonPeriodStartDate,
    DT.COMPARISON_PERIOD_END_DATE as comparisonPeriodEndDate,
    COUNT( CASE WHEN WORKORDER.reportedDateTime BETWEEN SELECTED_PERIOD_START_DATE AND SELECTED_PERIOD_END_DATE THEN 1 END ) selectedPeriodBreakdownMaintenanceCount,
    COUNT( CASE WHEN WORKORDER.reportedDateTime BETWEEN COMPARISON_PERIOD_START_DATE AND COMPARISON_PERIOD_END_DATE THEN 1 END ) comparisonPeriodBreakdownMaintenanceCount,
    COUNT( CASE WHEN WORKORDER.reportedDateTime BETWEEN COMPARISON_PERIOD_START_DATE AND COMPARISON_PERIOD_END_DATE THEN 1 END )/5 yearlyAverageBreakdownMaintenanceCount,
    CASE WHEN COUNT( CASE WHEN WORKORDER.reportedDateTime BETWEEN SELECTED_PERIOD_START_DATE AND SELECTED_PERIOD_END_DATE THEN 1 END )>0 THEN 1 ELSE 0 END selectedPeriodFailedAssetsCount,
    CASE WHEN COUNT( CASE WHEN WORKORDER.reportedDateTime BETWEEN SELECTED_PERIOD_START_DATE AND SELECTED_PERIOD_END_DATE THEN 1 END )>0 AND 
    COUNT( CASE WHEN WORKORDER.reportedDateTime BETWEEN COMPARISON_PERIOD_START_DATE AND COMPARISON_PERIOD_END_DATE THEN 1 END )>0 THEN 1 ELSE 0 END selectedPeriodRepeatedlyFailedAssetsCount
    , 'LINEAR' as classType
    
    FROM  
    
    (SELECT 
    ASSETLIST.ASSETNUM,
    ASSETLIST.assetSK,
    MONTHS.M ORDER_ID,
    (ADD_MONTHS(LAST_DAY(CURRENT_DATE),-MONTHS.M+1-12)+1) SELECTED_PERIOD_START_DATE,
    (ADD_MONTHS(LAST_DAY(CURRENT_DATE),-MONTHS.M+1)) SELECTED_PERIOD_END_DATE,
    (ADD_MONTHS(LAST_DAY(CURRENT_DATE),-MONTHS.M+1-72)+1) COMPARISON_PERIOD_START_DATE,
    (ADD_MONTHS(LAST_DAY(CURRENT_DATE),-MONTHS.M+1-12)) COMPARISON_PERIOD_END_DATE

    FROM ( 
            SELECT DISTINCT (da.assetNumber) as ASSETNUM, da.assetSK
            FROM 
            ( 
            select *  
            from {get_table_namespace('curated', 'factworkorder')}
            qualify row_number() over(partition by workOrderCreationId order by workOrderChangeDate desc)=1
            ) fwo
            inner join {get_table_namespace('curated', 'dimasset')} da
            ON da.assetSK = fwo.assetFK  
            and da.sourceRecordCurrent = 1

            inner join {get_table_namespace('curated', 'dimassetlocation')} dal
            ON da.assetLocationFK = dal.assetLocationSK
            and dal.sourceRecordCurrent = 1
            and dal.assetLocationTypeCode IN ('SYSAREA','FACILITY','PROCESS','FUNCLOC') 
            and dal.assetLocationStatusDescription  = 'OPERATING'
            and (dal.assetLocationFacilityShortCode NOT IN ('AV','CP','DB','DG','DP','DQ','DR','EG','FM','GE','GG','MV','NC',
            'NT','OO','P0','PR','RF','RK','RM','RN','RP','RQ','RS','RT','RX','SC','SF','SG','SK','SL','SM','SN','SO','SP',
            'SQ','SR','SS','ST','SU','SV','SW','SX','SY','TD','TP','TS','WA','WC','WD','WF','WG','WH','WK','WM','WN','WP','WQ','WS','WT','WU','WX','WZ')
            OR
            dal.assetLocationFacilityCode IS NULL
            --dal.assetLocationFacilityCode IS NULL ***column rename not impemented***
            )
    ) ASSETLIST

    CROSS JOIN
    ( 
        SELECT 1 AS M UNION ALL
        SELECT 2 AS M UNION ALL
        SELECT 3 AS M UNION ALL
        SELECT 4 AS M UNION ALL
        SELECT 5 AS M UNION ALL
        SELECT 6 AS M UNION ALL
        SELECT 7 AS M UNION ALL
        SELECT 8 AS M UNION ALL
        SELECT 9 AS M UNION ALL
        SELECT 10 AS M UNION ALL
        SELECT 11 AS M UNION ALL
        SELECT 12 AS M UNION ALL
        SELECT 13 AS M
    ) MONTHS
    
    )DT
    inner join
    (
        select
        WORKORDER.workOrder, WORKORDER.asset, WORKORDER.worktype,
        WORKORDER.taskcode, WORKORDER.parentWo, WORKORDER.originatingRecord,
        WORKORDER.reportedDateTime,b.failurecode, parentWO.asset as parentAsset, oriWO.asset as oriAsset
        from 
        (
        select workOrder, asset, worktype, taskcode, parentWo, originatingRecord, reportedDateTime, Class as workorderClass, fCId, serviceType, STATUS
        from {get_table_namespace('cleansed', 'maximo_workorder')} 
        qualify row_number() over(partition by workOrder order by changeDate desc) =1
        ) WORKORDER
        left join 
        (
        SELECT failurecode, workOrder 
        FROM {get_table_namespace('cleansed', 'MAXIMO_FAILUREREPORT')} WHERE TYPE = 'REMEDY' 
        ) b 
        on b.workOrder=WORKORDER.workOrder 
        left join 
        (
        select asset, workOrder 
        from {get_table_namespace('cleansed', 'maximo_workorder')} 
        qualify row_number() over(partition by workOrder order by changeDate desc) =1
        )parentWO 
        on parentWO.workOrder = WORKORDER.parentWo
        left join 
        (
        select asset,workorder  
        from {get_table_namespace('cleansed', 'maximo_workorder')} 
        qualify row_number() over(partition by workOrder order by changeDate desc) =1
        )oriWO
        on oriWO.workorder = WORKORDER.originatingRecord
        where 
        (
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WD8C') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP2A') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP2R') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP3S') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WD8C' and b.failurecode = 'RW-WC3H') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WP4C' and b.failurecode = 'RW-WR4D') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WP1H' and b.failurecode = 'RW-WR1H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SR2B' and b.failurecode = 'RWW-SR2B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SR2B' and b.failurecode = 'RWW-SR2D') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SR2B' and b.failurecode = 'RWW-XX') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SR2B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SR2B' and b.failurecode = 'RWW-SR2E') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP2C') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SS1Z' and b.failurecode = 'RWW-SR2E') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP2A' and b.failurecode = 'RWW-SR8A') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR9A' and b.failurecode = 'RWW-SR2B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR9A' and b.failurecode = 'RWW-SR2E') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR9A' and b.failurecode = 'RW-WR1H') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP2B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR9A' and b.failurecode = 'RW-WR1V') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR9A' and b.failurecode = 'RW-WR1B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR9A' and b.failurecode = 'RW-WR3I') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SR2H' and b.failurecode = 'RWW-SR2B') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WP1H' and b.failurecode = 'RW-WR4D') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WC3Q' and b.failurecode = 'RW-WC3H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WC3Q' and b.failurecode = 'RW-WC3Q') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WP1V' and b.failurecode = 'RW-WR1V') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WC3H' and b.failurecode = 'RW-WC3H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WC3H' and b.failurecode = 'RW-WC3Q') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1H' and b.failurecode = 'RW-WR1H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1H' and b.failurecode = 'RW-WR4D') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1H' and b.failurecode = 'RW-WR1A') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP2R' and b.failurecode = 'RWW-SR2E') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WD8C' and b.failurecode = 'RW-WR3M') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1H' and b.failurecode = 'RW-WR1B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1H' and b.failurecode = 'RW-WR3I') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1H' and b.failurecode = 'RW-WR3M') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1H' and b.failurecode = 'RW-WR1V') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SR2E' and b.failurecode = 'RWW-SR2E') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SR2E') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SR2E' and b.failurecode = 'RWW-SR2B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1V' and b.failurecode = 'RW-WR1V') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'D170') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1V' and b.failurecode = 'RW-WR4D') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1V') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1V' and b.failurecode = 'RW-WR1A') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1V' and b.failurecode = 'RW-WR1H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1V' and b.failurecode = 'RW-WR1B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1V' and b.failurecode = 'RW-WR3I') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1V' and b.failurecode = 'RW-WR3M') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1B' and b.failurecode = 'RW-WR1A') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1B' and b.failurecode = 'RW-WR1B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1B' and b.failurecode = 'RW-WR3I') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1B' and b.failurecode = 'RW-WR3M') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1B' and b.failurecode = 'RW-WR1H') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP2W') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1B' and b.failurecode = 'RW-WR1V') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1B' and b.failurecode = 'RW-WC3P') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1B' and b.failurecode = 'RW-WR4D') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1A' and b.failurecode = 'RW-WR1A') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1A' and b.failurecode = 'RW-WR1B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1A' and b.failurecode = 'RW-WR3I') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1A' and b.failurecode = 'RW-WR3M') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1A' and b.failurecode = 'RW-WR1H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1A' and b.failurecode = 'RW-WR1V') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1A' and b.failurecode = 'RW-WC3P') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1A' and b.failurecode = 'RW-WC3H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3I' and b.failurecode = 'RW-WR3I') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3I' and b.failurecode = 'RW-WR3M') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3I' and b.failurecode = 'RW-WR1A') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3I' and b.failurecode = 'RW-WR1B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3I') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3I' and b.failurecode = 'RW-WC3P') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3I' and b.failurecode = 'RW-WC3H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3I' and b.failurecode = 'RW-WR1H') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP2A' and b.failurecode = 'RWW-SR2B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3I' and b.failurecode = 'RW-WR1V') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3M') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3M' and b.failurecode = 'RW-WR3M') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3M' and b.failurecode = 'RW-WR3I') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WP4C' and b.failurecode = 'RW-WR1H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3M' and b.failurecode = 'RW-WR1A') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3M' and b.failurecode = 'RW-WC3H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3M' and b.failurecode = 'RW-WR1B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3M' and b.failurecode = 'RW-WC3P') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR4D' and b.failurecode = 'RW-WR4D') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR4D' and b.failurecode = 'RW-WR1H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR4D' and b.failurecode = 'RW-WR1V') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR4D' and b.failurecode = 'RRW-RR4D') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP2R' and b.failurecode = 'RWW-SR2B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR4D' and b.failurecode = 'RW-WR1A') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP2A' and b.failurecode = 'RWW-SR2E') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WD8C' and b.failurecode = 'RW-WC3P') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SR2W' and b.failurecode = 'RWW-SR2E') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SR2W' and b.failurecode = 'RWW-SR2B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WC3P') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WP4C' and b.failurecode = 'RW-WR1V') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WP1H' and b.failurecode = 'RW-WR1A') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WP1V' and b.failurecode = 'RW-WR4D') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WC3P' and b.failurecode = 'RW-WC3P') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WC3P' and b.failurecode = 'RW-WR3M') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WC3P' and b.failurecode = 'RW-WR1A') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'IVA1' and b.failurecode = 'RWW-SR2B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'IVA1' and b.failurecode = 'RW-WR1H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'IVA1' and b.failurecode = 'RW-WR1V') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'IVA1' and b.failurecode = 'RW-WR4D') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'IVA1' and b.failurecode = 'RW-WC3H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'IVA1' and b.failurecode = 'RW-WR3M') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'IVA1' and b.failurecode = 'RW-WR3I') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'IVA1' and b.failurecode = 'RW-WR1A') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'IVA1' and b.failurecode = 'RWW-SR2E') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP2R' and b.failurecode = 'RWW-SR2W') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'IVA1' and b.failurecode = 'RW-WR1B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'IVA1' and b.failurecode = 'RW-WC3Q') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'IVA1' and b.failurecode = 'RW-WC3P') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WP1V' and b.failurecode = 'RW-WR1H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SR8A' and b.failurecode = 'RWW-SR2B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SR8A' and b.failurecode = 'RWW-SR2E') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WC3H' and b.failurecode = 'RW-WC3H') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'IVA1' and b.failurecode = 'RW-WR1H') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP2L' and b.failurecode = 'RWW-SR2E') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP2K' and b.failurecode = 'RWW-SR2E')
        ) 
        AND WORKORDER.worktype in ('BM', 'CM') 
        AND WORKORDER.workorderClass = 'WORKORDER' 
        AND WORKORDER.STATUS NOT IN ('CAN','CANDUP','DRAFT') 
        AND WORKORDER.serviceType IN ('M','E','F','C') 
        AND WORKORDER.fCId IS NULL 
        and 
        (
        ((WORKORDER.asset <> parentWO.asset or parentWO.asset is null) and (WORKORDER.asset <> oriWO.asset or oriWO.asset is null)) or
        (WORKORDER.parentWo is null and (WORKORDER.asset <> oriWO.asset or oriWo.asset is null)) or 
        (WORKORDER.originatingRecord is null and (WORKORDER.asset <> parentWO.asset or parentWO.asset is null)) or 
        (WORKORDER.parentWo is null and WORKORDER.originatingRecord is null)
        )  
    )
    WORKORDER 
    WHERE
    DT.ASSETNUM = WORKORDER.asset 
    --AND WORKORDER.assetnum in ( '2765052')
    
    GROUP BY 
    WORKORDER.asset,
    DT.assetSK,
    DT.ORDER_ID,
    DT.SELECTED_PERIOD_START_DATE,
    DT.SELECTED_PERIOD_END_DATE,
    DT.COMPARISON_PERIOD_START_DATE,
    DT.COMPARISON_PERIOD_END_DATE
    )
    where yearlyAverageBreakdownMaintenanceCount > 0 
    or selectedPeriodBreakdownMaintenanceCount >0 
    or comparisonPeriodBreakdownMaintenanceCount > 0 
    or selectedPeriodFailedAssetsCount>0 
    or selectedPeriodRepeatedlyFailedAssetsCount>0
    """)

    # display(df)
    # print(df.count())
    Save(df)
    #DisplaySelf()
pass
monthly_Transform()

# COMMAND ----------

def yearly_Transform():
    df = spark.sql(f"""
    SELECT
    ASSETNUM||'|'||REPORTING_YEAR||'|'|| REPORTING_MONTH||'|'||CALCULATION_TYPE||'|'||ORDER_ID {BK}, 
    ASSETNUM as assetNumber, 
    assetSK as assetFK,
    REPORTING_YEAR as reportingYear, 
    REPORTING_MONTH as reportingMonth,
    CALCULATION_TYPE as calculationTypeCode,
    ORDER_ID as orderID, 
    SELECTED_PERIOD_START_DATE as selectedPeriodStartDate,
    SELECTED_PERIOD_END_DATE as selectedPeriodEndDate,
    COMPARISON_PERIOD_START_DATE as comparisonPeriodStartDate,
    COMPARISON_PERIOD_END_DATE as comparisonPeriodEndDate,
    SELECTED_PERIOD_BM as selectedPeriodBreakdownMaintenanceCount,
    COMPARISON_PERIOD_BM as comparisonPeriodBreakdownMaintenanceCount, 
    YEARLY_BM as yearlyAverageBreakdownMaintenanceCount, 
    NO_OF_FAILED_ASSETS_SELECTED_PERIOD as selectedPeriodFailedAssetsCount,
    NO_OF_REPEATEDLY_FAILED_ASSETS_SELECTED_PERIOD selectedPeriodRepeatedlyFailedAssetsCount,
    CLASSTYPE
    FROM(
    
    SELECT 
    WORKORDER.ASSET as ASSETNUM,
    DT.assetSK,
    EXTRACT (YEAR FROM DT.SELECTED_PERIOD_END_DATE) REPORTING_YEAR,
    EXTRACT (MONTH FROM DT.SELECTED_PERIOD_END_DATE) REPORTING_MONTH,
    'Y' CALCULATION_TYPE,
    DT.ORDER_ID,
    DT.SELECTED_PERIOD_START_DATE,
    DT.SELECTED_PERIOD_END_DATE,
    DT.COMPARISON_PERIOD_START_DATE,
    DT.COMPARISON_PERIOD_END_DATE,
    COUNT( CASE WHEN WORKORDER.reportedDateTime BETWEEN SELECTED_PERIOD_START_DATE AND SELECTED_PERIOD_END_DATE THEN 1 END ) SELECTED_PERIOD_BM,
    COUNT( CASE WHEN WORKORDER.reportedDateTime BETWEEN COMPARISON_PERIOD_START_DATE AND COMPARISON_PERIOD_END_DATE THEN 1 END ) COMPARISON_PERIOD_BM,
    COUNT( CASE WHEN WORKORDER.reportedDateTime BETWEEN COMPARISON_PERIOD_START_DATE AND COMPARISON_PERIOD_END_DATE THEN 1 END )/5 YEARLY_BM,
    CASE WHEN COUNT( CASE WHEN WORKORDER.reportedDateTime BETWEEN SELECTED_PERIOD_START_DATE AND SELECTED_PERIOD_END_DATE THEN 1 END )>0 THEN 1 ELSE 0 END NO_OF_FAILED_ASSETS_SELECTED_PERIOD,
    CASE WHEN COUNT( CASE WHEN WORKORDER.reportedDateTime BETWEEN SELECTED_PERIOD_START_DATE AND SELECTED_PERIOD_END_DATE THEN 1 END )>0 AND 
    COUNT( CASE WHEN WORKORDER.reportedDateTime BETWEEN COMPARISON_PERIOD_START_DATE AND COMPARISON_PERIOD_END_DATE THEN 1 END )>0 THEN 1 ELSE 0 END NO_OF_REPEATEDLY_FAILED_ASSETS_SELECTED_PERIOD
    , 'LINEAR' as classType
    
    FROM  
    
    (SELECT 
    ASSETLIST.ASSETNUM,
    ASSETLIST.assetSK,
    YEARS.Y ORDER_ID,
    CASE WHEN EXTRACT (MONTH FROM CURRENT_DATE) IN (1,2,3,4,5,6) THEN TO_DATE((EXTRACT (YEAR FROM CURRENT_DATE)-YEARS.Y-1)||'-07-01') 
        WHEN EXTRACT (MONTH FROM CURRENT_DATE) IN (7,8,9,10,11,12) THEN TO_DATE((EXTRACT (YEAR FROM CURRENT_DATE)-YEARS.Y)||'-07-01') END SELECTED_PERIOD_START_DATE,
    CASE WHEN EXTRACT (MONTH FROM CURRENT_DATE) IN (1,2,3,4,5,6) THEN TO_DATE((EXTRACT (YEAR FROM CURRENT_DATE)-YEARS.Y)||'-06-30') 
        WHEN EXTRACT (MONTH FROM CURRENT_DATE) IN (7,8,9,10,11,12) THEN TO_DATE((EXTRACT (YEAR FROM CURRENT_DATE)-YEARS.Y+1)||'-06-30') END SELECTED_PERIOD_END_DATE,
    CASE WHEN EXTRACT (MONTH FROM CURRENT_DATE) IN (1,2,3,4,5,6) THEN TO_DATE((EXTRACT (YEAR FROM CURRENT_DATE)-YEARS.Y-6)||'-07-01') 
        WHEN EXTRACT (MONTH FROM CURRENT_DATE) IN (7,8,9,10,11,12) THEN TO_DATE((EXTRACT (YEAR FROM CURRENT_DATE)-YEARS.Y-5)||'-07-01') END COMPARISON_PERIOD_START_DATE,
    CASE WHEN EXTRACT (MONTH FROM CURRENT_DATE) IN (1,2,3,4,5,6) THEN TO_DATE((EXTRACT (YEAR FROM CURRENT_DATE)-YEARS.Y-1)||'-06-30') 
        WHEN EXTRACT (MONTH FROM CURRENT_DATE) IN (7,8,9,10,11,12) THEN TO_DATE((EXTRACT (YEAR FROM CURRENT_DATE)-YEARS.Y)||'-06-30') END COMPARISON_PERIOD_END_DATE

    FROM ( 
            SELECT DISTINCT (da.assetNumber) as ASSETNUM, da.assetSK
            FROM 
            ( 
            select *  
            from {get_table_namespace('curated', 'factworkorder')}
            qualify row_number() over(partition by workOrderCreationId order by workOrderChangeDate desc)=1
            ) fwo
            inner join {get_table_namespace('curated', 'dimasset')} da
            ON da.assetSK = fwo.assetFK  
            and da.sourceRecordCurrent = 1

            inner join {get_table_namespace('curated', 'dimassetlocation')} dal
            ON da.assetLocationFK = dal.assetLocationSK
            and dal.sourceRecordCurrent = 1
            and dal.assetLocationTypeCode IN ('SYSAREA','FACILITY','PROCESS','FUNCLOC') 
            and dal.assetLocationStatusDescription  = 'OPERATING'
            and (dal.assetLocationFacilityShortCode NOT IN ('AV','CP','DB','DG','DP','DQ','DR','EG','FM','GE','GG','MV','NC',
            'NT','OO','P0','PR','RF','RK','RM','RN','RP','RQ','RS','RT','RX','SC','SF','SG','SK','SL','SM','SN','SO','SP',
            'SQ','SR','SS','ST','SU','SV','SW','SX','SY','TD','TP','TS','WA','WC','WD','WF','WG','WH','WK','WM','WN','WP','WQ','WS','WT','WU','WX','WZ')
            OR
            dal.assetLocationFacilityCode IS NULL
            --dal.assetLocationFacilityCode IS NULL ***column rename not impemented***
            )
    ) ASSETLIST

    CROSS JOIN 
    ( 
        SELECT 1 AS Y UNION ALL
        SELECT 2 AS Y UNION ALL
        SELECT 3 AS Y UNION ALL
        SELECT 4 AS Y UNION ALL
        SELECT 5 AS Y UNION ALL
        SELECT 6 AS Y UNION ALL
        SELECT 7 AS Y UNION ALL
        SELECT 8 AS Y UNION ALL
        SELECT 9 AS Y UNION ALL
        SELECT 10 AS Y
    ) YEARS 
    
    )DT
    inner join
    (
        select
        WORKORDER.workOrder, WORKORDER.asset, WORKORDER.worktype,
        WORKORDER.taskcode, WORKORDER.parentWo, WORKORDER.originatingRecord,
        WORKORDER.reportedDateTime,b.failurecode, parentWO.asset as parentAsset, oriWO.asset as oriAsset
        from 
        (
        select workOrder, asset, worktype, taskcode, parentWo, originatingRecord, reportedDateTime, Class as workorderClass, fCId, serviceType, STATUS
        from {get_table_namespace('cleansed', 'maximo_workorder')} 
        qualify row_number() over(partition by workOrder order by changeDate desc) =1
        ) WORKORDER
        left join 
        (
        SELECT failurecode, workOrder 
        FROM {get_table_namespace('cleansed', 'MAXIMO_FAILUREREPORT')} WHERE TYPE = 'REMEDY' 
        ) b 
        on b.workOrder=WORKORDER.workOrder 
        left join 
        (
        select asset, workOrder 
        from {get_table_namespace('cleansed', 'maximo_workorder')} 
        qualify row_number() over(partition by workOrder order by changeDate desc) =1
        )parentWO 
        on parentWO.workOrder = WORKORDER.parentWo
        left join 
        (
        select asset,workorder  
        from {get_table_namespace('cleansed', 'maximo_workorder')} 
        qualify row_number() over(partition by workOrder order by changeDate desc) =1
        )oriWO
        on oriWO.workorder = WORKORDER.originatingRecord
        where 
        (
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WD8C') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP2A') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP2R') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP3S') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WD8C' and b.failurecode = 'RW-WC3H') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WP4C' and b.failurecode = 'RW-WR4D') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WP1H' and b.failurecode = 'RW-WR1H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SR2B' and b.failurecode = 'RWW-SR2B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SR2B' and b.failurecode = 'RWW-SR2D') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SR2B' and b.failurecode = 'RWW-XX') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SR2B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SR2B' and b.failurecode = 'RWW-SR2E') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP2C') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SS1Z' and b.failurecode = 'RWW-SR2E') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP2A' and b.failurecode = 'RWW-SR8A') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR9A' and b.failurecode = 'RWW-SR2B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR9A' and b.failurecode = 'RWW-SR2E') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR9A' and b.failurecode = 'RW-WR1H') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP2B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR9A' and b.failurecode = 'RW-WR1V') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR9A' and b.failurecode = 'RW-WR1B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR9A' and b.failurecode = 'RW-WR3I') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SR2H' and b.failurecode = 'RWW-SR2B') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WP1H' and b.failurecode = 'RW-WR4D') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WC3Q' and b.failurecode = 'RW-WC3H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WC3Q' and b.failurecode = 'RW-WC3Q') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WP1V' and b.failurecode = 'RW-WR1V') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WC3H' and b.failurecode = 'RW-WC3H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WC3H' and b.failurecode = 'RW-WC3Q') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1H' and b.failurecode = 'RW-WR1H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1H' and b.failurecode = 'RW-WR4D') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1H' and b.failurecode = 'RW-WR1A') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP2R' and b.failurecode = 'RWW-SR2E') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WD8C' and b.failurecode = 'RW-WR3M') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1H' and b.failurecode = 'RW-WR1B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1H' and b.failurecode = 'RW-WR3I') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1H' and b.failurecode = 'RW-WR3M') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1H' and b.failurecode = 'RW-WR1V') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SR2E' and b.failurecode = 'RWW-SR2E') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SR2E') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SR2E' and b.failurecode = 'RWW-SR2B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1V' and b.failurecode = 'RW-WR1V') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'D170') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1V' and b.failurecode = 'RW-WR4D') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1V') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1V' and b.failurecode = 'RW-WR1A') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1V' and b.failurecode = 'RW-WR1H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1V' and b.failurecode = 'RW-WR1B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1V' and b.failurecode = 'RW-WR3I') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1V' and b.failurecode = 'RW-WR3M') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1B' and b.failurecode = 'RW-WR1A') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1B' and b.failurecode = 'RW-WR1B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1B' and b.failurecode = 'RW-WR3I') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1B' and b.failurecode = 'RW-WR3M') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1B' and b.failurecode = 'RW-WR1H') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP2W') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1B' and b.failurecode = 'RW-WR1V') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1B' and b.failurecode = 'RW-WC3P') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1B' and b.failurecode = 'RW-WR4D') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1A' and b.failurecode = 'RW-WR1A') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1A' and b.failurecode = 'RW-WR1B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1A' and b.failurecode = 'RW-WR3I') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1A' and b.failurecode = 'RW-WR3M') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1A' and b.failurecode = 'RW-WR1H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1A' and b.failurecode = 'RW-WR1V') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1A' and b.failurecode = 'RW-WC3P') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR1A' and b.failurecode = 'RW-WC3H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3I' and b.failurecode = 'RW-WR3I') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3I' and b.failurecode = 'RW-WR3M') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3I' and b.failurecode = 'RW-WR1A') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3I' and b.failurecode = 'RW-WR1B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3I') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3I' and b.failurecode = 'RW-WC3P') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3I' and b.failurecode = 'RW-WC3H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3I' and b.failurecode = 'RW-WR1H') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP2A' and b.failurecode = 'RWW-SR2B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3I' and b.failurecode = 'RW-WR1V') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3M') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3M' and b.failurecode = 'RW-WR3M') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3M' and b.failurecode = 'RW-WR3I') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WP4C' and b.failurecode = 'RW-WR1H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3M' and b.failurecode = 'RW-WR1A') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3M' and b.failurecode = 'RW-WC3H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3M' and b.failurecode = 'RW-WR1B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR3M' and b.failurecode = 'RW-WC3P') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR4D' and b.failurecode = 'RW-WR4D') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR4D' and b.failurecode = 'RW-WR1H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR4D' and b.failurecode = 'RW-WR1V') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR4D' and b.failurecode = 'RRW-RR4D') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP2R' and b.failurecode = 'RWW-SR2B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WR4D' and b.failurecode = 'RW-WR1A') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP2A' and b.failurecode = 'RWW-SR2E') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WD8C' and b.failurecode = 'RW-WC3P') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SR2W' and b.failurecode = 'RWW-SR2E') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SR2W' and b.failurecode = 'RWW-SR2B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WC3P') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WP4C' and b.failurecode = 'RW-WR1V') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WP1H' and b.failurecode = 'RW-WR1A') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WP1V' and b.failurecode = 'RW-WR4D') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WC3P' and b.failurecode = 'RW-WC3P') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WC3P' and b.failurecode = 'RW-WR3M') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'WC3P' and b.failurecode = 'RW-WR1A') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'IVA1' and b.failurecode = 'RWW-SR2B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'IVA1' and b.failurecode = 'RW-WR1H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'IVA1' and b.failurecode = 'RW-WR1V') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'IVA1' and b.failurecode = 'RW-WR4D') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'IVA1' and b.failurecode = 'RW-WC3H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'IVA1' and b.failurecode = 'RW-WR3M') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'IVA1' and b.failurecode = 'RW-WR3I') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'IVA1' and b.failurecode = 'RW-WR1A') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'IVA1' and b.failurecode = 'RWW-SR2E') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP2R' and b.failurecode = 'RWW-SR2W') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'IVA1' and b.failurecode = 'RW-WR1B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'IVA1' and b.failurecode = 'RW-WC3Q') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'IVA1' and b.failurecode = 'RW-WC3P') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WP1V' and b.failurecode = 'RW-WR1H') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SR8A' and b.failurecode = 'RWW-SR2B') or 
        (WORKORDER.worktype='BM' and WORKORDER.taskCode = 'SR8A' and b.failurecode = 'RWW-SR2E') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'WC3H' and b.failurecode = 'RW-WC3H') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'IVA1' and b.failurecode = 'RW-WR1H') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP2L' and b.failurecode = 'RWW-SR2E') or 
        (WORKORDER.worktype='CM' and WORKORDER.taskCode = 'SP2K' and b.failurecode = 'RWW-SR2E')
        ) 
        AND WORKORDER.worktype in ('BM', 'CM') 
        AND WORKORDER.workorderClass = 'WORKORDER' 
        AND WORKORDER.STATUS NOT IN ('CAN','CANDUP','DRAFT') 
        AND WORKORDER.serviceType IN ('M','E','F','C') 
        AND WORKORDER.fCId IS NULL 
        and 
        (
        ((WORKORDER.asset <> parentWO.asset or parentWO.asset is null) and (WORKORDER.asset <> oriWO.asset or oriWO.asset is null)) or
        (WORKORDER.parentWo is null and (WORKORDER.asset <> oriWO.asset or oriWo.asset is null)) or 
        (WORKORDER.originatingRecord is null and (WORKORDER.asset <> parentWO.asset or parentWO.asset is null)) or 
        (WORKORDER.parentWo is null and WORKORDER.originatingRecord is null)
        )  
    )
    WORKORDER 
    WHERE
    DT.ASSETNUM = WORKORDER.asset 
    --AND WORKORDER.assetnum in ( '2765052')
    
    GROUP BY 
    WORKORDER.asset,
    DT.assetSK,
    DT.ORDER_ID,
    DT.SELECTED_PERIOD_START_DATE,
    DT.SELECTED_PERIOD_END_DATE,
    DT.COMPARISON_PERIOD_START_DATE,
    DT.COMPARISON_PERIOD_END_DATE
    
    union
    
    SELECT
    WORKORDER.ASSETNUM,
    DT.assetSK,
    EXTRACT (YEAR FROM DT.SELECTED_PERIOD_END_DATE) REPORTING_YEAR,
    EXTRACT (MONTH FROM DT.SELECTED_PERIOD_END_DATE) REPORTING_MONTH,
    'Y' CALCULATION_TYPE,
    DT.ORDER_ID,
    DT.SELECTED_PERIOD_START_DATE,
    DT.SELECTED_PERIOD_END_DATE,
    DT.COMPARISON_PERIOD_START_DATE,
    DT.COMPARISON_PERIOD_END_DATE,
    COUNT( CASE WHEN WORKORDER.REPORTDATE BETWEEN SELECTED_PERIOD_START_DATE AND SELECTED_PERIOD_END_DATE THEN 1 END ) SELECTED_PERIOD_BM,
    COUNT( CASE WHEN WORKORDER.REPORTDATE BETWEEN COMPARISON_PERIOD_START_DATE AND COMPARISON_PERIOD_END_DATE THEN 1 END ) COMPARISON_PERIOD_BM,
    COUNT( CASE WHEN WORKORDER.REPORTDATE BETWEEN COMPARISON_PERIOD_START_DATE AND COMPARISON_PERIOD_END_DATE THEN 1 END )/5 YEARLY_BM,
    CASE WHEN COUNT( CASE WHEN WORKORDER.REPORTDATE BETWEEN SELECTED_PERIOD_START_DATE AND SELECTED_PERIOD_END_DATE THEN 1 END )>0 THEN 1 ELSE 0 END NO_OF_FAILED_ASSETS_SELECTED_PERIOD,
    CASE WHEN COUNT( CASE WHEN WORKORDER.REPORTDATE BETWEEN SELECTED_PERIOD_START_DATE AND SELECTED_PERIOD_END_DATE THEN 1 END )>0 AND COUNT( CASE WHEN WORKORDER.REPORTDATE BETWEEN COMPARISON_PERIOD_START_DATE AND COMPARISON_PERIOD_END_DATE THEN 1 END )>0 THEN 1 ELSE 0 END NO_OF_REPEATEDLY_FAILED_ASSETS_SELECTED_PERIOD
    , 'NONLINEAR' as classType
    FROM  
    
    (
    SELECT
    ASSETLIST.ASSETNUM,
    ASSETLIST.assetSK,
    YEARS.Y ORDER_ID,
    CASE WHEN EXTRACT (MONTH FROM CURRENT_DATE) IN (1,2,3,4,5,6) THEN TO_DATE((EXTRACT (YEAR FROM CURRENT_DATE)-YEARS.Y-1)||'-07-01') 
        WHEN EXTRACT (MONTH FROM CURRENT_DATE) IN (7,8,9,10,11,12) THEN TO_DATE((EXTRACT (YEAR FROM CURRENT_DATE)-YEARS.Y)||'-07-01') END SELECTED_PERIOD_START_DATE,
    CASE WHEN EXTRACT (MONTH FROM CURRENT_DATE) IN (1,2,3,4,5,6) THEN TO_DATE((EXTRACT (YEAR FROM CURRENT_DATE)-YEARS.Y)||'-06-30') 
        WHEN EXTRACT (MONTH FROM CURRENT_DATE) IN (7,8,9,10,11,12) THEN TO_DATE((EXTRACT (YEAR FROM CURRENT_DATE)-YEARS.Y+1)||'-06-30') END SELECTED_PERIOD_END_DATE,
    CASE WHEN EXTRACT (MONTH FROM CURRENT_DATE) IN (1,2,3,4,5,6) THEN TO_DATE((EXTRACT (YEAR FROM CURRENT_DATE)-YEARS.Y-6)||'-07-01') 
        WHEN EXTRACT (MONTH FROM CURRENT_DATE) IN (7,8,9,10,11,12) THEN TO_DATE((EXTRACT (YEAR FROM CURRENT_DATE)-YEARS.Y-5)||'-07-01') END COMPARISON_PERIOD_START_DATE,
    CASE WHEN EXTRACT (MONTH FROM CURRENT_DATE) IN (1,2,3,4,5,6) THEN TO_DATE((EXTRACT (YEAR FROM CURRENT_DATE)-YEARS.Y-1)||'-06-30') 
        WHEN EXTRACT (MONTH FROM CURRENT_DATE) IN (7,8,9,10,11,12) THEN TO_DATE((EXTRACT (YEAR FROM CURRENT_DATE)-YEARS.Y)||'-06-30') END COMPARISON_PERIOD_END_DATE

    FROM 
    ( 
            SELECT DISTINCT (da.assetNumber) as ASSETNUM, da.assetSK
            FROM 
            ( 
            select *  
            from {get_table_namespace('curated', 'factworkorder')}
            qualify row_number() over(partition by workOrderCreationId order by workOrderChangeDate desc)=1
            ) fwo
            inner join {get_table_namespace('curated', 'dimasset')} da
            ON da.assetSK = fwo.assetFK  
            and da.sourceRecordCurrent = 1

            inner join {get_table_namespace('curated', 'dimassetlocation')} dal
            ON da.assetLocationFK = dal.assetLocationSK
            and dal.sourceRecordCurrent = 1
            and dal.assetLocationTypeCode IN ('SYSAREA','FACILITY','PROCESS','FUNCLOC') 
            and dal.assetLocationStatusDescription  = 'OPERATING'
            and (dal.assetLocationFacilityShortCode  in ('AV','CP','DB','DG','DP','DQ','DR','EG','FM','GE','GG','MV','NC',
            'NT','OO','P0','PR','RF','RK','RM','RN','RP','RQ','RS','RT','RX','SC','SF','SG','SK','SL','SM','SN','SO','SP',
            'SQ','SR','SS','ST','SU','SV','SW','SX','SY','TD','TP','TS','WA','WC','WD','WF','WG','WH','WK','WM','WN','WP','WQ','WS','WT','WU','WX','WZ')
            --dal.assetLocationFacilityCode IS NULL ***column rename not impemented***
            )
    ) ASSETLIST CROSS JOIN
    ( 
    SELECT  1 AS Y UNION ALL
    SELECT  2 AS Y UNION ALL
    SELECT  3 AS Y UNION ALL
    SELECT  4 AS Y UNION ALL
    SELECT  5 AS Y 
    ) YEARS
    )DT
    inner join 
        (
        select asset as ASSETNUM,
        workorder , 
        WORKTYPE, 
        Class as WOCLASS,
        --workOrderClass as WOCLASS, 
        status,
        serviceType as SWCSERVTYPE, 
        fcid as FINCNTRLID, 
        reportedDateTime as REPORTDATE
        from {get_table_namespace('cleansed', 'maximo_workorder')} 
        qualify row_number() over(partition by workOrder order by changeDate desc) =1
        ) WORKORDER
    ON DT.ASSETNUM = WORKORDER.ASSETNUM
    WHERE
    WORKORDER.WORKTYPE = 'BM' AND
    WORKORDER.WOCLASS = 'WORKORDER' AND
    WORKORDER.STATUS NOT IN ('CAN','CANDUP','DRAFT') AND
    WORKORDER.SWCSERVTYPE IN ('M','E','F','C') AND
    WORKORDER.FINCNTRLID IS NULL
    
    GROUP BY
    WORKORDER.ASSETNUM,
    DT.assetSK,
    DT.ORDER_ID,
    DT.SELECTED_PERIOD_START_DATE,
    DT.SELECTED_PERIOD_END_DATE,
    DT.COMPARISON_PERIOD_START_DATE,
    DT.COMPARISON_PERIOD_END_DATE
    
    )
    where YEARLY_BM > 0 or SELECTED_PERIOD_BM >0 or COMPARISON_PERIOD_BM > 0 or NO_OF_FAILED_ASSETS_SELECTED_PERIOD>0 or NO_OF_REPEATEDLY_FAILED_ASSETS_SELECTED_PERIOD>0
    """)
     # display(df)
    # print(df.count())
    Save(df)
    #DisplaySelf()
pass
yearly_Transform()

# COMMAND ----------

spark.sql(f"""
          create or replace view {get_table_namespace('curated', 'factassetperformanceindex')} as (select * from {get_table_namespace('curated', 'factassetperformanceindex')})
          """)

# COMMAND ----------


