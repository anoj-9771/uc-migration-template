# Databricks notebook source
# MAGIC %run ../../Common/common-helpers

# COMMAND ----------

# MAGIC %run ../../Common/common-transform

# COMMAND ----------

# DBTITLE 1,Project Status & Commentary
spark.sql(f"""
CREATE OR REPLACE VIEW {get_env()}curated.asset_performance.eppmProjectStatusCommentary AS 
(
  WITH projectCommentary AS (
      SELECT projectObject, projectStatus, projectStatusIcon, commentary, calendarYear, calendarMonth  FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY projectObject,projectStatus ORDER BY calendarYear DESC,calendarMonth DESC) as rowNumber 
        FROM {get_env()}cleansed.ppm.zps_psf_commlog
        ) WHERE rowNumber = 1
  ),
  fiscalDate AS (
      SELECT DISTINCT  calendarYear, monthOfYear, financialYear, monthOfFinancialYear FROM {get_env()}curated.dim.`date`
  )
  SELECT 
    comm.projectObject as project
    ,comm.projectStatus
    ,comm.projectStatusIcon
    ,comm.commentary
    ,fisc.financialYear
    ,fisc.monthOfFinancialYear
    ,item.itemDetailId
    ,item.itemDetailGuid
    ,init.initiativeId
    ,item.initiativeDetailGuid
    ,buck.bucketId
    ,item.bucketGuid
    ,port.portfolioId
    ,item.portfolioGuid 
    
  FROM projectCommentary comm
    LEFT JOIN fiscalDate fisc ON fisc.calendarYear = comm.calendarYear AND fisc.monthOfYear = comm.calendarMonth 
    LEFT JOIN {get_env()}cleansed.ppm.rpm_item_d item ON item.itemDetailId = comm.projectObject
    LEFT JOIN {get_env()}cleansed.ppm.0rpm_bucket_guid_attr buck ON buck.bucketGuid = item.bucketGuid
    LEFT JOIN {get_env()}cleansed.ppm.0inm_initiative_guid_attr init ON init.initiativeGuid = item.initiativeDetailGuid
    LEFT JOIN {get_env()}cleansed.ppm.0rpm_port_guid_attr port on port.portGuid = item.portfolioGuid
)
""")

# COMMAND ----------

# DBTITLE 1,Master Data
spark.sql(f"""
CREATE OR REPLACE VIEW {get_env()}curated.asset_performance.eppmSelfServiceMasterCurrent AS
(
    SELECT 
    item.itemDetailGuid as itemDetailGUID
    ,item.itemDetailId
    ,itemTx.shortText as itemDetailIdDescription
    ,item.bucketGuid as bucketGUID
    ,buckTx.bucketIdDescription
    ,buck.bucketID as bucketId
    ,pbuck.bucketID as parentBucketId
    ,item.initiativeDetailGuid as initiativeDetailGUID
    ,init.initiativeID as initiativeId
    ,initTx.shortText as initiativeDescription
    ,item.portfolioGuid as portfolioGUID
    ,port.portfolioId
    ,portTx.portfolioGuidDescription as portfolioIdDescription
    ,item.projectApprovalStatusId
    ,statTx.projectApprovalStatusDescription
    ,item.requestingCostCenterNumber
    ,reqcTx.costCenterShortDescription as requestingCostCenterShortDescription
    ,reqcTx.costCenterMeduimDescription as requestingCostCenterMediumDescription
    ,reqcTx.costCenterLongDescription as requestingCostCenterLongDescription
    ,item.responsibleCostCenterNumber
    ,rescTx.costCenterShortDescription as responsibleCostCenterShortDescription
    ,rescTx.costCenterMeduimDescription as responsibleCostCenterMediumDescription
    ,rescTx.costCenterLongDescription as responsibleCostCenterLongDescription
    ,item.itemAactualStartDate as itemActualStartDate
    ,item.itemActualFinishDate as itemActualFinishDate
    ,item.itemForecastStartDate
    ,item.itemForecastFinishDate as itemForecastFinishDate
    ,item.itemPlannedStartDate
    ,item.itemPlannedFinishDate
    ,item.itemTypeId
    ,typeTx.itemTypeText as itemTypeDescription
    ,item.overallStatusId
    ,CASE 
        WHEN item.overallStatusId = '@5B@' THEN 'GREEN'
        WHEN item.overallStatusId = '@5C@' THEN 'RED'
        WHEN item.overallStatusId = '@5D@' THEN 'YELLOW'
    END as itemOverallStatus
    ,item.ipartSIRId
    ,item.itemPrimeId     

    ,proj.projectIdDefinition
    ,projTx.projectDescriptionMedium as projectDescription
    ,proj.projectId 
    ,proj.projectProfile
    ,profTx.profileText as projectProfileDescription
    ,proj.objectNumberId as projectObjectNumberId
    ,proj.municipality
    ,proj.electorate
    ,item.projectGeographyCode
    ,CASE 
        WHEN item.projectGeographyCode = 'N' THEN 'North'
        WHEN item.projectGeographyCode = 'S' THEN 'South'
        WHEN item.projectGeographyCode = 'E' THEN 'East'
        WHEN item.projectGeographyCode = 'W' THEN 'West'
        ELSE item.projectGeographyCode 
    END as projectGeographyDescription
    ,proj.cipAssetType
    ,cipTx.cipAssetTypeDescription

    ,wbs.wbsElement
    ,wbs.wbsElementExternal
    ,wbsTx.mediumDescription as wbsElementDescription
    ,wbs.wbsElementControllingArea
    ,wbs.objectNumber as wbsElementObjectNumber
    ,wbs.deliveryPartner
    ,dpTx.deliveryPartner as deliveryPartnerDescription
    ,wbs.subDeliveryPartner
    ,subTx.subDeliveryPartnerDescription
    ,wbs.projectType as wbsElementProjectType
    ,wbs.majorProjectAssociationList as majorProjectAssociationId
    ,mpaTx.majorProjectAssociationDescription
    ,wbs.projectHierarchyLevel
    ,wbs.systemStatus1 as systemStatusCode

    ,decp.decisionGuid as decisionGUID
    ,decpTx.decisionGuidDescription decisionGUIDDescription
    ,decp.decisionId
    ,decidTx.decisionPointText as decisionIdDescription
    ,decp.activeFlag as decisionActiveFlag
    ,decp.statusCode as decisionStatusCode
    ,decp.forecastedDecisionDate
    ,decp.plannedDecisiondate

    -- ,coalesce (pfz1.objectNumber,pfz2.objectNumber) as partnerFunctionObjectNumber
    -- ,pfz1.partnerId as partnerFunctionPartnerIdZ1
    -- ,pfz2.partnerId as partnerFunctionPartnerIdZ2
    -- ,pfz1.partnerFunctionId as z1partnerFunctionI
    -- ,pfz2.partnerFunctionId as z2partnerFunctionId

    ,empZ1.employeesName as projectManager
    ,empZ2.employeesName as programManager
    ,empZ4.employeesName as procurementResponsibleOwner 
    ,empZ6.employeesName as programDirector

    -- ,relBuck.bucketID as chBucketId
    -- ,relBuck.parentBucketGuid as chParentBucketGUID
    -- ,relBuck1.bucketID as chParentBucketId
    -- ,relPort.portfolioId as chPortfolioId

    ,cctr.costCenterNumber
    ,cctrTx.costCenterShortDescription
    ,cctrTx.costCenterMeduimDescription as costCenterMediumDescription
    ,cctrTx.costCenterLongDescription

    ,proj.profitCenterNumber
    ,pcrtTx.shortDescription as profitCenterShortDescription
    ,pcrtTX.mediumDescription as profitCenterMediumDescription

    FROM {get_env()}cleansed.ppm.rpm_item_d item
    LEFT JOIN {get_env()}cleansed.ppm.cgpl_text itemTx ON itemTx.projectPlanningGUID = item.itemDetailGuid
    LEFT JOIN {get_env()}cleansed.ppm.rpm_status_t statTx ON statTx.projectApprovalStatusId = item.projectApprovalStatusId
    LEFT JOIN {get_env()}cleansed.ppm.0rpm_item_type_text typeTx ON typeTx.itemType = item.itemTypeId
    LEFT JOIN {get_env()}cleansed.fin.0costcenter_text reqcTx ON reqcTx.costCenterNumber = item.requestingCostCenterNumber AND reqcTx.controllingArea = '1000' AND reqcTx.validToDate = '9999-12-31'
    LEFT JOIN {get_env()}cleansed.fin.0costcenter_text rescTx ON rescTx.costCenterNumber = item.responsibleCostCenterNumber AND rescTx.controllingArea = '1000' AND rescTx.validToDate ='9999-12-31'
    LEFT JOIN {get_env()}cleansed.ppm.0rpm_bucket_guid_id_text buckTx ON buckTx.bucketGuid = item.bucketGuid

    LEFT JOIN {get_env()}cleansed.ppm.0rpm_bucket_guid_attr buck ON buck.bucketGuid = item.bucketGuid
    LEFT JOIN {get_env()}cleansed.ppm.0rpm_bucket_guid_attr pbuck ON pbuck.bucketGuid = buck.parentBucketGuid

    LEFT JOIN {get_env()}cleansed.ppm.0inm_initiative_guid_attr init ON init.initiativeGuid = item.initiativeDetailGuid
    LEFT JOIN {get_env()}cleansed.ppm.0inm_initiative_guid_text initTx ON initTx.initiativeGuid = init.initiativeGuid

    LEFT JOIN {get_env()}cleansed.ppm.0rpm_port_guid_attr port on port.portGuid = item.portfolioGuid
    LEFT JOIN {get_env()}cleansed.ppm.0rpm_port_guid_id_text portTx on portTx.portGuid = port.portGuid

    LEFT JOIN {get_env()}cleansed.ppm.0rpm_decision_guid_attr decp ON decp.itemGuid = item.itemDetailGuid AND decp.activeFlag = 'Y'
    LEFT JOIN {get_env()}cleansed.ppm.0rpm_decision_guid_id_text decpTx ON decpTx.decisionGuid = decp.decisionGuid
    LEFT JOIN {get_env()}cleansed.ppm.0rpm_decision_id_text decidTx ON decidTx.decisionPointId = decp.decisionId AND decidTx.itemType == decp.itemType

    LEFT JOIN {get_env()}cleansed.ppm.0project_attr proj ON proj.projectId = item.itemDetailId
    LEFT JOIN {get_env()}cleansed.ppm.0project_text projTx ON projTx.projectIdDefinition = proj.projectIdDefinition
    LEFT JOIN {get_env()}cleansed.ppm.zept_cip_typ cipTx ON cipTX.cipAssetType = proj.cipAssetType

    LEFT JOIN {get_env()}cleansed.ppm.tcj4t profTx ON profTx.projectProfile = proj.projectProfile    

    LEFT JOIN {get_env()}cleansed.ppm.ihpa pfz1 on pfz1.objectNumber = proj.objectNumberId and pfz1.partnerFunctionId in ('Z1') and ( pfz1.deleteDataRecord != 'X' OR pfz1.deleteDataRecord is null)
    LEFT JOIN {get_env()}cleansed.hrm.0employee_attr empZ1 on empZ1.personnelNumber = pfz1.partnerId and empZ1.endDate = '9999-12-31'
    LEFT JOIN {get_env()}cleansed.ppm.ihpa pfz2 on pfz2.objectNumber = proj.objectNumberId and pfz2.partnerFunctionId in ('Z2') and (pfz2.deleteDataRecord != 'X' OR pfz2.deleteDataRecord is null)
    LEFT JOIN {get_env()}cleansed.hrm.0employee_attr empZ2 on empZ2.personnelNumber = pfz2.partnerId and empZ2.endDate = '9999-12-31'
    LEFT JOIN {get_env()}cleansed.ppm.ihpa pfz4 on pfz4.objectNumber = proj.objectNumberId and pfz4.partnerFunctionId in ('Z4') and (pfz4.deleteDataRecord != 'X' OR pfz4.deleteDataRecord is null)
    LEFT JOIN {get_env()}cleansed.hrm.0employee_attr empZ4 on empZ4.personnelNumber = pfz4.partnerId and empZ4.endDate = '9999-12-31'    
    LEFT JOIN {get_env()}cleansed.ppm.ihpa pfz6 on pfz6.objectNumber = proj.objectNumberId and pfz6.partnerFunctionId in ('Z6') and (pfz6.deleteDataRecord != 'X' OR pfz6.deleteDataRecord is null)
    LEFT JOIN {get_env()}cleansed.hrm.0employee_attr empZ6 on empZ6.personnelNumber = pfz6.partnerId and empZ6.endDate = '9999-12-31'

    LEFT JOIN {get_env()}cleansed.fin.0profit_ctr_text pcrtTx on proj.profitCenterNumber = pcrtTx.profitCenter and pcrtTx.validToDate = '9999-12-31'

    -- LEFT JOIN {get_env()}cleansed.ppm.rpm_relation_d rel on rel.guid2 = item.itemDetailGuid and rel.relationType = 'RHI'
    -- LEFT JOIN {get_env()}cleansed.ppm.0rpm_bucket_guid_attr relBuck on relBuck.bucketGuid = rel.guid1
    -- LEFT JOIN {get_env()}cleansed.ppm.0rpm_port_guid_attr relPort on relPort.portGuid = relBuck.portfolioGuid
    -- LEFT JOIN {get_env()}cleansed.ppm.0rpm_bucket_guid_attr relBuck1 on relBuck1.bucketGuid = relBuck.parentBucketGuid

    LEFT JOIN {get_env()}cleansed.ppm.0wbs_elemt_attr wbs ON wbs.projectDefinition = proj.projectIdDefinition
    LEFT JOIN {get_env()}cleansed.ppm.0wbs_elemt_text wbsTx ON wbs.wbsElement = wbsTx.wbsElement
    LEFT JOIN {get_env()}cleansed.ppm.zept_sub_delpart subTx ON subTx.subDeliveryPartner = wbs.subDeliveryPartner and subTx.projectType = 'CP'
    LEFT JOIN {get_env()}cleansed.ppm.zept_mpalist mpaTx ON mpaTx.majorProjectAssociationId = wbs.majorProjectAssociationList
    LEFT JOIN {get_env()}cleansed.ppm.zept_del_partner dpTx ON dpTx.deliveryPartnerKey = wbs.deliveryPartner

    LEFT JOIN {get_env()}cleansed.fin.0costcenter_attr cctr ON cctr.costCenterNumber = wbs.requestingCostCenter AND cctr.costCenterCategory in ('P','U') AND cctr.controllingArea = '1000' AND cctr.validToDate = '9999-12-31'
    LEFT JOIN {get_env()}cleansed.fin.0costcenter_text cctrTx ON cctrTx.costCenterNumber = cctr.costCenterNumber AND cctrTx.controllingArea = cctr.controllingArea AND cctrTx.validToDate = cctr.validToDate

)
""")

# COMMAND ----------

# DBTITLE 1,Master Data - History
spark.sql(f"""
CREATE OR REPLACE VIEW {get_env()}curated.asset_performance.eppmSelfServiceMasterHistory AS
SELECT * FROM
(
    SELECT
    '' as itemDetailGUID
    ,itemHist.projectId as itemDetailId
    ,itemTx.projectDescription as itemDetailIdDescription
    ,buck.bucketGuid as bucketGUID
    ,buckTx.bucketIdDescription
    ,itemHist.bucketId
    ,pbuck.bucketID as parentBucketId
    ,init.initiativeGuid as initiativeDetailGUID
    ,itemHist.programId as initiativeId
    ,initTx.shortText as initiativeDescription
    ,port.portfolioGuid as portfolioGUID
    ,itemHist.portfolioId
    ,portTx.portfolioGuidDescription as portfolioIdDescription
    ,itemHist.projectStatus as projectApprovalStatusId
    ,statTx.projectApprovalStatusDescription
    ,'' as requestingCostCenterNumber
    ,'' as requestingCostCenterShortDescription
    ,'' as requestingCostCenterMediumDescription
    ,'' as requestingCostCenterLongDescription
    ,itemHist.responsibleCostCentre as responsibleCostCenterNumber
    ,rescTx.costCenterShortDescription as responsibleCostCenterShortDescription
    ,rescTx.costCenterMeduimDescription as responsibleCostCenterMediumDescription
    ,rescTx.costCenterLongDescription as responsibleCostCenterLongDescription
    ,'' as itemActualStartDate
    ,'' as itemActualFinishDate
    ,'' as itemForecastStartDate
    ,'' as itemForecastFinishDate

    ,itemHist.startDate as itemPlannedStartDate
    ,itemHist.endDate as itemPlannedFinishDate
    ,'' as itemTypeId
    ,'' as itemTypeDescription
    ,'' as overallStatusId
    ,'' as itemOverallStatus
    ,'' as ipartSIRId
    ,'' as itemPrimeId 

    ,proj.projectIdDefinition
    ,projTx.projectDescriptionMedium as projectDescription
    ,itemHist.projectId 
    ,itemHist.projectProfile
    ,profTx.profileText as projectProfileDescription
    ,proj.objectNumberId as projectObjectNumberId
    ,proj.municipality
    ,proj.electorate
    ,'' as projectGeographyCode
    ,'' as projectGeographyDescription
    ,proj.cipAssetType
    ,cipTx.cipAssetTypeDescription

    ,itemHist.wbsId as wbsElement
    ,wbs.wbsElementExternal
    ,wbsTx.mediumDescription as wbsElementDescription
    ,wbs.wbsElementControllingArea
    ,wbs.objectNumber as wbsElementObjectNumber
    ,itemHist.deliveryPartner
    ,dpTx.deliveryPartner as deliveryPartnerDescription
    ,wbs.subDeliveryPartner
    ,subTx.subDeliveryPartnerDescription
    ,wbs.projectType as wbsElementProjectType
    ,wbs.majorProjectAssociationList as majorProjectAssociationId
    ,mpaTx.majorProjectAssociationDescription
    ,wbs.projectHierarchyLevel
    ,'' as systemStatusCode

    ,'' decisionGUID
    ,'' as decisionGUIDDescription
    ,'' as decisionId
    ,'' as decisionIdDescription
    ,'' as decisionActiveFlag
    ,'' as decisionStatusCode
    ,'' as forecastedDecisionDate
    ,'' as plannedDecisiondate

    ,itemHist.projectManager
    ,itemHist.programManager
    ,'' as procurementResponsibleOwner 
    ,'' as programDirector

    FROM {get_env()}cleansed.ppm.ds_historical_data itemHist
    LEFT ANTI JOIN {get_env()}cleansed.ppm.rpm_item_d itemAnti ON itemAnti.itemDetailId = itemHist.projectId
    LEFT JOIN {get_env()}cleansed.ppm.ds_project_text itemTx ON itemTx.projectId = itemHist.projectId
    LEFT JOIN {get_env()}cleansed.ppm.rpm_status_t statTx ON statTx.projectApprovalStatusId = itemHist.projectStatus
    LEFT JOIN {get_env()}cleansed.fin.0costcenter_text rescTx ON rescTx.costCenterNumber = itemHist.responsibleCostCentre AND rescTx.controllingArea = '1000' AND rescTx.validToDate ='9999-12-31'
    LEFT JOIN {get_env()}cleansed.ppm.zept_del_partner dpTx ON dpTx.deliveryPartnerKey = itemHist.deliveryPartner

    LEFT JOIN {get_env()}cleansed.ppm.0rpm_bucket_guid_attr buck ON buck.bucketId = itemHist.bucketId
    LEFT JOIN {get_env()}cleansed.ppm.0rpm_bucket_guid_id_text buckTx ON buckTx.bucketGuid = buck.bucketGuid
    LEFT JOIN {get_env()}cleansed.ppm.0rpm_bucket_guid_attr pbuck ON pbuck.bucketGuid = buck.parentBucketGuid

    LEFT JOIN {get_env()}cleansed.ppm.0inm_initiative_guid_attr init ON init.initiativeId = itemHist.programId
    LEFT JOIN {get_env()}cleansed.ppm.0inm_initiative_guid_text initTx ON initTx.initiativeGuid = init.initiativeGuid

    LEFT JOIN {get_env()}cleansed.ppm.0rpm_port_guid_attr port on port.portfolioId = itemHist.portfolioId
    LEFT JOIN {get_env()}cleansed.ppm.0rpm_port_guid_id_text portTx on portTx.portGuid = port.portGuid

    LEFT JOIN {get_env()}cleansed.ppm.0project_attr proj ON proj.projectId = itemHist.projectId
    LEFT JOIN {get_env()}cleansed.ppm.0project_text projTx ON projTx.projectIdDefinition = proj.projectIdDefinition
    LEFT JOIN {get_env()}cleansed.ppm.zept_cip_typ cipTx ON cipTX.cipAssetType = proj.cipAssetType

    LEFT JOIN {get_env()}cleansed.ppm.tcj4t profTx ON profTx.projectProfile = itemHist.projectProfile    

    LEFT JOIN {get_env()}cleansed.ppm.0wbs_elemt_attr wbs ON wbs.wbsElement = itemHist.wbsId
    LEFT JOIN {get_env()}cleansed.ppm.0wbs_elemt_text wbsTx ON wbs.wbsElement = wbsTx.wbsElement
    LEFT JOIN {get_env()}cleansed.ppm.zept_sub_delpart subTx ON subTx.subDeliveryPartner = wbs.subDeliveryPartner and subTx.projectType = 'CP'
    LEFT JOIN {get_env()}cleansed.ppm.zept_mpalist mpaTx ON mpaTx.majorProjectAssociationId = wbs.majorProjectAssociationList
)
UNION

    SELECT
    item.itemDetailGUID
    ,item.itemDetailId
    ,item.itemDetailIdDescription
    ,item.bucketGUID
    ,item.bucketIdDescription
    ,item.bucketId
    ,item.parentBucketId
    ,item.initiativeDetailGUID
    ,item.initiativeId
    ,item.initiativeDescription
    ,item.portfolioGUID
    ,item.portfolioId
    ,item.portfolioIdDescription
    ,item.projectApprovalStatusId
    ,item.projectApprovalStatusDescription
    ,item.requestingCostCenterNumber
    ,item.requestingCostCenterShortDescription
    ,item.requestingCostCenterMediumDescription
    ,item.requestingCostCenterLongDescription
    ,item.responsibleCostCenterNumber
    ,item.responsibleCostCenterShortDescription
    ,item.responsibleCostCenterMediumDescription
    ,item.responsibleCostCenterLongDescription
    ,item.itemActualStartDate
    ,item.itemActualFinishDate
    ,item.itemForecastStartDate
    ,item.itemForecastFinishDate

    ,item.itemPlannedStartDate
    ,item.itemPlannedFinishDate
    ,item.itemTypeId
    ,item.itemTypeDescription
    ,item.overallStatusId
    ,item.itemOverallStatus
    ,item.ipartSIRId
    ,item.itemPrimeId
    ,item.projectIdDefinition
    ,item.projectDescription
    ,item.projectId 
    ,item.projectProfile
    ,item.projectProfileDescription
    ,item.projectObjectNumberId
    ,item.municipality
    ,item.electorate
    ,item.projectGeographyCode
    ,item.projectGeographyDescription
    ,item.cipAssetType
    ,item.cipAssetTypeDescription

    ,item.wbsElement
    ,item.wbsElementExternal
    ,item.wbsElementDescription
    ,item.wbsElementControllingArea
    ,item.wbsElementObjectNumber
    ,item.deliveryPartner
    ,item.deliveryPartnerDescription
    ,item.subDeliveryPartner
    ,item.subDeliveryPartnerDescription
    ,item.wbsElementProjectType
    ,item.majorProjectAssociationId
    ,item.majorProjectAssociationDescription
    ,item.projectHierarchyLevel
    ,item.systemStatusCode

    ,item.decisionGUID
    ,item.decisionGUIDDescription
    ,item.decisionId
    ,item.decisionIdDescription
    ,item.decisionActiveFlag
    ,item.decisionStatusCode
    ,item.forecastedDecisionDate
    ,item.plannedDecisiondate

    ,item.projectManager
    ,item.programManager
    ,item.procurementResponsibleOwner
    ,item.programDirector

    FROM {get_env()}curated.asset_performance.eppmSelfServiceMasterCurrent item 
    WHERE wbsElement IN (SELECT DISTINCT wbsElement FROM {get_env()}cleansed.ppm.ds_gl_historical_data UNION SELECT DISTINCT wbsElement FROM {get_env()}cleansed.ppm.ds_bpc_historical_data)

""")

# COMMAND ----------

# DBTITLE 1,Curated Reporting View - Master Data
spark.sql(f"""
CREATE OR REPLACE VIEW {get_env()}curated.asset_performance.eppmSelfServiceMaster AS
SELECT * FROM {get_env()}curated.asset_performance.eppmSelfServiceMasterCurrent
UNION
SELECT *
,'' AS costCenterNumber
,'' AS costCenterShortDescription
,'' AS costCenterMediumDescription
,'' AS costCenterLongDescription
,'' AS profitCenterNumber
,'' AS profitCenterShortDescription
,'' AS profitCenterMediumDescription
FROM {get_env()}curated.asset_performance.eppmSelfServiceMasterHistory
""")

# COMMAND ----------

# DBTITLE 1,Classification Hierarchy
spark.sql(f"""
CREATE OR REPLACE VIEW {get_env()}curated.asset_performance.eppmSelfServiceClassificationHierarchy AS
    SELECT
    item.itemDetailGuid
    ,item.itemDetailId
    ,relBuck.bucketID as chBucketId
    ,buckTx.bucketIdDescription as chBucketIdDescription
    ,relBuck.parentBucketGuid as chParentBucketGUID
    ,relBuck1.bucketID as chParentBucketId
    ,buckTx1.bucketIdDescription as chParentBucketIdDescription
    ,relPort.portfolioId as chPortfolioId
    ,portTx.portfolioGuidDescription as chPortfolioIdDescription
    ,rel.financialPercentage as associationSplit
    FROM {get_env()}cleansed.ppm.rpm_item_d item 
    LEFT JOIN {get_env()}cleansed.ppm.rpm_relation_d rel on rel.guid2 = item.itemDetailGuid 
    LEFT JOIN {get_env()}cleansed.ppm.0rpm_bucket_guid_attr relBuck on relBuck.bucketGuid = rel.guid1 and rel.relationType = 'RHI'
    LEFT JOIN {get_env()}cleansed.ppm.0rpm_bucket_guid_id_text buckTx ON buckTx.bucketGuid = relbuck.bucketGuid
    LEFT JOIN {get_env()}cleansed.ppm.0rpm_port_guid_attr relPort on relPort.portGuid = relBuck.portfolioGuid
    LEFT JOIN {get_env()}cleansed.ppm.0rpm_port_guid_id_text portTx on portTx.portGuid = relport.portGuid
    LEFT JOIN {get_env()}cleansed.ppm.0rpm_bucket_guid_attr relBuck1 on relBuck1.bucketGuid = relBuck.parentBucketGuid
    LEFT JOIN {get_env()}cleansed.ppm.0rpm_bucket_guid_id_text buckTx1 ON buckTx1.bucketGuid = relbuck1.bucketGuid
""")

# COMMAND ----------

# DBTITLE 1,Transaction Data - Final View
spark.sql(f"""
CREATE OR REPLACE VIEW {get_env()}curated.asset_performance.eppmSelfServiceTransactionCurrent AS
    select 
    infoprovider
    ,objectnumberId
    ,fiscalYear
    ,fiscalYearPeriod
    ,fiscalYearVariant
    ,postingPeriod
    ,costElement
    ,version
    ,valuetype as valueType
    ,sum(amountInTransactionCurrency) as amountInTransactionCurrency
    ,transactionCurrency
    ,sum(amountInControllingAreaCurrency) as amountInControllingAreaCurrency
    ,controllingAreaCurrency 
    FROM (
    select 
    'COSS - Cost Totals for Internal Postings (BW - ZEPPM_D28)' as infoprovider
    ,objectnumber as objectnumberId
    ,fiscalYear
    ,fiscalYearPeriod
    ,'Z6' as fiscalYearVariant
    ,postingPeriod
    ,costElement
    ,version
    ,valuetype
    ,amountInTransactionCurrency
    ,transactionCurrency
    ,amountInControllingAreaCurrency
    ,'AUD' as controllingAreaCurrency 
    FROM {get_env()}cleansed.fin.eppmCossTranspose
    WHERE amountInTransactionCurrency <> 0

    UNION ALL

    select 
    'COSP - Cost Totals for External Postings (BW - ZEPPM_D29)' as infoprovider
    ,objectnumber as objectnumberId
    ,fiscalYear
    ,fiscalYearPeriod
    ,'Z6' as fiscalYearVariant
    ,postingPeriod
    ,costElement
    ,version
    ,valuetype
    ,amountInTransactionCurrency
    ,transactionCurrency
    ,amountInControllingAreaCurrency
    ,'AUD' as controllingAreaCurrency 
    FROM {get_env()}cleansed.fin.eppmCospTranspose   
    WHERE amountInTransactionCurrency <> 0

    UNION ALL

    select 
    'RPSCO - Project info database: Costs, revenues, finances (BW - ZEPPM_D03)' as infoprovider
    ,objectnumber as objectnumberId
    ,fiscalYear
    ,fiscalYearPeriod
    ,'Z6' as fiscalYearVariant
    ,postingPeriod
    ,'' as costElement
    ,planningVersion as version
    ,valuetype
    ,amountInTransactionCurrency
    ,transactionCurrency
    ,amountInControllingAreaCurrency
    ,'AUD' as controllingAreaCurrency 
    FROM {get_env()}cleansed.ppm.eppmRpscoTranspose
    WHERE amountInTransactionCurrency <> 0 and valuetype = '41'
    )
    WHERE (valuetype <> '04' or fiscalYear <> '2021' or version <> '000')
    GROUP BY
    infoprovider
    ,objectnumberId
    ,fiscalYear
    ,fiscalYearPeriod
    ,fiscalYearVariant
    ,postingPeriod
    ,costElement
    ,version
    ,valueType
    ,transactionCurrency
    ,controllingAreaCurrency 
""")

# COMMAND ----------

# DBTITLE 1,Curated Reporting View
spark.sql(f"""
CREATE OR REPLACE VIEW {get_env()}curated.asset_performance.eppmSelfServiceReportCurrent AS
    select 
    mast.*
    ,tran.infoprovider
    ,tran.fiscalYear
    ,tran.fiscalYearPeriod
    ,tran.fiscalYearVariant
    ,tran.postingPeriod
    ,tran.costElement
    ,costTx.costElementMeduimDescription as costElementMediumDescription
    ,costTx.costElementShortDescription
    ,tran.valuetype as valueType
    ,tran.version
    ,tran.amountInTransactionCurrency
    ,tran.transactionCurrency
    ,tran.amountInControllingAreaCurrency
    ,tran.controllingAreaCurrency 
    FROM {get_env()}curated.asset_performance.eppmSelfServiceMasterCurrent mast 
    LEFT JOIN {get_env()}curated.asset_performance.eppmSelfServiceTransactionCurrent tran ON tran.objectnumberId = mast.wbsElementObjectNumber
    LEFT JOIN {get_env()}cleansed.fin.0costelmnt_text costTx ON costTx.costElement = tran.costElement and costTx.controllingArea = '1000'
""")

# COMMAND ----------

# DBTITLE 1,Curated Reporting View - History
spark.sql(f"""
CREATE OR REPLACE VIEW {get_env()}curated.asset_performance.eppmSelfServiceReportHistory AS
    With viewEppmSelfServiceTransactionHistoryInter AS (
        SELECT 
        'EPPM History (BW - Z6)' as infoprovider
        ,fiscalYear
        ,fiscalYearPeriod
        ,fiscalYearVariant
        ,postingPeriod
        ,costCenterNumber
        ,profitCenterNumber
        ,lpad(glAccount,10,'0') as costElement
        ,wbsElement
        ,version
        ,valueType
        ,sum(amount) as amountInTransactionCurrency
        ,amountCurrency as transactionCurrency
        ,sum(amount) as amountInControllingAreaCurrency
        ,amountCurrency as controllingAreaCurrency 
        FROM (
            SELECT 
            'glHistory' as  infoprovider
            ,lpad(costCentre,10,'0') as costCenterNumber
            ,lpad(profitCentre,10,'0') as profitCenterNumber
            ,date.financialYear as fiscalYear
            ,concat(date.financialYear,lpad(date.monthOfFinancialYear,3,'0')) as fiscalYearPeriod
            ,'Z6' as fiscalYearVariant
            ,lpad(date.monthOfFinancialYear,3,'0') as postingPeriod 
            ,'' as forecastedYear
            ,'H' as dataIndicator
            ,wbsElement
            ,'1000' as controllingArea
            ,'000' as version
            ,lpad(valuetype,2,'0') as valueType
            ,glAccount as glAccount
            ,'1000' as chartOfAccounts
            ,amount
            ,currency as amountCurrency
            FROM {get_env()}cleansed.ppm.ds_gl_historical_data hist
            LEFT JOIN {get_env()}curated.dim.date date ON date.calendarDate = hist.postingDate 
            WHERE hist.ignoreFlag IS NULL AND hist.wbsElement IS NOT NULL

            UNION ALL

            SELECT
            'bpcHistory' as  infoprovider
            ,lpad(costCenter,10,'0') as costCenterNumber
            ,'' as profitCenterNumber
            ,fiscalYear
            ,fiscalYearPeriod
            ,'Z6' as fiscalYearVariant
            ,substring(fiscalYearPeriod,5,3) as postingPeriod
            ,forecastedYear
            ,'H' as dataIndicator
            ,wbsElement
            ,'1000' as controllingArea
            ,lpad(version, 3, '0') as version
            ,lpad(valuetype, 2,'0') as valueType
            ,costElement as glAccount
            ,'1000' as chartOfAccounts
            ,amountValue as amount
            ,'AUD' as amountCurrency
            FROM {get_env()}cleansed.ppm.ds_bpc_historical_data
            WHERE wbsElement IS NOT NULL
        )
        GROUP BY
        infoprovider
        ,fiscalYear
        ,fiscalYearPeriod
        ,fiscalYearVariant
        ,postingPeriod
        ,costCenterNumber
        ,profitCenterNumber
        ,glAccount
        ,wbsElement
        ,version
        ,valueType
        ,transactionCurrency
        ,controllingAreaCurrency),                                                                                                                                                                                                     
    viewEppmSelfServiceTransactionHistory AS (
        SELECT 
        tran.*
        ,costTx.glAccountLongText as costElementMeduimDescription
        ,costTx.glAccountShortText as costElementShortDescription
        FROM viewEppmSelfServiceTransactionHistoryInter tran
        LEFT JOIN {get_env()}cleansed.fin.0gl_account_text costTx ON costTx.glaccountnumber = tran.costElement and costTx.chartOfAccounts = '1000'
        )
        
    select 
    mast.itemDetailGUID
    ,mast.itemDetailId
    ,mast.itemDetailIdDescription
    ,mast.bucketGUID
    ,mast.bucketIdDescription
    ,mast.bucketId
    ,mast.parentBucketId
    ,mast.initiativeDetailGUID
    ,mast.initiativeId
    ,mast.initiativeDescription
    ,mast.portfolioGUID
    ,mast.portfolioId
    ,mast.portfolioIdDescription
    ,mast.projectApprovalStatusId
    ,mast.projectApprovalStatusDescription
    ,mast.requestingCostCenterNumber
    ,mast.requestingCostCenterShortDescription
    ,mast.requestingCostCenterMediumDescription
    ,mast.requestingCostCenterLongDescription
    ,mast.responsibleCostCenterNumber
    ,mast.responsibleCostCenterShortDescription
    ,mast.responsibleCostCenterMediumDescription
    ,mast.responsibleCostCenterLongDescription
    ,mast.itemActualStartDate
    ,mast.itemActualFinishDate
    ,mast.itemForecastStartDate
    ,mast.itemForecastFinishDate

    ,mast.itemPlannedStartDate
    ,mast.itemPlannedFinishDate
    ,mast.itemTypeId
    ,mast.itemTypeDescription
    ,mast.overallStatusId
    ,mast.itemOverallStatus
    ,mast.ipartSIRId
    ,mast.itemPrimeId 
    ,mast.projectIdDefinition
    ,mast.projectDescription
    ,mast.projectId 
    ,mast.projectProfile
    ,mast.projectProfileDescription
    ,mast.projectObjectNumberId
    ,mast.municipality
    ,mast.electorate
    ,mast.projectGeographyCode
    ,mast.projectGeographyDescription
    ,mast.cipAssetType  
    ,mast.cipAssetTypeDescription

    ,tran.wbsElement
    ,mast.wbsElementExternal
    ,mast.wbsElementDescription
    ,mast.wbsElementControllingArea
    ,mast.wbsElementObjectNumber
    ,mast.deliveryPartner
    ,mast.deliveryPartnerDescription
    ,mast.subDeliveryPartner
    ,mast.subDeliveryPartnerDescription
    ,mast.wbsElementProjectType
    ,mast.majorProjectAssociationId
    ,mast.majorProjectAssociationDescription
    ,mast.projectHierarchyLevel
    ,mast.systemStatusCode

    ,mast.decisionGUID
    ,mast.decisionGUIDDescription
    ,mast.decisionId
    ,mast.decisionIdDescription
    ,mast.decisionActiveFlag
    ,mast.decisionStatusCode
    ,mast.forecastedDecisionDate
    ,mast.plannedDecisiondate

    ,mast.projectManager
    ,mast.programManager
    ,mast.procurementResponsibleOwner
    ,mast.programDirector
    
    ,tran.costCenterNumber
    ,cctrTx.costCenterShortDescription
    ,cctrTx.costCenterMeduimDescription as costCenterMediumDescription
    ,cctrTx.costCenterLongDescription    
    ,tran.profitCenterNumber
    ,pcrtTx.shortDescription as profitCenterShortDescription
    ,pcrtTX.mediumDescription as profitCenterMediumDescription   
    ,tran.infoprovider
    ,tran.fiscalYear
    ,tran.fiscalYearPeriod
    ,tran.fiscalYearVariant
    ,tran.postingPeriod
    ,tran.costElement
    ,tran.costElementMeduimDescription
    ,tran.costElementShortDescription
    ,tran.valuetype as valueType
    ,tran.version
    ,tran.amountInTransactionCurrency
    ,tran.transactionCurrency
    ,tran.amountInControllingAreaCurrency
    ,tran.controllingAreaCurrency 

    FROM viewEppmSelfServiceTransactionHistory tran 
    LEFT JOIN {get_env()}curated.asset_performance.eppmSelfServiceMasterHistory mast ON mast.wbsElement = tran.wbsElement    
    LEFT JOIN {get_env()}cleansed.fin.0costcenter_text cctrTx ON cctrTx.costCenterNumber = tran.costCenterNumber AND cctrTx.controllingArea = '1000' AND cctrTx.validToDate = '9999-12-31'    
    LEFT JOIN {get_env()}cleansed.fin.0profit_ctr_text pcrtTx on pcrtTx.profitCenter = tran.profitCenterNumber AND pcrtTx.validToDate = '9999-12-31'

""")

# COMMAND ----------

# DBTITLE 1,Curated Reporting View - Consolidated
spark.sql(f"""
CREATE OR REPLACE VIEW {get_env()}curated.asset_performance.eppmSelfServiceReport AS
SELECT * FROM {get_env()}curated.asset_performance.eppmSelfServiceReportCurrent
UNION
SELECT * FROM {get_env()}curated.asset_performance.eppmSelfServiceReportHistory
""")

# COMMAND ----------

# DBTITLE 1,Curated Reporting Materialised View - Consolidated
# spark.sql(f"""
# CREATE OR REPLACE TABLE {get_env()}curated.asset_performance.eppmSelfServiceReportTab AS
# SELECT * FROM {get_env()}curated.asset_performance.eppmSelfServiceReportCurrent
# UNION
# SELECT * FROM {get_env()}curated.asset_performance.eppmSelfServiceReportHistory
# """)

# COMMAND ----------

# %sql
# ALTER VIEW ppd_curated.asset_performance.eppmselfserviceclassificationhierarchy OWNER TO `ppd-G3-Admins`;
# ALTER VIEW ppd_curated.asset_performance.eppmselfservicemastercurrent OWNER TO `ppd-G3-Admins`;
# ALTER VIEW ppd_curated.asset_performance.eppmselfservicemasterhistory OWNER TO `ppd-G3-Admins`;
# ALTER VIEW ppd_curated.asset_performance.eppmselfservicereport OWNER TO `ppd-G3-Admins`;
# ALTER VIEW ppd_curated.asset_performance.eppmselfservicereportcurrent OWNER TO `ppd-G3-Admins`;
# ALTER VIEW ppd_curated.asset_performance.eppmselfservicereporthistory OWNER TO `ppd-G3-Admins`;
# ALTER VIEW ppd_curated.asset_performance.eppmselfservicetransactioncurrent OWNER TO `ppd-G3-Admins`;
# ALTER VIEW ppd_curated.asset_performance.eppmSelfServiceMaster OWNER TO `ppd-G3-Admins`;
# -- ALTER TABLE ppd_curated.asset_performance.eppmSelfServiceReportTab OWNER TO `ppd-G3-Admins`;
# ALTER TABLE ppd_curated.asset_performance.eppmProjectStatusCommentary OWNER TO `ppd-G3-Admins`;
