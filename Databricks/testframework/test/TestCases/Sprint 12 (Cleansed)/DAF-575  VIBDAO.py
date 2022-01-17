# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = 'VIBDAO'

environment = 'test'
storage_account_name = "sablobdaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sablob-key")
containerName = "archive"


# COMMAND ----------

# MAGIC %run ../../includes/tableEvaluation

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC select 
# MAGIC architecturalObjectInternalId,
# MAGIC architecturalObjectId,
# MAGIC architecturalObjectTypeCode,
# MAGIC architecturalObjectType,
# MAGIC architecturalObjectNumber,
# MAGIC validFromDate,
# MAGIC validToDate,
# MAGIC partArchitecturalObjectId,
# MAGIC objectNumber,
# MAGIC firstEnteredBy,
# MAGIC firstEnteredDateTime,
# MAGIC firstEnteredSource,
# MAGIC employeeId,
# MAGIC lastEditedDateTime,
# MAGIC lastEdittedSource,
# MAGIC responsiblePerson,
# MAGIC exclusiveUser,
# MAGIC lastRelocationDate,
# MAGIC measurementStructure,
# MAGIC shortDescription,
# MAGIC reservationArea,
# MAGIC maintenanceDistrict,
# MAGIC businessEntityTransportConnectionsIndicator,
# MAGIC propertyNumber,
# MAGIC propertyCreatedDate,
# MAGIC propertyLotNumber,
# MAGIC propertyRequestNumber,
# MAGIC planTypeCode,
# MAGIC planType,
# MAGIC planNumber,
# MAGIC processTypeCode,
# MAGIC processType,
# MAGIC addressLotNumber,
# MAGIC lotTypeCode,
# MAGIC unitEntitlement,
# MAGIC flatCount,
# MAGIC superiorPropertyTypeCode,
# MAGIC superiorPropertyType,
# MAGIC inferiorPropertyTypeCode,
# MAGIC inferiorPropertyType,
# MAGIC stormWaterAssesmentIndicator,
# MAGIC mlimIndicator,
# MAGIC wicaIndicator,
# MAGIC sopaIndicator,
# MAGIC communityTitleIndicator,
# MAGIC sectionNumber,
# MAGIC hydraCalculatedArea,
# MAGIC hydraAreaUnit,
# MAGIC hydraAreaIndicator,
# MAGIC caseNumberIndicator,
# MAGIC overrideArea,
# MAGIC overrideAreaUnit,
# MAGIC cancellationDate,
# MAGIC cancellationReasonCode,
# MAGIC comments,
# MAGIC propertyInfo
# MAGIC from
# MAGIC (SELECT
# MAGIC INTRENO	as	architecturalObjectInternalId
# MAGIC ,AOID	as	architecturalObjectId
# MAGIC ,a.AOTYPE	as	architecturalObjectTypeCode
# MAGIC ,b.XMAOTYPE	as	architecturalObjectType
# MAGIC ,AONR	as	architecturalObjectNumber
# MAGIC ,VALIDFROM	as	validFromDate
# MAGIC ,VALIDTO	as	validToDate
# MAGIC ,PARTAOID	as	partArchitecturalObjectId
# MAGIC ,OBJNR	as	objectNumber
# MAGIC ,RERF	as	firstEnteredBy
# MAGIC ,cast((concat(DERF,TERF)) as timestamp) as firstEnteredDateTime
# MAGIC ,REHER	as	firstEnteredSource
# MAGIC ,RBEAR	as	employeeId
# MAGIC ,cast((concat(DBEAR,TBEAR))as timestamp) as lastEditedDateTime
# MAGIC ,RBHER	as	lastEdittedSource
# MAGIC ,RESPONSIBLE	as	responsiblePerson
# MAGIC ,USEREXCLUSIVE	as	exclusiveUser
# MAGIC ,LASTRENO	as	lastRelocationDate
# MAGIC ,MEASSTRC	as	measurementStructure
# MAGIC ,DOORPLT	as	shortDescription
# MAGIC ,RSAREA	as	reservationArea
# MAGIC ,SINSTBEZ	as	maintenanceDistrict
# MAGIC ,SVERKEHR	as	businessEntityTransportConnectionsIndicator
# MAGIC ,ZCD_PROPERTY_NO	as	propertyNumber
# MAGIC ,ZCD_PROP_CR_DATE	as	propertyCreatedDate
# MAGIC ,ZCD_PROP_LOT_NO	as	propertyLotNumber
# MAGIC ,ZCD_REQUEST_NO	as	propertyRequestNumber
# MAGIC ,ZCD_PLAN_TYPE	as	planTypeCode
# MAGIC ,c.DESCRIPTION	as	planType
# MAGIC ,ZCD_PLAN_NUMBER	as	planNumber
# MAGIC ,ZCD_PROCESS_TYPE	as	processTypeCode
# MAGIC ,d.DESCRIPTION	as	processType
# MAGIC ,ZCD_ADDR_LOT_NO	as	addressLotNumber
# MAGIC ,ZCD_LOT_TYPE	as	lotTypeCode
# MAGIC ,ZCD_UNIT_ENTITLEMENT	as	unitEntitlement
# MAGIC ,ZCD_NO_OF_FLATS	as	flatCount
# MAGIC ,ZCD_SUP_PROP_TYPE	as	superiorPropertyTypeCode
# MAGIC ,e.superiorPropertyType	as	superiorPropertyType
# MAGIC ,ZCD_INF_PROP_TYPE	as	inferiorPropertyTypeCode
# MAGIC ,f.inferiorPropertyType	as	inferiorPropertyType
# MAGIC ,ZCD_STORM_WATER_ASSESS	as	stormWaterAssesmentIndicator
# MAGIC ,ZCD_IND_MLIM	as	mlimIndicator
# MAGIC ,ZCD_IND_WICA	as	wicaIndicator
# MAGIC ,ZCD_IND_SOPA	as	sopaIndicator
# MAGIC ,ZCD_IND_COMMUNITY_TITLE	as	communityTitleIndicator
# MAGIC ,ZCD_SECTION_NUMBER	as	sectionNumber
# MAGIC ,ZCD_HYDRA_CALC_AREA	as	hydraCalculatedArea
# MAGIC ,ZCD_HYDRA_AREA_UNIT	as	hydraAreaUnit
# MAGIC ,ZCD_HYDRA_AREA_FLAG	as	hydraAreaIndicator
# MAGIC ,ZCD_CASENO_FLAG	as	caseNumberIndicator
# MAGIC ,ZCD_OVERRIDE_AREA	as	overrideArea
# MAGIC ,ZCD_OVERRIDE_AREA_UNIT	as	overrideAreaUnit
# MAGIC ,ZCD_CANCELLATION_DATE	as	cancellationDate
# MAGIC ,ZCD_CANC_REASON	as	cancellationReasonCode
# MAGIC ,ZCD_COMMENTS	as	comments
# MAGIC ,ZCD_PROPERTY_INFO	as	propertyInfo
# MAGIC ,row_number() over (partition by INTRENO order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} a
# MAGIC left join cleansed.isu_tivbdarobjtypet b
# MAGIC on a.AOTYPE = b.AOTYPE --and d.SPRAS = 'E'
# MAGIC left join cleansed.isu_zcd_tplantype_tx c
# MAGIC on a.ZCD_PLAN_TYPE = c.PLAN_TYPE --and e.LANGU ='E'
# MAGIC left join cleansed.isu_zcd_tproctype_tx d
# MAGIC on a.ZCD_PROCESS_TYPE = d.PROCESS_TYPE --and f.LANGUE='E'
# MAGIC left join cleansed.isu_zcd_tsupprtyp_tx e
# MAGIC on a.ZCD_SUP_PROP_TYPE = e.superiorPropertyTypeCode
# MAGIC left join cleansed.isu_zcd_tinfprty_tx f
# MAGIC on a.ZCD_INF_PROP_TYPE = f.inferiorPropertyTypeCode)a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select 
# MAGIC architecturalObjectInternalId,
# MAGIC architecturalObjectId,
# MAGIC architecturalObjectTypeCode,
# MAGIC architecturalObjectType,
# MAGIC architecturalObjectNumber,
# MAGIC validFromDate,
# MAGIC validToDate,
# MAGIC partArchitecturalObjectId,
# MAGIC objectNumber,
# MAGIC firstEnteredBy,
# MAGIC firstEnteredDateTime,
# MAGIC firstEnteredSource,
# MAGIC employeeId,
# MAGIC lastEditedDateTime,
# MAGIC lastEdittedSource,
# MAGIC responsiblePerson,
# MAGIC exclusiveUser,
# MAGIC lastRelocationDate,
# MAGIC measurementStructure,
# MAGIC shortDescription,
# MAGIC reservationArea,
# MAGIC maintenanceDistrict,
# MAGIC businessEntityTransportConnectionsIndicator,
# MAGIC propertyNumber,
# MAGIC propertyCreatedDate,
# MAGIC propertyLotNumber,
# MAGIC propertyRequestNumber,
# MAGIC planTypeCode,
# MAGIC planType,
# MAGIC planNumber,
# MAGIC processTypeCode,
# MAGIC processType,
# MAGIC addressLotNumber,
# MAGIC lotTypeCode,
# MAGIC unitEntitlement,
# MAGIC flatCount,
# MAGIC superiorPropertyTypeCode,
# MAGIC superiorPropertyType,
# MAGIC inferiorPropertyTypeCode,
# MAGIC inferiorPropertyType,
# MAGIC stormWaterAssesmentIndicator,
# MAGIC mlimIndicator,
# MAGIC wicaIndicator,
# MAGIC sopaIndicator,
# MAGIC communityTitleIndicator,
# MAGIC sectionNumber,
# MAGIC hydraCalculatedArea,
# MAGIC hydraAreaUnit,
# MAGIC hydraAreaIndicator,
# MAGIC caseNumberIndicator,
# MAGIC overrideArea,
# MAGIC overrideAreaUnit,
# MAGIC cancellationDate,
# MAGIC cancellationReasonCode,
# MAGIC comments,
# MAGIC propertyInfo
# MAGIC from
# MAGIC (SELECT
# MAGIC INTRENO	as	architecturalObjectInternalId
# MAGIC ,AOID	as	architecturalObjectId
# MAGIC ,a.AOTYPE	as	architecturalObjectTypeCode
# MAGIC ,b.XMAOTYPE	as	architecturalObjectType
# MAGIC ,AONR	as	architecturalObjectNumber
# MAGIC ,VALIDFROM	as	validFromDate
# MAGIC ,VALIDTO	as	validToDate
# MAGIC ,PARTAOID	as	partArchitecturalObjectId
# MAGIC ,OBJNR	as	objectNumber
# MAGIC ,RERF	as	firstEnteredBy
# MAGIC ,cast((concat(DERF,TERF)) as timestamp) as firstEnteredDateTime
# MAGIC ,REHER	as	firstEnteredSource
# MAGIC ,RBEAR	as	employeeId
# MAGIC ,cast((concat(DBEAR,TBEAR))as timestamp) as lastEditedDateTime
# MAGIC ,RBHER	as	lastEdittedSource
# MAGIC ,RESPONSIBLE	as	responsiblePerson
# MAGIC ,USEREXCLUSIVE	as	exclusiveUser
# MAGIC ,LASTRENO	as	lastRelocationDate
# MAGIC ,MEASSTRC	as	measurementStructure
# MAGIC ,DOORPLT	as	shortDescription
# MAGIC ,RSAREA	as	reservationArea
# MAGIC ,SINSTBEZ	as	maintenanceDistrict
# MAGIC ,SVERKEHR	as	businessEntityTransportConnectionsIndicator
# MAGIC ,ZCD_PROPERTY_NO	as	propertyNumber
# MAGIC ,ZCD_PROP_CR_DATE	as	propertyCreatedDate
# MAGIC ,ZCD_PROP_LOT_NO	as	propertyLotNumber
# MAGIC ,ZCD_REQUEST_NO	as	propertyRequestNumber
# MAGIC ,ZCD_PLAN_TYPE	as	planTypeCode
# MAGIC ,c.DESCRIPTION	as	planType
# MAGIC ,ZCD_PLAN_NUMBER	as	planNumber
# MAGIC ,ZCD_PROCESS_TYPE	as	processTypeCode
# MAGIC ,d.DESCRIPTION	as	processType
# MAGIC ,ZCD_ADDR_LOT_NO	as	addressLotNumber
# MAGIC ,ZCD_LOT_TYPE	as	lotTypeCode
# MAGIC ,ZCD_UNIT_ENTITLEMENT	as	unitEntitlement
# MAGIC ,ZCD_NO_OF_FLATS	as	flatCount
# MAGIC ,ZCD_SUP_PROP_TYPE	as	superiorPropertyTypeCode
# MAGIC ,e.superiorPropertyType	as	superiorPropertyType
# MAGIC ,ZCD_INF_PROP_TYPE	as	inferiorPropertyTypeCode
# MAGIC ,f.inferiorPropertyType	as	inferiorPropertyType
# MAGIC ,ZCD_STORM_WATER_ASSESS	as	stormWaterAssesmentIndicator
# MAGIC ,ZCD_IND_MLIM	as	mlimIndicator
# MAGIC ,ZCD_IND_WICA	as	wicaIndicator
# MAGIC ,ZCD_IND_SOPA	as	sopaIndicator
# MAGIC ,ZCD_IND_COMMUNITY_TITLE	as	communityTitleIndicator
# MAGIC ,ZCD_SECTION_NUMBER	as	sectionNumber
# MAGIC ,ZCD_HYDRA_CALC_AREA	as	hydraCalculatedArea
# MAGIC ,ZCD_HYDRA_AREA_UNIT	as	hydraAreaUnit
# MAGIC ,ZCD_HYDRA_AREA_FLAG	as	hydraAreaIndicator
# MAGIC ,ZCD_CASENO_FLAG	as	caseNumberIndicator
# MAGIC ,ZCD_OVERRIDE_AREA	as	overrideArea
# MAGIC ,ZCD_OVERRIDE_AREA_UNIT	as	overrideAreaUnit
# MAGIC ,ZCD_CANCELLATION_DATE	as	cancellationDate
# MAGIC ,ZCD_CANC_REASON	as	cancellationReasonCode
# MAGIC ,ZCD_COMMENTS	as	comments
# MAGIC ,ZCD_PROPERTY_INFO	as	propertyInfo
# MAGIC ,row_number() over (partition by INTRENO order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} a
# MAGIC left join cleansed.isu_tivbdarobjtypet b
# MAGIC on a.AOTYPE = b.AOTYPE --and d.SPRAS = 'E'
# MAGIC left join cleansed.isu_zcd_tplantype_tx c
# MAGIC on a.ZCD_PLAN_TYPE = c.PLAN_TYPE --and e.LANGU ='E'
# MAGIC left join cleansed.isu_zcd_tproctype_tx d
# MAGIC on a.ZCD_PROCESS_TYPE = d.PROCESS_TYPE --and f.LANGUE='E'
# MAGIC left join cleansed.isu_zcd_tsupprtyp_tx e
# MAGIC on a.ZCD_SUP_PROP_TYPE = e.superiorPropertyTypeCode
# MAGIC left join cleansed.isu_zcd_tinfprty_tx f
# MAGIC on a.ZCD_INF_PROP_TYPE = f.inferiorPropertyTypeCode)a where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT architecturalObjectInternalId,architecturalObjectId,architecturalObjectTypeCode,architecturalObjectType,
# MAGIC architecturalObjectNumber,validFromDate,validToDate,partArchitecturalObjectId,objectNumber,firstEnteredBy,
# MAGIC firstEnteredDateTime,firstEnteredSource,employeeId,lastEditedDateTime,lastEdittedSource,responsiblePerson,
# MAGIC exclusiveUser,lastRelocationDate,measurementStructure,shortDescription,reservationArea,maintenanceDistrict,
# MAGIC businessEntityTransportConnectionsIndicator,propertyNumber,propertyCreatedDate,propertyLotNumber,propertyRequestNumber,
# MAGIC planTypeCode,planType,planNumber,processTypeCode,processType,addressLotNumber,lotTypeCode,unitEntitlement,flatCount,
# MAGIC superiorPropertyTypeCode,superiorPropertyType,inferiorPropertyTypeCode,inferiorPropertyType,stormWaterAssesmentIndicator,
# MAGIC mlimIndicator,wicaIndicator,sopaIndicator,communityTitleIndicator,sectionNumber,hydraCalculatedArea,hydraAreaUnit,
# MAGIC hydraAreaIndicator,caseNumberIndicator,overrideArea,overrideAreaUnit,cancellationDate,cancellationReasonCode,
# MAGIC comments,propertyInfo, COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY architecturalObjectInternalId,architecturalObjectId,architecturalObjectTypeCode,architecturalObjectType,
# MAGIC architecturalObjectNumber,validFromDate,validToDate,partArchitecturalObjectId,objectNumber,firstEnteredBy,
# MAGIC firstEnteredDateTime,firstEnteredSource,employeeId,lastEditedDateTime,lastEdittedSource,responsiblePerson,
# MAGIC exclusiveUser,lastRelocationDate,measurementStructure,shortDescription,reservationArea,maintenanceDistrict,
# MAGIC businessEntityTransportConnectionsIndicator,propertyNumber,propertyCreatedDate,propertyLotNumber,propertyRequestNumber,
# MAGIC planTypeCode,planType,planNumber,processTypeCode,processType,addressLotNumber,lotTypeCode,unitEntitlement,flatCount,
# MAGIC superiorPropertyTypeCode,superiorPropertyType,inferiorPropertyTypeCode,inferiorPropertyType,stormWaterAssesmentIndicator,
# MAGIC mlimIndicator,wicaIndicator,sopaIndicator,communityTitleIndicator,sectionNumber,hydraCalculatedArea,hydraAreaUnit,
# MAGIC hydraAreaIndicator,caseNumberIndicator,overrideArea,overrideAreaUnit,cancellationDate,cancellationReasonCode,
# MAGIC comments,propertyInfo
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY architecturalObjectInternalId
# MAGIC order by architecturalObjectInternalId) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC (select 
# MAGIC architecturalObjectInternalId,
# MAGIC architecturalObjectId,
# MAGIC architecturalObjectTypeCode,
# MAGIC architecturalObjectType,
# MAGIC architecturalObjectNumber,
# MAGIC validFromDate,
# MAGIC validToDate,
# MAGIC partArchitecturalObjectId,
# MAGIC objectNumber,
# MAGIC firstEnteredBy,
# MAGIC firstEnteredDateTime,
# MAGIC firstEnteredSource,
# MAGIC employeeId,
# MAGIC lastEditedDateTime,
# MAGIC lastEdittedSource,
# MAGIC responsiblePerson,
# MAGIC exclusiveUser,
# MAGIC lastRelocationDate,
# MAGIC measurementStructure,
# MAGIC shortDescription,
# MAGIC reservationArea,
# MAGIC maintenanceDistrict,
# MAGIC businessEntityTransportConnectionsIndicator,
# MAGIC propertyNumber,
# MAGIC propertyCreatedDate,
# MAGIC propertyLotNumber,
# MAGIC propertyRequestNumber,
# MAGIC planTypeCode,
# MAGIC planType,
# MAGIC planNumber,
# MAGIC processTypeCode,
# MAGIC processType,
# MAGIC addressLotNumber,
# MAGIC lotTypeCode,
# MAGIC unitEntitlement,
# MAGIC flatCount,
# MAGIC superiorPropertyTypeCode,
# MAGIC superiorPropertyType,
# MAGIC inferiorPropertyTypeCode,
# MAGIC inferiorPropertyType,
# MAGIC stormWaterAssesmentIndicator,
# MAGIC mlimIndicator,
# MAGIC wicaIndicator,
# MAGIC sopaIndicator,
# MAGIC communityTitleIndicator,
# MAGIC sectionNumber,
# MAGIC hydraCalculatedArea,
# MAGIC hydraAreaUnit,
# MAGIC hydraAreaIndicator,
# MAGIC caseNumberIndicator,
# MAGIC overrideArea,
# MAGIC overrideAreaUnit,
# MAGIC cancellationDate,
# MAGIC cancellationReasonCode,
# MAGIC comments,
# MAGIC propertyInfo
# MAGIC from
# MAGIC (SELECT
# MAGIC INTRENO	as	architecturalObjectInternalId
# MAGIC ,AOID	as	architecturalObjectId
# MAGIC ,a.AOTYPE	as	architecturalObjectTypeCode
# MAGIC ,b.XMAOTYPE	as	architecturalObjectType
# MAGIC ,AONR	as	architecturalObjectNumber
# MAGIC ,VALIDFROM	as	validFromDate
# MAGIC ,VALIDTO	as	validToDate
# MAGIC ,PARTAOID	as	partArchitecturalObjectId
# MAGIC ,OBJNR	as	objectNumber
# MAGIC ,RERF	as	firstEnteredBy
# MAGIC ,cast((concat(DERF,TERF)) as timestamp) as firstEnteredDateTime
# MAGIC ,REHER	as	firstEnteredSource
# MAGIC ,RBEAR	as	employeeId
# MAGIC ,cast((concat(DBEAR,TBEAR))as timestamp) as lastEditedDateTime
# MAGIC ,RBHER	as	lastEdittedSource
# MAGIC ,RESPONSIBLE	as	responsiblePerson
# MAGIC ,USEREXCLUSIVE	as	exclusiveUser
# MAGIC ,LASTRENO	as	lastRelocationDate
# MAGIC ,MEASSTRC	as	measurementStructure
# MAGIC ,DOORPLT	as	shortDescription
# MAGIC ,RSAREA	as	reservationArea
# MAGIC ,SINSTBEZ	as	maintenanceDistrict
# MAGIC ,SVERKEHR	as	businessEntityTransportConnectionsIndicator
# MAGIC ,ZCD_PROPERTY_NO	as	propertyNumber
# MAGIC ,ZCD_PROP_CR_DATE	as	propertyCreatedDate
# MAGIC ,ZCD_PROP_LOT_NO	as	propertyLotNumber
# MAGIC ,ZCD_REQUEST_NO	as	propertyRequestNumber
# MAGIC ,ZCD_PLAN_TYPE	as	planTypeCode
# MAGIC ,c.DESCRIPTION	as	planType
# MAGIC ,ZCD_PLAN_NUMBER	as	planNumber
# MAGIC ,ZCD_PROCESS_TYPE	as	processTypeCode
# MAGIC ,d.DESCRIPTION	as	processType
# MAGIC ,ZCD_ADDR_LOT_NO	as	addressLotNumber
# MAGIC ,ZCD_LOT_TYPE	as	lotTypeCode
# MAGIC ,ZCD_UNIT_ENTITLEMENT	as	unitEntitlement
# MAGIC ,ZCD_NO_OF_FLATS	as	flatCount
# MAGIC ,ZCD_SUP_PROP_TYPE	as	superiorPropertyTypeCode
# MAGIC ,e.superiorPropertyType	as	superiorPropertyType
# MAGIC ,ZCD_INF_PROP_TYPE	as	inferiorPropertyTypeCode
# MAGIC ,f.inferiorPropertyType	as	inferiorPropertyType
# MAGIC ,ZCD_STORM_WATER_ASSESS	as	stormWaterAssesmentIndicator
# MAGIC ,ZCD_IND_MLIM	as	mlimIndicator
# MAGIC ,ZCD_IND_WICA	as	wicaIndicator
# MAGIC ,ZCD_IND_SOPA	as	sopaIndicator
# MAGIC ,ZCD_IND_COMMUNITY_TITLE	as	communityTitleIndicator
# MAGIC ,ZCD_SECTION_NUMBER	as	sectionNumber
# MAGIC ,ZCD_HYDRA_CALC_AREA	as	hydraCalculatedArea
# MAGIC ,ZCD_HYDRA_AREA_UNIT	as	hydraAreaUnit
# MAGIC ,ZCD_HYDRA_AREA_FLAG	as	hydraAreaIndicator
# MAGIC ,ZCD_CASENO_FLAG	as	caseNumberIndicator
# MAGIC ,ZCD_OVERRIDE_AREA	as	overrideArea
# MAGIC ,ZCD_OVERRIDE_AREA_UNIT	as	overrideAreaUnit
# MAGIC ,ZCD_CANCELLATION_DATE	as	cancellationDate
# MAGIC ,ZCD_CANC_REASON	as	cancellationReasonCode
# MAGIC ,ZCD_COMMENTS	as	comments
# MAGIC ,ZCD_PROPERTY_INFO	as	propertyInfo
# MAGIC ,row_number() over (partition by INTRENO order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} a
# MAGIC left join cleansed.isu_tivbdarobjtypet b
# MAGIC on a.AOTYPE = b.AOTYPE --and d.SPRAS = 'E'
# MAGIC left join cleansed.isu_zcd_tplantype_tx c
# MAGIC on a.ZCD_PLAN_TYPE = c.PLAN_TYPE --and e.LANGU ='E'
# MAGIC left join cleansed.isu_zcd_tproctype_tx d
# MAGIC on a.ZCD_PROCESS_TYPE = d.PROCESS_TYPE --and f.LANGUE='E'
# MAGIC left join cleansed.isu_zcd_tsupprtyp_tx e
# MAGIC on a.ZCD_SUP_PROP_TYPE = e.superiorPropertyTypeCode
# MAGIC left join cleansed.isu_zcd_tinfprty_tx f
# MAGIC on a.ZCD_INF_PROP_TYPE = f.inferiorPropertyTypeCode)a where  a.rn = 1)
# MAGIC except
# MAGIC select
# MAGIC architecturalObjectInternalId,
# MAGIC architecturalObjectId,
# MAGIC architecturalObjectTypeCode,
# MAGIC architecturalObjectType,
# MAGIC architecturalObjectNumber,
# MAGIC validFromDate,
# MAGIC validToDate,
# MAGIC partArchitecturalObjectId,
# MAGIC objectNumber,
# MAGIC firstEnteredBy,
# MAGIC firstEnteredDateTime,
# MAGIC firstEnteredSource,
# MAGIC employeeId,
# MAGIC lastEditedDateTime,
# MAGIC lastEdittedSource,
# MAGIC responsiblePerson,
# MAGIC exclusiveUser,
# MAGIC lastRelocationDate,
# MAGIC measurementStructure,
# MAGIC shortDescription,
# MAGIC reservationArea,
# MAGIC maintenanceDistrict,
# MAGIC businessEntityTransportConnectionsIndicator,
# MAGIC propertyNumber,
# MAGIC propertyCreatedDate,
# MAGIC propertyLotNumber,
# MAGIC propertyRequestNumber,
# MAGIC planTypeCode,
# MAGIC planType,
# MAGIC planNumber,
# MAGIC processTypeCode,
# MAGIC processType,
# MAGIC addressLotNumber,
# MAGIC lotTypeCode,
# MAGIC unitEntitlement,
# MAGIC flatCount,
# MAGIC superiorPropertyTypeCode,
# MAGIC superiorPropertyType,
# MAGIC inferiorPropertyTypeCode,
# MAGIC inferiorPropertyType,
# MAGIC stormWaterAssesmentIndicator,
# MAGIC mlimIndicator,
# MAGIC wicaIndicator,
# MAGIC sopaIndicator,
# MAGIC communityTitleIndicator,
# MAGIC sectionNumber,
# MAGIC hydraCalculatedArea,
# MAGIC hydraAreaUnit,
# MAGIC hydraAreaIndicator,
# MAGIC caseNumberIndicator,
# MAGIC overrideArea,
# MAGIC overrideAreaUnit,
# MAGIC cancellationDate,
# MAGIC cancellationReasonCode,
# MAGIC comments,
# MAGIC propertyInfo
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC architecturalObjectInternalId,
# MAGIC architecturalObjectId,
# MAGIC architecturalObjectTypeCode,
# MAGIC architecturalObjectType,
# MAGIC architecturalObjectNumber,
# MAGIC validFromDate,
# MAGIC validToDate,
# MAGIC partArchitecturalObjectId,
# MAGIC objectNumber,
# MAGIC firstEnteredBy,
# MAGIC firstEnteredDateTime,
# MAGIC firstEnteredSource,
# MAGIC employeeId,
# MAGIC lastEditedDateTime,
# MAGIC lastEdittedSource,
# MAGIC responsiblePerson,
# MAGIC exclusiveUser,
# MAGIC lastRelocationDate,
# MAGIC measurementStructure,
# MAGIC shortDescription,
# MAGIC reservationArea,
# MAGIC maintenanceDistrict,
# MAGIC businessEntityTransportConnectionsIndicator,
# MAGIC propertyNumber,
# MAGIC propertyCreatedDate,
# MAGIC propertyLotNumber,
# MAGIC propertyRequestNumber,
# MAGIC planTypeCode,
# MAGIC planType,
# MAGIC planNumber,
# MAGIC processTypeCode,
# MAGIC processType,
# MAGIC addressLotNumber,
# MAGIC lotTypeCode,
# MAGIC unitEntitlement,
# MAGIC flatCount,
# MAGIC superiorPropertyTypeCode,
# MAGIC superiorPropertyType,
# MAGIC inferiorPropertyTypeCode,
# MAGIC inferiorPropertyType,
# MAGIC stormWaterAssesmentIndicator,
# MAGIC mlimIndicator,
# MAGIC wicaIndicator,
# MAGIC sopaIndicator,
# MAGIC communityTitleIndicator,
# MAGIC sectionNumber,
# MAGIC hydraCalculatedArea,
# MAGIC hydraAreaUnit,
# MAGIC hydraAreaIndicator,
# MAGIC caseNumberIndicator,
# MAGIC overrideArea,
# MAGIC overrideAreaUnit,
# MAGIC cancellationDate,
# MAGIC cancellationReasonCode,
# MAGIC comments,
# MAGIC propertyInfo
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select 
# MAGIC architecturalObjectInternalId,
# MAGIC architecturalObjectId,
# MAGIC architecturalObjectTypeCode,
# MAGIC architecturalObjectType,
# MAGIC architecturalObjectNumber,
# MAGIC validFromDate,
# MAGIC validToDate,
# MAGIC partArchitecturalObjectId,
# MAGIC objectNumber,
# MAGIC firstEnteredBy,
# MAGIC firstEnteredDateTime,
# MAGIC firstEnteredSource,
# MAGIC employeeId,
# MAGIC lastEditedDateTime,
# MAGIC lastEdittedSource,
# MAGIC responsiblePerson,
# MAGIC exclusiveUser,
# MAGIC lastRelocationDate,
# MAGIC measurementStructure,
# MAGIC shortDescription,
# MAGIC reservationArea,
# MAGIC maintenanceDistrict,
# MAGIC businessEntityTransportConnectionsIndicator,
# MAGIC propertyNumber,
# MAGIC propertyCreatedDate,
# MAGIC propertyLotNumber,
# MAGIC propertyRequestNumber,
# MAGIC planTypeCode,
# MAGIC planType,
# MAGIC planNumber,
# MAGIC processTypeCode,
# MAGIC processType,
# MAGIC addressLotNumber,
# MAGIC lotTypeCode,
# MAGIC unitEntitlement,
# MAGIC flatCount,
# MAGIC superiorPropertyTypeCode,
# MAGIC superiorPropertyType,
# MAGIC inferiorPropertyTypeCode,
# MAGIC inferiorPropertyType,
# MAGIC stormWaterAssesmentIndicator,
# MAGIC mlimIndicator,
# MAGIC wicaIndicator,
# MAGIC sopaIndicator,
# MAGIC communityTitleIndicator,
# MAGIC sectionNumber,
# MAGIC hydraCalculatedArea,
# MAGIC hydraAreaUnit,
# MAGIC hydraAreaIndicator,
# MAGIC caseNumberIndicator,
# MAGIC overrideArea,
# MAGIC overrideAreaUnit,
# MAGIC cancellationDate,
# MAGIC cancellationReasonCode,
# MAGIC comments,
# MAGIC propertyInfo
# MAGIC from
# MAGIC (SELECT
# MAGIC INTRENO	as	architecturalObjectInternalId
# MAGIC ,AOID	as	architecturalObjectId
# MAGIC ,a.AOTYPE	as	architecturalObjectTypeCode
# MAGIC ,b.XMAOTYPE	as	architecturalObjectType
# MAGIC ,AONR	as	architecturalObjectNumber
# MAGIC ,VALIDFROM	as	validFromDate
# MAGIC ,VALIDTO	as	validToDate
# MAGIC ,PARTAOID	as	partArchitecturalObjectId
# MAGIC ,OBJNR	as	objectNumber
# MAGIC ,RERF	as	firstEnteredBy
# MAGIC ,cast((concat(DERF,TERF)) as timestamp) as firstEnteredDateTime
# MAGIC ,REHER	as	firstEnteredSource
# MAGIC ,RBEAR	as	employeeId
# MAGIC ,cast((concat(DBEAR,TBEAR))as timestamp) as lastEditedDateTime
# MAGIC ,RBHER	as	lastEdittedSource
# MAGIC ,RESPONSIBLE	as	responsiblePerson
# MAGIC ,USEREXCLUSIVE	as	exclusiveUser
# MAGIC ,LASTRENO	as	lastRelocationDate
# MAGIC ,MEASSTRC	as	measurementStructure
# MAGIC ,DOORPLT	as	shortDescription
# MAGIC ,RSAREA	as	reservationArea
# MAGIC ,SINSTBEZ	as	maintenanceDistrict
# MAGIC ,SVERKEHR	as	businessEntityTransportConnectionsIndicator
# MAGIC ,ZCD_PROPERTY_NO	as	propertyNumber
# MAGIC ,ZCD_PROP_CR_DATE	as	propertyCreatedDate
# MAGIC ,ZCD_PROP_LOT_NO	as	propertyLotNumber
# MAGIC ,ZCD_REQUEST_NO	as	propertyRequestNumber
# MAGIC ,ZCD_PLAN_TYPE	as	planTypeCode
# MAGIC ,c.DESCRIPTION	as	planType
# MAGIC ,ZCD_PLAN_NUMBER	as	planNumber
# MAGIC ,ZCD_PROCESS_TYPE	as	processTypeCode
# MAGIC ,d.DESCRIPTION	as	processType
# MAGIC ,ZCD_ADDR_LOT_NO	as	addressLotNumber
# MAGIC ,ZCD_LOT_TYPE	as	lotTypeCode
# MAGIC ,ZCD_UNIT_ENTITLEMENT	as	unitEntitlement
# MAGIC ,ZCD_NO_OF_FLATS	as	flatCount
# MAGIC ,ZCD_SUP_PROP_TYPE	as	superiorPropertyTypeCode
# MAGIC ,e.superiorPropertyType	as	superiorPropertyType
# MAGIC ,ZCD_INF_PROP_TYPE	as	inferiorPropertyTypeCode
# MAGIC ,f.inferiorPropertyType	as	inferiorPropertyType
# MAGIC ,ZCD_STORM_WATER_ASSESS	as	stormWaterAssesmentIndicator
# MAGIC ,ZCD_IND_MLIM	as	mlimIndicator
# MAGIC ,ZCD_IND_WICA	as	wicaIndicator
# MAGIC ,ZCD_IND_SOPA	as	sopaIndicator
# MAGIC ,ZCD_IND_COMMUNITY_TITLE	as	communityTitleIndicator
# MAGIC ,ZCD_SECTION_NUMBER	as	sectionNumber
# MAGIC ,ZCD_HYDRA_CALC_AREA	as	hydraCalculatedArea
# MAGIC ,ZCD_HYDRA_AREA_UNIT	as	hydraAreaUnit
# MAGIC ,ZCD_HYDRA_AREA_FLAG	as	hydraAreaIndicator
# MAGIC ,ZCD_CASENO_FLAG	as	caseNumberIndicator
# MAGIC ,ZCD_OVERRIDE_AREA	as	overrideArea
# MAGIC ,ZCD_OVERRIDE_AREA_UNIT	as	overrideAreaUnit
# MAGIC ,ZCD_CANCELLATION_DATE	as	cancellationDate
# MAGIC ,ZCD_CANC_REASON	as	cancellationReasonCode
# MAGIC ,ZCD_COMMENTS	as	comments
# MAGIC ,ZCD_PROPERTY_INFO	as	propertyInfo
# MAGIC ,row_number() over (partition by INTRENO order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} a
# MAGIC left join cleansed.isu_tivbdarobjtypet b
# MAGIC on a.AOTYPE = b.AOTYPE --and d.SPRAS = 'E'
# MAGIC left join cleansed.isu_zcd_tplantype_tx c
# MAGIC on a.ZCD_PLAN_TYPE = c.PLAN_TYPE --and e.LANGU ='E'
# MAGIC left join cleansed.isu_zcd_tproctype_tx d
# MAGIC on a.ZCD_PROCESS_TYPE = d.PROCESS_TYPE --and f.LANGUE='E'
# MAGIC left join cleansed.isu_zcd_tsupprtyp_tx e
# MAGIC on a.ZCD_SUP_PROP_TYPE = e.superiorPropertyTypeCode
# MAGIC left join cleansed.isu_zcd_tinfprty_tx f
# MAGIC on a.ZCD_INF_PROP_TYPE = f.inferiorPropertyTypeCode)a where  a.rn = 1
