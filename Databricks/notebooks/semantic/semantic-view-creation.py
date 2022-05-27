# Databricks notebook source
# DBTITLE 1,Create Semantic schema if it doesn't exist
database_name = 'semantic'
query = "CREATE DATABASE IF NOT EXISTS {0}".format(database_name)
spark.sql(query)


# COMMAND ----------

# DBTITLE 1,Create views in Semantic layer for all dimensions, facts, bridge tables and views in Curated layer
import re
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").getOrCreate()

#List tables in curated layer
for table in spark.catalog.listTables("curated"):
    if table.name != '' and not table.name.startswith("vw") and table.name.startswith(("brg", "meter", "view", "dim", "fact")):
        table_name_seperated = ' '.join(re.sub( r"([A-Z])", r" \1", table.name).split())
        table_name_formatted = table_name_seperated[0:1].capitalize() + table_name_seperated[1:100]
        sql_statement = "create or replace view " + database_name + "." + table.name + " as select "
        #indexing the column
        curr_column = 0
        #list columns of the table selected
        for column in spark.catalog.listColumns(table.name, "curated"):
            #formatting column names ignoring metadata columns
            if not column.name.startswith("_"):
                curr_column = curr_column + 1
                if "SK" not in column.name:
                    col_name = column.name.replace("GUID","Guid").replace("ID","Id").replace("SCAMP","Scamp").replace("LGA", "Lga")
                    col_name_seperated = ' '.join(re.sub( r"([A-Z])", r" \1", col_name).split())
                    col_name_formatted = col_name_seperated[0:1].capitalize() + col_name_seperated[1:100].replace(" Guid"," GUID").replace(" Id"," ID")
                    col_name_formatted = col_name_formatted.replace("Scamp","SCAMP").replace("Lga","LGA")
                elif "SK" in column.name:
                    col_name_formatted = column.name
                if curr_column == 1:
                    sql_statement = sql_statement + " " + column.name + " as `" + col_name_formatted + "`"
                elif curr_column != 1:
                    sql_statement = sql_statement + " , " + column.name + " as `" + col_name_formatted + "`"
        sql_statement = sql_statement + "  from curated." + table.name + ";"
        #print(sql_statement)
        print(table.name)
        #executing the sql statement on spark creating the semantic view
        df = sqlContext.sql(sql_statement)

# COMMAND ----------

# DBTITLE 1,For Clean-up of views created in the environment
#for table in spark.catalog.listTables("semantic"):
#    if table.name.startswith("dim") or table.name.startswith("fact"):
#        print(table.name)
#        sqlContext.sql("DROP VIEW IF EXISTS "+database_name+"."+table.name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View: VW_DEVICE
# MAGIC -- Description: this view provide DAF data similar to C&B IM_DEVICE
# MAGIC -- History:     1.0 06/5/2022 LV created
# MAGIC --              1.1 10/5/2022 Added new columns
# MAGIC CREATE OR REPLACE VIEW semantic.VW_DEVICE as
# MAGIC select isu_0uc_deviceh_attr.equipmentNumber,
# MAGIC isu_0uc_deviceh_attr.validFromDate devh_validFromDate,
# MAGIC isu_0uc_deviceh_attr.validToDate devh_validToDate,
# MAGIC isu_0uc_device_attr.deviceNumber,
# MAGIC isu_0uc_deviceh_attr.logicalDeviceNumber,
# MAGIC isu_0uc_devinst_attr.installationId,
# MAGIC isu_0uc_devinst_attr.validFromDate devinst_validFromDate,
# MAGIC isu_0uc_devinst_attr.validToDate devinst_validToDate,
# MAGIC isu_0uc_regist_attr.registerNumber,
# MAGIC isu_0uc_regist_attr.validFromDate regist_validFromDate,
# MAGIC isu_0uc_regist_attr.validToDate regist_validToDate,
# MAGIC isu_0uc_regist_attr.logicalRegisterNumber,
# MAGIC isu_0uc_reginst_str_attr.validFromDate reginst_str_attr_validFromDate,
# MAGIC isu_0uc_reginst_str_attr.validToDate reginst_str_attr_validToDate,
# MAGIC isu_0ucinstalla_attr_2.divisionCode,
# MAGIC isu_0ucinstalla_attr_2.division,
# MAGIC isu_0uc_deviceh_attr.deviceLocation,
# MAGIC isu_0uc_device_attr.materialNumber,
# MAGIC isu_0uc_devcat_attr.functionClassCode,
# MAGIC isu_0uc_devcat_attr.functionClass,
# MAGIC isu_0uc_devcat_attr.constructionClassCode,
# MAGIC isu_0uc_devcat_attr.constructionClass,
# MAGIC isu_0uc_devcat_attr.deviceCategoryName,
# MAGIC isu_0uc_devcat_attr.deviceCategoryDescription,
# MAGIC isu_0uc_reginst_str_attr.operationCode,
# MAGIC isu_te405t.operationDescription,
# MAGIC isu_0uc_reginst_str_attr.rateTypeCode reginst_rateTypeCode,
# MAGIC isu_0uc_stattart_text.rateType reginst_rateType,
# MAGIC isu_0uc_reginst_str_attr.registerNotRelevantToBilling,
# MAGIC isu_0uc_reginst_str_attr.rateFactGroupCode,
# MAGIC isu_0uc_devinst_attr.priceClassCode,
# MAGIC isu_0uc_devinst_attr.priceClass,
# MAGIC isu_0uc_deviceh_attr.deviceCategoryCombination,
# MAGIC isu_0uc_deviceh_attr.registerGroupCode,
# MAGIC isu_0uc_deviceh_attr.registerGroup,
# MAGIC isu_0uc_deviceh_attr.installationDate,
# MAGIC min(isu_0uc_deviceh_attr.installationDate) over (partition by isu_0uc_deviceh_attr.equipmentNumber) firstInstallationDate,
# MAGIC isu_0uc_deviceh_attr.deviceRemovalDate,
# MAGIC case
# MAGIC when (isu_0uc_deviceh_attr.validFromDate <= current_date and isu_0uc_deviceh_attr.validToDate >= current_date) then isu_0uc_deviceh_attr.deviceRemovalDate
# MAGIC else null
# MAGIC END lastDeviceRemovalDate,
# MAGIC isu_0uc_deviceh_attr.activityReasonCode,
# MAGIC isu_0uc_deviceh_attr.activityReason,
# MAGIC isu_0uc_devinst_attr.rateTypeCode,
# MAGIC isu_0uc_devinst_attr.rateType,
# MAGIC isu_0uc_device_attr.inspectionRelevanceIndicator, 
# MAGIC isu_0uc_device_attr.deviceSize,
# MAGIC isu_0uc_device_attr.assetManufacturerName,
# MAGIC isu_0uc_device_attr.manufacturerSerialNumber,
# MAGIC isu_0uc_device_attr.manufacturerModelNumber,
# MAGIC isu_0uc_regist_attr.divisionCategoryCode,
# MAGIC isu_0uc_regist_attr.divisionCategory,
# MAGIC isu_0uc_regist_attr.registerIdCode,
# MAGIC isu_0uc_regist_attr.registerId,
# MAGIC isu_0uc_regist_attr.registerTypeCode,
# MAGIC isu_0uc_regist_attr.registerType,
# MAGIC isu_0uc_regist_attr.registerCategoryCode,
# MAGIC isu_0uc_regist_attr.registerCategory,
# MAGIC isu_0uc_regist_attr.reactiveApparentOrActiveRegister,
# MAGIC isu_dd07t.domainValueText reactiveApparentOrActiveRegisterTxt,
# MAGIC isu_0uc_regist_attr.unitOfMeasurementMeterReading,
# MAGIC isu_0uc_devcat_attr.ptiNumber,
# MAGIC isu_0uc_devcat_attr.ggwaNumber,
# MAGIC isu_0uc_devcat_attr.certificationRequirementType,
# MAGIC isu_0uc_regist_attr.doNotReadIndicator,
# MAGIC isu_0uc_device_attr.objectNumber,
# MAGIC case 
# MAGIC when (isu_0uc_deviceh_attr.validFromDate <= current_date and isu_0uc_deviceh_attr.validToDate >= current_date
# MAGIC and (isu_0uc_devinst_attr.validToDate is null or (isu_0uc_devinst_attr.validFromDate <= current_date and isu_0uc_devinst_attr.validToDate >= current_date))
# MAGIC and (isu_0uc_regist_attr.validToDate is null or (isu_0uc_regist_attr.validFromDate <= current_date and isu_0uc_regist_attr.validToDate >= current_date))
# MAGIC and (isu_0uc_reginst_str_attr.validToDate is null or (isu_0uc_reginst_str_attr.validFromDate <= current_date and isu_0uc_reginst_str_attr.validToDate >= current_date))
# MAGIC )  THEN 'Y' 
# MAGIC else 'N' 
# MAGIC END CUR_IND,
# MAGIC 'Y' CUR_REC_IND
# MAGIC from cleansed.isu_0uc_deviceh_attr
# MAGIC inner join cleansed.isu_0uc_device_attr
# MAGIC on isu_0uc_deviceh_attr.equipmentNumber = isu_0uc_device_attr.equipmentNumber
# MAGIC left outer join cleansed.isu_0uc_devinst_attr
# MAGIC on isu_0uc_deviceh_attr.logicalDeviceNumber = isu_0uc_devinst_attr.logicalDeviceNumber
# MAGIC and isu_0uc_deviceh_attr.validFromDate >= isu_0uc_devinst_attr.validFromDate
# MAGIC and isu_0uc_deviceh_attr.validFromDate <= isu_0uc_devinst_attr.validToDate
# MAGIC and isu_0uc_devinst_attr.`_RecordCurrent` = 1
# MAGIC and isu_0uc_devinst_attr.deletedIndicator is null
# MAGIC left outer join cleansed.isu_0uc_regist_attr
# MAGIC on isu_0uc_regist_attr.equipmentNumber = isu_0uc_deviceh_attr.equipmentNumber
# MAGIC and isu_0uc_deviceh_attr.validFromDate >= isu_0uc_regist_attr.validFromDate
# MAGIC and isu_0uc_deviceh_attr.validFromDate <= isu_0uc_regist_attr.validToDate
# MAGIC and isu_0uc_regist_attr.`_RecordCurrent` = 1
# MAGIC and isu_0uc_regist_attr.deletedIndicator is null
# MAGIC left outer join cleansed.isu_0uc_reginst_str_attr
# MAGIC on isu_0uc_reginst_str_attr.installationId = isu_0uc_devinst_attr.installationId
# MAGIC and isu_0uc_reginst_str_attr.logicalRegisterNumber = isu_0uc_regist_attr.logicalRegisterNumber
# MAGIC and isu_0uc_deviceh_attr.validFromDate >= isu_0uc_reginst_str_attr.validFromDate
# MAGIC and isu_0uc_deviceh_attr.validFromDate <= isu_0uc_reginst_str_attr.validToDate
# MAGIC and isu_0uc_reginst_str_attr._RecordCurrent = 1
# MAGIC and isu_0uc_reginst_str_attr.deletedIndicator is null
# MAGIC left outer join cleansed.isu_0ucinstalla_attr_2
# MAGIC on isu_0ucinstalla_attr_2.installationId = isu_0uc_devinst_attr.installationId
# MAGIC and isu_0ucinstalla_attr_2.`_RecordCurrent` = 1
# MAGIC and isu_0ucinstalla_attr_2.deletedIndicator is null
# MAGIC left outer join cleansed.isu_0uc_devcat_attr
# MAGIC on isu_0uc_device_attr.materialNumber = isu_0uc_devcat_attr.materialNumber
# MAGIC and isu_0uc_devcat_attr.`_RecordCurrent` = 1
# MAGIC left outer join cleansed.isu_te405t
# MAGIC on isu_0uc_reginst_str_attr.operationCode = isu_te405t.operationCode
# MAGIC and isu_te405t.`_RecordCurrent` = 1
# MAGIC left outer join cleansed.isu_0uc_stattart_text
# MAGIC on isu_0uc_regist_attr.registerTypeCode = isu_0uc_stattart_text.rateTypeCode
# MAGIC and isu_0uc_stattart_text.`_RecordCurrent` = 1
# MAGIC left outer join cleansed.isu_dd07t
# MAGIC on  isu_dd07t.domainName = 'BLIWIRK'
# MAGIC and isu_dd07t.domainValueSingleUpperLimit = isu_0uc_regist_attr.reactiveApparentOrActiveRegister
# MAGIC and isu_dd07t.`_RecordCurrent` = 1
# MAGIC where 
# MAGIC nvl(isu_0uc_reginst_str_attr.registerNotRelevantToBilling, ' ') <> 'X'
# MAGIC and isu_0uc_deviceh_attr.`_RecordCurrent` = 1
# MAGIC and isu_0uc_deviceh_attr.deletedIndicator is null
# MAGIC and isu_0uc_device_attr.`_RecordCurrent` = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View: VW_INSTALLATION
# MAGIC -- Description: this view provide DAF data similar to C&B VW_INSTALLATION
# MAGIC -- History:     1.0 11/5/2022 LV created
# MAGIC --  
# MAGIC CREATE OR REPLACE VIEW semantic.VW_INSTALLATION as
# MAGIC select isu_0ucinstalla_attr_2.installationId,
# MAGIC isu_0ucinstallah_attr_2.validFromDate,
# MAGIC isu_0ucinstallah_attr_2.validToDate,
# MAGIC isu_0ucinstalla_attr_2.divisionCode,
# MAGIC isu_0ucinstalla_attr_2.division,
# MAGIC isu_0ucinstalla_attr_2.propertyNumber,
# MAGIC isu_0ucinstalla_attr_2.premise,
# MAGIC isu_0ucinstalla_attr_2.reference,
# MAGIC isu_0ucinstalla_attr_2.serviceTypeCode,
# MAGIC isu_0ucinstalla_attr_2.serviceType,
# MAGIC isu_0ucinstallah_attr_2.rateCategoryCode,
# MAGIC isu_0ucinstallah_attr_2.rateCategory,
# MAGIC isu_0ucinstallah_attr_2.industryCode,
# MAGIC isu_0ucinstallah_attr_2.industry,
# MAGIC isu_0ucinstallah_attr_2.billingClassCode,
# MAGIC isu_0ucinstallah_attr_2.billingClass,
# MAGIC isu_0ucinstallah_attr_2.meterReadingUnit,
# MAGIC isu_0ucmtrdunit_attr.portionNumber,
# MAGIC isu_0ucinstallah_attr_2.industrySystemCode,
# MAGIC isu_0ucinstallah_attr_2.industrySystem,
# MAGIC isu_0ucinstalla_attr_2.authorizationGroupCode,
# MAGIC isu_0ucinstalla_attr_2.meterReadingControlCode,
# MAGIC isu_0ucinstalla_attr_2.meterReadingControl,
# MAGIC isu_0uc_isu_32.disconnectionDocumentNumber,
# MAGIC isu_0uc_isu_32.disconnectionActivityPeriod,
# MAGIC isu_0uc_isu_32.disconnectionObjectNumber,
# MAGIC isu_0uc_isu_32.disconnectiondate,
# MAGIC isu_0uc_isu_32.disconnectionActivityTypeCode,
# MAGIC isu_0uc_isu_32.disconnectionActivityType,
# MAGIC isu_0uc_isu_32.disconnectionObjectTypeCode,
# MAGIC isu_0uc_isu_32.disconnectionReasonCode,
# MAGIC isu_0uc_isu_32.disconnectionReason,
# MAGIC isu_0uc_isu_32.processingVariantCode,
# MAGIC isu_0uc_isu_32.processingVariant,
# MAGIC isu_0uc_isu_32.disconnectionReconnectionStatusCode,
# MAGIC isu_0uc_isu_32.disconnectionReconnectionStatus,
# MAGIC isu_0uc_isu_32.disconnectionDocumentStatusCode,
# MAGIC isu_0uc_isu_32.disconnectionDocumentStatus,
# MAGIC isu_0ucinstalla_attr_2.createdBy installa_createdBy,
# MAGIC isu_0ucinstalla_attr_2.createdDate installa_createdDate,
# MAGIC isu_0ucinstalla_attr_2.lastChangedBy installa_lastChangedBy,
# MAGIC isu_0ucinstalla_attr_2.lastChangedDate installa_lastChangedDate,
# MAGIC isu_0ucinstalla_attr_2.deletedIndicator installa_deletedIndicator,
# MAGIC isu_0ucinstallah_attr_2.deltaProcessRecordMode installah_deletedIndicator,
# MAGIC case when (isu_0ucinstallah_attr_2.validFromDate <= current_date and isu_0ucinstallah_attr_2.validToDate >=  current_date) then 'Y'
# MAGIC else 'N'
# MAGIC end CUR_IND,
# MAGIC 'Y' CUR_REC_IND 
# MAGIC from cleansed.isu_0ucinstalla_attr_2
# MAGIC inner join cleansed.isu_0ucinstallah_attr_2
# MAGIC on isu_0ucinstalla_attr_2.installationId = isu_0ucinstallah_attr_2.installationId
# MAGIC left outer join cleansed.isu_0uc_isu_32
# MAGIC on isu_0ucinstallah_attr_2.installationId = isu_0uc_isu_32.installationId
# MAGIC and isu_0ucinstallah_attr_2.validFromDate <= current_date and isu_0ucinstallah_attr_2.validToDate >= current_date
# MAGIC and isu_0uc_isu_32.referenceObjectTypeCode = 'INSTLN'
# MAGIC and isu_0uc_isu_32.validFromDate <= current_date and isu_0uc_isu_32.validToDate >= current_date
# MAGIC and isu_0uc_isu_32.`_RecordCurrent` = 1
# MAGIC left outer join cleansed.isu_0ucmtrdunit_attr
# MAGIC on isu_0ucinstallah_attr_2.meterReadingUnit = isu_0ucmtrdunit_attr.portion
# MAGIC and isu_0ucmtrdunit_attr.`_RecordCurrent` = 1
# MAGIC where
# MAGIC isu_0ucinstalla_attr_2.`_RecordCurrent` = 1
# MAGIC and isu_0ucinstallah_attr_2.`_RecordCurrent` = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View: VW_PROPERT_SERV
# MAGIC -- Description: this view provide snapshot of integrated model of property at run time.
# MAGIC --              strucure similar to IM_PROPERTY_SERV, but without historical data
# MAGIC -- History:     1.0 1/4/2022 LV created
# MAGIC CREATE OR REPLACE VIEW semantic.VW_PROPERTY_SERV As
# MAGIC select 
# MAGIC isu_vibdnode.architecturalObjectNumber PROPERTY_NUM,
# MAGIC isu_vibdcharact.architecturalObjectInternalId INTRE_NUM,
# MAGIC isu_vibdcharact.validToDate SERV_EFF_TO_DT,
# MAGIC isu_vibdcharact.validFromDate SERV_EFF_FROM_DT,
# MAGIC isu_vibdcharact.fixtureAndFittingCharacteristicCode SERV_CD,
# MAGIC isu_vibdcharact.fixtureAndFittingCharacteristic SERV_DSC,
# MAGIC isu_vibdcharact.supplementInfo SUPP_INFO_TXT,
# MAGIC -- CHG_DT,
# MAGIC CASE WHEN isu_vibdcharact.validFromDate <= current_date and isu_vibdcharact.validToDate >=  current_date THEN 'Y' ELSE 'N' END CUR_IND,
# MAGIC 'Y' CUR_REC_IND 
# MAGIC from cleansed.isu_vibdcharact isu_vibdcharact
# MAGIC inner join cleansed.isu_vibdnode isu_vibdnode 
# MAGIC on isu_vibdcharact.architecturalObjectInternalId = isu_vibdnode.architecturalObjectInternalId

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View: VW_PROPERTY
# MAGIC -- Description: this view provide snapshot of integrated model of property at run time.
# MAGIC --              strucure similar to IM_PROPERTY, but without property services
# MAGIC -- History:     1.0 28/3/2022 LV created
# MAGIC --              1.1 26/4/2022 filtered with `_RecordCurrent` = 1
# MAGIC CREATE OR REPLACE view semantic.VW_PROPERTY AS
# MAGIC SELECT isu_ehauisu.propertyNumber as PROPERTY_NUM,
# MAGIC isu_0ucpremise_attr_2.premise PREMISE_NUM,
# MAGIC isu_zcd_tpropty_hist.superiorPropertyTypeCode SUP_PROP_TYPE,
# MAGIC isu_zcd_tpropty_hist.superiorPropertyType SUP_PROP_TYPE_DSC, 
# MAGIC isu_zcd_tpropty_hist.inferiorPropertyTypeCode INF_PROP_TYPE,
# MAGIC isu_zcd_tpropty_hist.inferiorPropertyType INF_PROP_TYPE_DSC,
# MAGIC isu_zcd_tpropty_hist.validFromDate PROP_TYPE_FROM_DT,
# MAGIC isu_zcd_tpropty_hist.validToDate PROP_TYPE_TO_DT,
# MAGIC isu_vibdnode.architecturalObjectInternalId INTRE_NUM,
# MAGIC isu_vibdnode.architecturalObjectNumber AOBJ_NUM,
# MAGIC isu_vibdnode.architecturalObjectTypeCode AOBJ_TYPE_CD,
# MAGIC isu_vibdnode.architecturalObjectType AOBJ_TYPE_DSC,
# MAGIC isu_vibdnode.parentArchitecturalObjectNumber PARENT_AOBJ_NUM,
# MAGIC isu_vibdnode.parentArchitecturalObjectTypeCode PARENT_AOBJ_TYPE_CD,
# MAGIC isu_vibdnode.parentArchitecturalObjectType PARENT_AOBJ_TYPE_DSC,
# MAGIC isu_vibdnode.parentArchitecturalObjectNumber    PARENT_PROPERTY_NUM,
# MAGIC isu_ehauisu.CRMConnectionObjectGUID CONN_CRM_GUID,
# MAGIC isu_0funct_loc_attr.locationDescription LOCATION_NM,
# MAGIC isu_0funct_loc_attr.buildingNumber BUILDING_CD,
# MAGIC isu_0funct_loc_attr.floorNumber LEVEL_NUM,
# MAGIC isu_0funct_loc_attr.houseNumber3 HOUSE_LOT_NUM,
# MAGIC isu_0funct_loc_attr.houseNumber2 HOUSE_SUPP_NUM,
# MAGIC isu_0funct_loc_attr.houseNumber1 HOUSE_NUM,
# MAGIC isu_0funct_loc_attr.streetName STREET_NM,
# MAGIC isu_0funct_loc_attr.streetLine1 STREET_SUPPL_NM,
# MAGIC isu_0funct_loc_attr.streetLine2 STREET_SUPPL2_NM,
# MAGIC isu_0funct_loc_attr.cityName CITY_NM,
# MAGIC isu_0uc_connobj_attr_2.streetCode STREET_CD,
# MAGIC isu_0uc_connobj_attr_2.cityCode CITY_CD,
# MAGIC isu_0funct_loc_attr.postcode POST_CD,
# MAGIC isu_0funct_loc_attr.stateCode REGION_CD,
# MAGIC isu_0uc_connobj_attr_2.politicalRegionCode LGA_CD,
# MAGIC isu_0uc_connobj_attr_2.LGA LGA_NM,
# MAGIC isu_0uc_connobj_attr_2.planTypeCode PLAN_TYPE_CD,
# MAGIC isu_0uc_connobj_attr_2.planType PLAN_TYP_DSC,
# MAGIC isu_0uc_connobj_attr_2.processTypeCode PROCESS_TYPE_CD,
# MAGIC isu_0uc_connobj_attr_2.processType PROCESS_TYPE_DSC,
# MAGIC isu_0uc_connobj_attr_2.planNumber PLAN_NUM,
# MAGIC isu_0uc_connobj_attr_2.lotTypeCode LOT_TYPE_CD,
# MAGIC -- LOT_TYPE_DSC,   -- missing in cleansed.isu_0uc_connobj_attr_2
# MAGIC isu_0uc_connobj_attr_2.lotNumber PLAN_LOT_NUM,
# MAGIC isu_0uc_connobj_attr_2.sectionNumber SECTION_NUM,
# MAGIC isu_0uc_connobj_attr_2.serviceAgreementIndicator SRV_AGR_IND,
# MAGIC isu_0uc_connobj_attr_2.mlimIndicator MLIM_IND,
# MAGIC isu_0uc_connobj_attr_2.wicaIndicator WICA_IND,
# MAGIC isu_0uc_connobj_attr_2.sopaIndicator SOPA_IND,
# MAGIC isu_0uc_connobj_attr_2.buildingFeeDate BLD_FEE_DT,
# MAGIC isu_0uc_connobj_attr_2.objectNumber OBJ_NUM,
# MAGIC isu_vibdao.hydraCalculatedArea HYDRA_CALC_AREA,
# MAGIC isu_vibdao.hydraAreaUnit HYDRA_AREA_UNIT,
# MAGIC isu_vibdao.propertyInfo PROPERTY_INFO,
# MAGIC isu_vibdao.stormWaterAssessmentIndicator HYDRA_STORMW_ASSESS_IND,
# MAGIC isu_vibdao.hydraAreaIndicator HYDRA_UPD_IND,
# MAGIC -- HYDRA_BAND,   -- missing in cleansed.isu_vibdao
# MAGIC isu_vibdao.overrideArea HYDRA_OVERRIDE_AREA,
# MAGIC isu_vibdao.overrideAreaUnit HYDRA_OVERRIDE_AREA_UNIT,
# MAGIC isu_vibdao.comments HYDRA_COMMENTS_TXT,
# MAGIC CASE WHEN isu_zcd_tpropty_hist.validFromDate <= current_date and isu_zcd_tpropty_hist.validToDate >=  current_date THEN 'Y' ELSE 'N' END CUR_IND,
# MAGIC 'Y' CUR_REC_IND
# MAGIC FROM cleansed.isu_vibdnode isu_vibdnode
# MAGIC INNER JOIN cleansed.isu_ehauisu
# MAGIC ON isu_vibdnode.architecturalObjectNumber = isu_ehauisu.propertyNumber
# MAGIC INNER JOIN cleansed.isu_0funct_loc_attr isu_0funct_loc_attr
# MAGIC ON isu_vibdnode.architecturalObjectNumber = isu_0funct_loc_attr.functionalLocationNumber
# MAGIC INNER JOIN cleansed.isu_0ucpremise_attr_2 isu_0ucpremise_attr_2
# MAGIC ON isu_vibdnode.architecturalObjectNumber = isu_0ucpremise_attr_2.propertyNumber
# MAGIC INNER JOIN cleansed.isu_zcd_tpropty_hist
# MAGIC ON isu_vibdnode.architecturalObjectNumber = isu_zcd_tpropty_hist.propertyNumber
# MAGIC INNER JOIN cleansed.isu_0uc_connobj_attr_2
# MAGIC ON isu_vibdnode.architecturalObjectNumber = isu_0uc_connobj_attr_2.propertyNumber
# MAGIC INNER JOIN cleansed.isu_vibdao
# MAGIC ON isu_vibdnode.architecturalObjectInternalId = isu_vibdao.architecturalObjectInternalId
# MAGIC WHERE
# MAGIC isu_vibdnode.`_RecordCurrent` = 1 and
# MAGIC isu_ehauisu.`_RecordCurrent` = 1 and
# MAGIC isu_0funct_loc_attr.`_RecordCurrent` = 1 and
# MAGIC isu_0funct_loc_attr.`_RecordCurrent` = 1 and
# MAGIC isu_0ucpremise_attr_2.`_RecordCurrent` = 1 and
# MAGIC isu_zcd_tpropty_hist.`_RecordCurrent` = 1 and
# MAGIC isu_0uc_connobj_attr_2.`_RecordCurrent` = 1 and
# MAGIC isu_vibdao.`_RecordCurrent` = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View: VW_PROPERT_REL
# MAGIC -- Description: this view provide snapshot of integrated model of property relationship at run time.
# MAGIC --              strucure similar to IM_PROPERTY_REL, but without historical data
# MAGIC -- History:     1.0 4/4/2022 LV created
# MAGIC CREATE OR REPLACE VIEW semantic.VW_PROPERTY_REL as
# MAGIC select isu_zcd_tprop_rel.property1Number PROPERTY1_NUM,
# MAGIC isu_zcd_tprop_rel.property2Number PROPERTY2_NUM,
# MAGIC isu_zcd_tprop_rel.validFromDate EFF_FROM_DT,
# MAGIC isu_zcd_tprop_rel.validToDate EFF_TO_DT,
# MAGIC isu_zcd_tprop_rel.relationshipTypeCode1  P1P2_REL_TYPE_CD,
# MAGIC isu_zcd_tprop_rel.relationshipType1 P1P2_REL_TYPE_DSC,
# MAGIC isu_zcd_tprop_rel.relationshipTypeCode2 P2P1_REL_TYPE_CD,
# MAGIC isu_zcd_tprop_rel.relationshipType2 P2P1_REL_TYPE_DSC,
# MAGIC -- CHG_DT -- not available in 
# MAGIC CASE WHEN isu_zcd_tprop_rel.validFromDate <= current_date and isu_zcd_tprop_rel.validToDate >=  current_date THEN 'Y' ELSE 'N' END CUR_IND,
# MAGIC 'Y' CUR_REC_IND
# MAGIC from cleansed.isu_zcd_tprop_rel
# MAGIC where isu_zcd_tprop_rel.`_RecordCurrent` = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View: VW_QQV
# MAGIC -- Description: this view provide DAF data similar to QQV extractor in C&B
# MAGIC -- History:     1.0 24/4/2022 LV created
# MAGIC --              1.1 10/05/2022 LV added erchc, updated renamed columns
# MAGIC CREATE OR REPLACE VIEW semantic.VW_QQV as
# MAGIC with statBilling as
# MAGIC (SELECT factdailyapportionedconsumption.meterConsumptionBillingDocumentSK, 
# MAGIC factdailyapportionedconsumption.PropertySK,
# MAGIC factdailyapportionedconsumption.LocationSK,
# MAGIC factdailyapportionedconsumption.BusinessPartnerGroupSK,
# MAGIC factdailyapportionedconsumption.ContractSK,
# MAGIC factdailyapportionedconsumption.MeterSK,
# MAGIC factdailyapportionedconsumption.sourceSystemCode,
# MAGIC trunc(consumptionDate, 'MM') FIRSTDAYOFMONTHDATE,
# MAGIC last_day(consumptionDate) LASTDAYOFMONTHDATE,
# MAGIC min(factdailyapportionedconsumption.consumptionDate) DEVMONTHSTARTDATE,
# MAGIC max(factdailyapportionedconsumption.consumptionDate) DEVMONTHENDDATE,
# MAGIC datediff(max(factdailyapportionedconsumption.consumptionDate), min(factdailyapportionedconsumption.consumptionDate)) + 1 TOTALDEVACTDAYSPERMONTH,
# MAGIC sum(factdailyapportionedconsumption.dailyApportionedConsumption) AVGCONSPERMONPERDEV
# MAGIC FROM curated.factdailyapportionedconsumption
# MAGIC WHERE 
# MAGIC factdailyapportionedconsumption.`_RecordCurrent` = 1
# MAGIC GROUP BY factdailyapportionedconsumption.meterconsumptionbillingdocumentSK, 
# MAGIC factdailyapportionedconsumption.PropertySK,
# MAGIC factdailyapportionedconsumption.LocationSK,
# MAGIC factdailyapportionedconsumption.BusinessPartnerGroupSK,
# MAGIC factdailyapportionedconsumption.ContractSK,
# MAGIC factdailyapportionedconsumption.MeterSK,
# MAGIC factdailyapportionedconsumption.sourceSystemCode,
# MAGIC trunc(factdailyapportionedconsumption.consumptionDate, 'MM'),
# MAGIC last_day(factdailyapportionedconsumption.consumptionDate)
# MAGIC )
# MAGIC select 
# MAGIC dimmeterconsumptionbillingdocument.meterConsumptionBillingDocumentSK,
# MAGIC dimmeterconsumptionbillingdocument.billingDocumentNumber, 
# MAGIC dimmeterconsumptionbillingdocument.invoiceMaxSequenceNumber,
# MAGIC statBilling.sourceSystemCode,
# MAGIC dimmeter.meterNumber,
# MAGIC dimmeter.meterSerialNumber,
# MAGIC dimmeter.materialNumber,
# MAGIC dimmeter.meterSize,
# MAGIC isu_0uc_devcat_attr.functionClassCode,
# MAGIC isu_0uc_devcat_attr.functionClass,
# MAGIC dimmeterconsumptionbillingdocument.billingPeriodStartDate,
# MAGIC dimmeterconsumptionbillingdocument.billingPeriodEndDate,
# MAGIC dimmeterconsumptionbillingdocument.billCreatedDate,
# MAGIC dimmeterconsumptionbillingdocument.lastChangedDate, 
# MAGIC dimmeterconsumptionbillingdocument.invoicePostingDate,
# MAGIC datediff(dimmeterconsumptionbillingdocument.billingPeriodEndDate, dimmeterconsumptionbillingdocument.billingPeriodStartDate) + 1 TOTALBILLEDDAYS,
# MAGIC dimproperty.propertyNumber,
# MAGIC isu_ehauisu.politicalRegionCode,
# MAGIC dimlocation.LGA,
# MAGIC dimbusinesspartnergroup.businessPartnerGroupNumber,
# MAGIC statBilling.FIRSTDAYOFMONTHDATE,
# MAGIC statBilling.LASTDAYOFMONTHDATE,
# MAGIC  statBilling.DEVMONTHSTARTDATE,
# MAGIC  statBilling.DEVMONTHENDDATE,
# MAGIC  statBilling.TOTALDEVACTDAYSPERMONTH,
# MAGIC statBilling.AVGCONSPERMONPERDEV,
# MAGIC dimmeterconsumptionbillingdocument.isOutSortedFlag,
# MAGIC dimmeterconsumptionbillingdocument.invoiceNotReleasedIndicator,
# MAGIC dimmeterconsumptionbillingdocument.isreversedFlag,
# MAGIC dimmeterconsumptionbillingdocument.reversalDate,
# MAGIC dimmeterconsumptionbillingdocument.invoiceReversalPostingDate,
# MAGIC dimmeterconsumptionbillingdocument.erchcExistIndicator,
# MAGIC dimmeterconsumptionbillingdocument.billingDocumentWithoutInvoicingCode,
# MAGIC dimmeterconsumptionbillingdocument.portionNumber,
# MAGIC dimmeterconsumptionbillingdocument.billingReasonCode,
# MAGIC dimProperty.superiorPropertyTypeCode,
# MAGIC dimProperty.superiorPropertyType,
# MAGIC dimProperty.PropertyTypeCode,
# MAGIC dimProperty.PropertyType,
# MAGIC dimContract.contractId,
# MAGIC dimcontract.contractAccountNumber,
# MAGIC diminstallation.installationId,
# MAGIC diminstallation.divisionCode,
# MAGIC diminstallation.division
# MAGIC from statBilling  
# MAGIC inner join curated.dimmeterconsumptionbillingdocument
# MAGIC on statBilling.meterconsumptionbillingdocumentSK = dimmeterconsumptionbillingdocument.meterconsumptionbillingdocumentSK
# MAGIC inner join curated.dimproperty
# MAGIC on dimproperty.PropertySK = statBilling.PropertySK
# MAGIC inner join curated.dimlocation
# MAGIC on dimlocation.LocationSK = statBilling.LocationSK
# MAGIC inner join curated.dimbusinesspartnergroup
# MAGIC on dimbusinesspartnergroup.BusinessPartnerGroupSK = statBilling.BusinessPartnerGroupSK
# MAGIC inner join curated.dimcontract
# MAGIC on dimContract.ContractSK = statBilling.ContractSK
# MAGIC inner join cleansed.isu_ehauisu
# MAGIC on dimproperty.propertyNumber = isu_ehauisu.propertyNumber
# MAGIC inner join curated.dimmeter
# MAGIC on dimmeter.MeterSK = statBilling.MeterSK
# MAGIC inner join cleansed.isu_0uc_devcat_attr
# MAGIC on dimmeter.materialNumber = isu_0uc_devcat_attr.materialNumber
# MAGIC inner join curated.brginstallationpropertymetercontract
# MAGIC on brginstallationpropertymetercontract.MeterSK = statBilling.MeterSK
# MAGIC inner join curated.diminstallation
# MAGIC on diminstallation.InstallationSK = brginstallationpropertymetercontract.InstallationSK
# MAGIC where dimmeterconsumptionbillingdocument.`_RecordCurrent` = 1
# MAGIC and dimproperty.`_RecordCurrent` = 1
# MAGIC and dimlocation.`_RecordCurrent` = 1
# MAGIC and dimbusinesspartnergroup.`_RecordCurrent` = 1
# MAGIC and isu_ehauisu.`_RecordCurrent` = 1
# MAGIC and dimmeter.`_RecordCurrent` = 1
# MAGIC and isu_0uc_devcat_attr.`_RecordCurrent` = 1
# MAGIC and brginstallationpropertymetercontract.`_RecordCurrent` = 1
# MAGIC and diminstallation.`_RecordCurrent` = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View: VW_ZBL
# MAGIC -- Description: this view provide DAF data similar to ZBL extractor in C&B
# MAGIC -- History:     1.0 09/5/2022 LV created
# MAGIC CREATE OR REPLACE VIEW semantic.VW_ZBL as
# MAGIC with ercho as
# MAGIC (
# MAGIC select isu_ercho.*,
# MAGIC      rank() over (partition by isu_ercho.billingDocumentNumber order by isu_ercho.outsortingNumber desc) rankNumber
# MAGIC from cleansed.isu_ercho
# MAGIC where isu_ercho.`_RecordCurrent` = 1
# MAGIC ),
# MAGIC erchc as
# MAGIC (
# MAGIC select isu_erchc.*,
# MAGIC rank() over (partition by isu_erchc.billingDocumentNumber order by sequenceNumber desc) rankNumber
# MAGIC from cleansed.isu_erchc
# MAGIC where isu_erchc.`_RecordCurrent` = 1
# MAGIC )
# MAGIC select 
# MAGIC isu_erch.billingDocumentNumber, -- BELNR,
# MAGIC isu_dberchz2.billingDocumentLineItemId, -- BELZEILE,
# MAGIC nvl(ercho.outsortingNumber, 0) outsortingNumber, -- OUTCNSO,
# MAGIC nvl(erchc.sequenceNumber, 0) sequenceNumber,  -- LFDNR
# MAGIC isu_erch.businessPartnerGroupNumber, --  GPARTNER,
# MAGIC isu_erch.contractId,  -- VERTRAG,
# MAGIC isu_erch.portionNumber,  -- PORTION,
# MAGIC isu_erch.startBillingPeriod,  -- BEGABRPE,
# MAGIC isu_erch.endBillingPeriod,  -- ENDABRPE,
# MAGIC isu_erch.contractAccountNumber,  -- VKONT,
# MAGIC isu_erch.reversalDate,  -- STORNODAT,
# MAGIC erchc.invoiceReversalPostingDate INTBUDAT,
# MAGIC isu_erch.divisionCode,  -- SPARTE,
# MAGIC isu_erch.lastChangedDate,  -- AEDAT,
# MAGIC isu_erch.createdDate,  -- ERDAT,
# MAGIC ercho.documentReleasedDate,  -- FREI_AM,
# MAGIC isu_erch.documentNotReleasedIndicator,   -- TOBRELEASD , -- new
# MAGIC erchc.postingDate,  -- BUDAT,
# MAGIC isu_erch.meterReadingUnit,  -- ABLEINH,
# MAGIC isu_erch.billingDocumentCreateDate,  -- BELEGDAT,
# MAGIC isu_erch.erchcExistIndicator,  -- ERCHC_V, 
# MAGIC isu_erch.billingDocumentWithoutInvoicingCode,  -- NINVOICE,
# MAGIC erchc.documentNotReleasedIndicator invoiceNotReleasedIndicator,  -- INV_TOBRELEASD
# MAGIC isu_dberchz1.billingQuantityPlaceBeforeDecimalPoint,  -- ABRMENGE,
# MAGIC isu_dberchz1.lineItemTypeCode,  -- BELZART,
# MAGIC isu_dberchz1.billingLineItemBudgetBillingIndicator,  -- ABSLKZ,
# MAGIC isu_dberchz1.subtransactionForDocumentItem,  -- TVORG,
# MAGIC isu_dberchz1.industryCode,  -- BRANCHE,
# MAGIC isu_dberchz1.billingClassCode,  -- AKLASSE,
# MAGIC isu_dberchz1.validFromDate,  -- AB,
# MAGIC isu_dberchz1.validToDate,  -- BIS,
# MAGIC isu_dberchz1.rateTypeCode,  -- TARIFTYP,
# MAGIC isu_dberchz1.rateId,  -- TARIFNR,
# MAGIC isu_dberchz1.statisticalAnalysisRateType,  -- STATTART,
# MAGIC isu_dberchz2.equipmentNumber,  -- EQUNR,
# MAGIC isu_dberchz2.deviceNumber,  -- GERAET ,
# MAGIC isu_dberchz2.materialNumber,  -- MATNR ,
# MAGIC isu_dberchz2.logicalRegisterNumber,  -- LOGIKZW,
# MAGIC isu_dberchz2.registerNumber,  -- ZWNUMMER,
# MAGIC isu_dberchz2.suppressedMeterReadingDocumentId,  -- ABLBELNR,
# MAGIC isu_dberchz2.meterReadingReasonCode,  -- ABLESGR,
# MAGIC isu_dberchz2.previousMeterReadingReasonCode,  -- ABLESGRV,
# MAGIC isu_dberchz2.meterReadingTypeCode,  -- ISTABLART,
# MAGIC isu_dberchz2.previousMeterReadingTypeCode,  -- ISTABLARTVA,
# MAGIC isu_dberchz2.maxMeterReadingDate,  -- ADATMAX,
# MAGIC isu_dberchz2.maxMeterReadingTime,  -- ATIMMAX,
# MAGIC isu_dberchz2.billingMeterReadingTime,  -- ATIM,
# MAGIC isu_dberchz2.previousMeterReadingTime,  -- ATIMVA,
# MAGIC isu_dberchz2.meterReadingResultsSimulationIndicator,  -- EXTPKZ,
# MAGIC isu_dberchz2.registerRelationshipConsecutiveNumber,  -- INDEXNR,
# MAGIC isu_dberchz2.meterReadingAllocationDate,  -- ZUORDDAT,
# MAGIC isu_dberchz2.registerRelationshipSortHelpCode ,  -- REGRELSORT
# MAGIC isu_dberchz2.logicalDeviceNumber,  -- LOGIKNR,
# MAGIC isu_dberchz2.meterReaderNoteText,  -- ABLHINW,
# MAGIC isu_dberchz2.quantityDeterminationProcedureCode, --QDPROC,  
# MAGIC isu_dberchz2.meterReadingDocumentId,  -- MRCONNECT,  
# MAGIC isu_dberchz2.previousMeterReadingBeforeDecimalPlaces,  -- V_ZWSTVOR,
# MAGIC isu_dberchz2.meterReadingBeforeDecimalPoint,  -- V_ZWSTAND,
# MAGIC isu_dberchz2.meterReadingDifferenceBeforeDecimalPlaces  -- V_ZWSTDIFF
# MAGIC --select sum(isu_dberchz1.billingQuantityPlaceBeforeDecimalPoint) dumx
# MAGIC from cleansed.isu_erch
# MAGIC inner join cleansed.isu_dberchz1 
# MAGIC on isu_erch.billingDocumentNumber = isu_dberchz1.billingDocumentNumber
# MAGIC inner join cleansed.isu_dberchz2 
# MAGIC on isu_dberchz1.billingDocumentNumber = isu_dberchz2.billingDocumentNumber
# MAGIC and isu_dberchz1.billingDocumentLineItemId = isu_dberchz2.billingDocumentLineItemId
# MAGIC left outer join ercho
# MAGIC on isu_erch.billingDocumentNumber = ercho.billingDocumentNumber
# MAGIC and ercho.rankNumber = 1
# MAGIC left outer join erchc
# MAGIC on isu_erch.billingDocumentNumber = erchc.billingDocumentNumber
# MAGIC and erchc.rankNumber = 1
# MAGIC where 
# MAGIC isu_erch.billingSimulationIndicator = ' '
# MAGIC and isu_erch.documentNotReleasedIndicator = ' '
# MAGIC and isu_dberchz1.lineItemTypeCode in ('ZDQUAN', 'ZRQUAN')
# MAGIC and isu_dberchz2.suppressedMeterReadingDocumentId <> ' '
# MAGIC and isu_erch.`_RecordCurrent` = 1
# MAGIC and isu_dberchz1.`_RecordCurrent` = 1
# MAGIC and isu_dberchz2.`_RecordCurrent` = 1
