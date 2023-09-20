# Databricks notebook source
# MAGIC %run ../common-controldb

# COMMAND ----------

labwaredata_tables =['ACCOUNT'
,'BATCH'
,'BATCH_RESULT'
,'CONTRACT_QUOTE'
,'CONTRACT_QUOTE_ITM'
,'DB_FILES'
,'EXPERIMENT'
,'INVENTORY_ITEM'
,'INVENTORY_LOCATION'
,'INVOICE'
,'INVOICE_ADJUSTMENT'
,'INVOICE_ITEM'
,'LIMS_NOTES'
,'PERSON'
,'PERSON_LINKING'
,'PROJECT'
,'PROJECT_RESULT'
,'REPORT_OBJECTS'
,'REPORTED_RESULT'
,'REPORTS'
,'RESULT'
,'RESULT_SPEC'
,'SAMPLE'
,'SWC_QUOTE_REVENUE'
,'SWC_SAP_BTCH_CUS_HDR'
,'SWC_SAP_BTCH_HDR'
,'TEST'
,'WORKBOOK'
,'WORKBOOK_FILES'
]

labwareref_tables = ['ADDRESS_BOOK'
,'ALGAL_MNEMONICS'
,'ANALYSIS'
,'ANALYSIS_LIMITS'
,'ANALYSIS_TYPES'
,'ANALYSIS_VARIATION'
,'APPROVAL'
,'APPROVAL_DETAILS'
,'BATCH_COMPONENT'
,'BATCH_HDR_TEMPLATE'
,'BATCH_LINK'
,'BATCH_STANDARD'
,'C_CUSTOMER_GROUP'
,'C_ENV_CONST_PARAM'
,'C_ENV_CONSTANTS'
,'CATALOGUE'
,'CATALOGUE_ITEM'
,'CHARGE_CODE'
,'CHARGES'
,'COLLECTION_RUN'
,'COMMON_NAME'
,'COMP_VARIATION'
,'COMPONENT'
,'COMPONENT_CODES'
,'CONDITION'
,'CONTACT'
,'CONTAINER'
,'CONTAINER_LIST_ENTRY'
,'CONTRACT_CURRENCY'
,'COST_ITEM'
,'COST_ITEM_RPT'
,'COUNTRY'
,'CUSTOMER'
,'HAZARD'
,'HOLIDAY_SCHED_ENTRY'
,'HTML_FILES'
,'INSTRUMENTS'
,'LIMS_CONSTANTS'
,'LIST'
,'LIST_ENTRY'
,'LOCATION'
,'PROD_GRADE_STAGE'
,'PRODUCT'
,'PRODUCT_GRADE'
,'PRODUCT_SPEC'
,'QC_SAMPLES'
,'SAMPLING_POINT'
,'SERVICE_RESERVOIR'
,'STANDARD_REAGENT'
,'STOCK'
,'SUPPLIER'
,'SWC_611SITES'
,'SWC_ABC_CODE'
,'SWC_ANALYTE_MAP'
,'SWC_COMP_TAUKEY'
,'SWC_HAZARD_NAME'
,'SWC_REPORT_TEMPLATE'
,'SWC_SP_HAZARDS'
,'SWC_TWAG'
,'SWC_X_ACCRED_USERS'
,'T_DISTRIBUTION_ITEM'
,'T_DISTRIBUTION_LIST'
,'T_UNCERTAINTY'
,'TAX_GROUP'
,'TAX_PLAN'
,'TAX_PLAN_ITEM'
,'TEST_LIST'
,'TEST_LIST_COMP'
,'TEST_LIST_ENTRY'
,'TREATMENT_WORKS'
,'UNITS'
,'VENDOR'
,'VERSIONS'
,'WATER_SOURCE'
,'WORKBOOK_IMAGE'
,'X_ERESULTS_TRANS_DET'
,'X_ERESULTS_TRANSLATE'
,'ZONE'
]

# COMMAND ----------

group_names = {"labwaredata":labwaredata_tables
              ,"labwareref":labwareref_tables
              } 
source_schema = "lims"
source_key_vault_secret = "daf-oracle-labware-connectionstring"
source_handler = 'oracle-load'
raw_handler = 'raw-load-delta'
cleansed_handler = 'cleansed-load-delta'

# COMMAND ----------

# ------------- CONSTRUCT QUERY ----------------- #
for code in group_names:
    base_query = f"""
    WITH _Base AS 
    (
    SELECT "{code}" SystemCode, "{source_schema}" SourceSchema, "{source_key_vault_secret}" SourceKeyVaultSecret, '' SourceQuery, "{source_handler}" SourceHandler, '' RawFileExtension, "{raw_handler}" RawHandler, '' ExtendedProperties, "{cleansed_handler}" CleansedHandler, '' WatermarkColumn
    )"""
    select_list = [
        'SELECT "' + x + '" SourceTableName, * FROM _Base' for x in group_names[code]
    ]
    select_string = " UNION ".join(select_list)
    select_statement = select_string + " ORDER BY SourceSchema, SourceTableName"

    df = spark.sql(f"""{base_query} {select_statement}""")
    SYSTEM_CODE = code

    AddIngestion(df)

# COMMAND ----------

 #ADD BUSINESS KEY
ExecuteStatement("""
update dbo.extractLoadManifest set
businessKeyColumn = case sourceTableName
when 'ACCOUNT' then 'accountNumber'
when 'ADDRESS_BOOK' then 'name'
when 'ALGAL_MNEMONICS' then 'name'
when 'ANALYSIS' then 'analysisName,version'
when 'ANALYSIS_LIMITS' then 'limitNumber,analysis,version'
when 'ANALYSIS_TYPES' then 'name'
when 'ANALYSIS_VARIATION' then 'analysis,variation,version'
when 'APPROVAL' then 'approvalId'
when 'APPROVAL_DETAILS' then 'approvalId,approvalStep'
when 'BATCH' then 'name'
when 'BATCH_COMPONENT' then 'template,name,version'
when 'BATCH_HDR_TEMPLATE' then 'name,version'
when 'BATCH_LINK' then 'name'
when 'BATCH_RESULT' then 'resultNumber'
when 'BATCH_STANDARD' then 'batchProtocol,orderNumber'
when 'CATALOGUE' then 'name,version'
when 'CATALOGUE_ITEM' then 'catalogue,version,costItemNo'
when 'CHARGES' then 'chargeEntry'
when 'CHARGE_CODE' then 'name'
when 'COLLECTION_RUN' then 'name'
when 'COMMON_NAME' then 'name'
when 'COMPONENT' then 'analysis,name,version'
when 'COMPONENT_CODES' then 'analysis,component,version,code'
when 'COMP_VARIATION' then 'analysis,variation,component,version'
when 'CONDITION' then 'name'
when 'CONTACT' then 'contactNumber'
when 'CONTAINER' then 'name'
when 'CONTAINER_LIST_ENTRY' then 'containerList,entryName,orderNumber'
when 'CONTRACT_CURRENCY' then 'name'
when 'CONTRACT_QUOTE' then 'contractQuoteNo'
when 'CONTRACT_QUOTE_ITM' then 'contractQuoteNo,cntrctQteItemNo'
when 'COST_ITEM' then 'costItemNo'
when 'COST_ITEM_RPT' then 'costItemRptNo'
when 'COUNTRY' then 'name'
when 'CUSTOMER' then 'name'
when 'C_CUSTOMER_GROUP' then 'name'
when 'C_ENV_CONSTANTS' then 'name'
when 'C_ENV_CONST_PARAM' then 'cEnvConstants,seqNum'
when 'DB_FILES' then 'fileName'
when 'EXPERIMENT' then 'name,experimentNumber'
when 'HAZARD' then 'name'
when 'HOLIDAY_SCHED_ENTRY' then 'name,month,year'
when 'HTML_FILES' then 'name'
when 'INSTRUMENTS' then 'name'
when 'INVENTORY_ITEM' then 'itemNumber'
when 'INVENTORY_LOCATION' then 'name'
when 'INVOICE' then 'invoiceNumber'
when 'INVOICE_ADJUSTMENT' then 'invoiceNumber,invoiceAdjustNo'
when 'INVOICE_ITEM' then 'invoiceItemNo'
when 'LIMS_CONSTANTS' then 'name'
when 'LIMS_NOTES' then 'noteId'
when 'LIST' then 'name'
when 'LIST_ENTRY' then 'list,name'
when 'LOCATION' then 'name'
when 'PERSON' then 'personNumber'
when 'PERSON_LINKING' then 'counter,linkNumber'
when 'PRODUCT' then 'name,version'
when 'PRODUCT_GRADE' then 'product,version,samplingPoint,grade'
when 'PRODUCT_SPEC' then 'product,entryCode'
when 'PROD_GRADE_STAGE' then 'product,version,samplingPoint,grade,stage,analysis,specType'
when 'PROJECT' then 'name'
when 'PROJECT_RESULT' then 'resultNumber'
when 'QC_SAMPLES' then 'name'
when 'REPORT_OBJECTS' then 'reportNumber,objectId'
when 'REPORTED_RESULT' then 'resultNumber,guId,revisionNo'
when 'REPORTS' then 'reportNumber'
when 'RESULT' then 'resultNumber'
when 'RESULT_SPEC' then 'resultNumber,productSpecCode'
when 'SAMPLE' then 'sampleNumber'
when 'SAMPLING_POINT' then 'name'
when 'SERVICE_RESERVOIR' then 'name'
when 'STANDARD_REAGENT' then 'name'
when 'STOCK' then 'name'
when 'SUPPLIER' then 'name'
when 'SWC_611SITES' then 'name'
when 'SWC_ABC_CODE' then 'name'
when 'SWC_ANALYTE_MAP' then 'name'
when 'SWC_COMP_TAUKEY' then 'name'
when 'SWC_HAZARD_NAME' then 'name'
when 'SWC_QUOTE_REVENUE' then 'contractQuoteNo,seqNum'
when 'SWC_REPORT_TEMPLATE' then 'name,changedOn'
when 'SWC_SAP_BTCH_CUS_HDR' then 'sapBtchHdrNum,sapBtchCusHdrNum'
when 'SWC_SAP_BTCH_HDR' then 'sapBtchHdrNum'
when 'SWC_SP_HAZARDS' then 'samplingPoint,orderNumber'
when 'SWC_TWAG' then 'name,changedOn'
when 'SWC_X_ACCRED_USERS' then 'lab,accreditedUser,accreditedTest'
when 'TAX_GROUP' then 'name'
when 'TAX_PLAN' then 'name'
when 'TAX_PLAN_ITEM' then 'taxPlan,taxPlanItemNo'
when 'TEST' then 'testNumber'
when 'TEST_LIST' then 'name'
when 'TEST_LIST_COMP' then 'refToTestList,analysis,component,analysisCount'
when 'TEST_LIST_ENTRY' then 'name,analysis,analysisCount'
when 'TREATMENT_WORKS' then 'name'
when 'T_DISTRIBUTION_ITEM' then 'tDistributionList,person'
when 'T_DISTRIBUTION_LIST' then 'name'
when 'T_UNCERTAINTY' then 'analysis,version,entryCode'
when 'UNITS' then 'unitCode'
when 'VENDOR' then 'name'
when 'VERSIONS' then 'tableName,name'
when 'WATER_SOURCE' then 'name'
when 'WORKBOOK' then 'workbookNumber'
when 'WORKBOOK_FILES' then 'fileName'
when 'WORKBOOK_IMAGE' then 'workbookNumber,imageNumber'
when 'X_ERESULTS_TRANSLATE' then 'name'
when 'X_ERESULTS_TRANS_DET' then 'xEresultsTranslate,itemId'
when 'ZONE' then 'name'
else businessKeyColumn
end
where systemCode in ('labwaredata','labwareref')
""")

# COMMAND ----------

 #updating WatermarkColumn 

# Tables with Changed_on watermark Column
ExecuteStatement("""
    UPDATE controldb.dbo.extractloadmanifest
    SET WatermarkColumn = 'TO_CHAR(CHANGED_ON, ''DD/MON/YYYY HH24:MI:SS'')' 
    WHERE (SystemCode = 'labwaredata' and SourceTableName in ('ACCOUNT','BATCH','BATCH_RESULT','CONTRACT_QUOTE','DB_FILES','EXPERIMENT','INVENTORY_ITEM','INVENTORY_LOCATION','INVOICE','INVOICE_ITEM','LIMS_NOTES','PERSON','PROJECT_RESULT','RESULT','RESULT_SPEC','SAMPLE','SWC_SAP_BTCH_HDR','TEST','WORKBOOK','WORKBOOK_FILES')) OR (SystemCode = 'labwareref' and SourceTableName in ('ADDRESS_BOOK','ALGAL_MNEMONICS','ANALYSIS','ANALYSIS_TYPES','BATCH_HDR_TEMPLATE','BATCH_LINK','C_CUSTOMER_GROUP','C_ENV_CONSTANTS','CATALOGUE','CHARGE_CODE','COLLECTION_RUN','COMMON_NAME','CONDITION','CONTACT','CONTAINER','CONTRACT_CURRENCY','COST_ITEM','COST_ITEM_RPT','COUNTRY','CUSTOMER','HAZARD','HTML_FILES','INSTRUMENTS','LIMS_CONSTANTS','LIST','LOCATION','PRODUCT','QC_SAMPLES','SAMPLING_POINT','SERVICE_RESERVOIR','STANDARD_REAGENT','STOCK','SUPPLIER','SWC_611SITES','SWC_ABC_CODE','SWC_ANALYTE_MAP','SWC_COMP_TAUKEY','SWC_HAZARD_NAME','SWC_REPORT_TEMPLATE','SWC_TWAG','SWC_X_ACCRED_USERS','T_DISTRIBUTION_LIST','TAX_GROUP','TAX_PLAN','TEST', 'TEST_LIST','TREATMENT_WORKS','UNITS','VENDOR','WATER_SOURCE','WORKBOOK_IMAGE','X_ERESULTS_TRANSLATE','ZONE'))""")

# COMMAND ----------

ExecuteStatement("""
    UPDATE controldb.dbo.extractloadmanifest
    SET DestinationSchema = 'labware' 
    WHERE (SystemCode in ('labwaredata','labwareref'))""")

# COMMAND ----------

 #ADD SOURCE QUERY
ExecuteStatement("""
update dbo.extractLoadManifest set
SourceQuery = case sourceTableName
when 'INVOICE_ITEM' then 'select INVOICE_ITEM.*,INVOICE.CHANGED_ON from lims.INVOICE_ITEM Inner join lims.INVOICE on INVOICE.INVOICE_NUMBER = INVOICE_ITEM.INVOICE_NUMBER'
when 'RESULT' then 'SELECT r.* FROM lims.RESULT r INNER JOIN lims.sample s ON r.SAMPLE_NUMBER = s.SAMPLE_NUMBER
WHERE s.CUSTOMER not like ''Z%'''
when 'RESULT_SPEC' then 'select RESULT_SPEC.*,RESULT.CHANGED_ON from lims.RESULT_SPEC Inner join lims.RESULT on RESULT.RESULT_NUMBER = RESULT_SPEC.RESULT_NUMBER'
when 'SAMPLE' then 'SELECT * FROM lims.SAMPLE s WHERE s.CUSTOMER not like ''Z%'''
when 'TEST' then 'SELECT t.* FROM lims.TEST t INNER JOIN lims.sample s ON t.SAMPLE_NUMBER = s.SAMPLE_NUMBER WHERE s.CUSTOMER not like ''Z%'''
else SourceQuery
end
where systemCode in ('labwaredata','labwareref')
""")

# COMMAND ----------

ExecuteStatement("""
update dbo.extractLoadManifest set
SourceQuery = 'select DIR_NUM, FILE_NAME ,ORIGINAL_PATH, ORIGINAL_NAME, CHANGED_BY, CHANGED_ON, LAST_MODIFIED, FILE_SIZE, DESCRIPTION, GROUP_NAME, COMPUTER_NAME, HASH, FILE_LABEL, WORKBOOK_NUMBER, DOCUMENT_NUMBER from lims.WORKBOOK_FILES'
where systemCode = 'labwaredata' and sourceTableName = 'WORKBOOK_FILES'
""")

# COMMAND ----------

#Adding Extended Properties GourpOrderBy for picking the latest business key record from raw to cleansed 
ExecuteStatement("""
update dbo.ExtractLoadManifest
set ExtendedProperties = '{"GroupOrderBy" : "changedOn Desc"}'
where SystemCode like '%labware%' and WatermarkColumn like '%changed_on%'
""")
