# Databricks notebook source
# MAGIC %run ../common-controldb

# COMMAND ----------

(
    spark.table("controldb.dbo_extractloadmanifest")
    .filter("SystemCode in ('labwaredata','labwareref')")
    .filter("Enabled = 'true'")
    .display()
)

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
    SELECT "{code}" SystemCode, "{source_schema}" SourceSchema, "{source_key_vault_secret}" SourceKeyVaultSecret, '' SourceQuery, "{source_handler}" SourceHandler, '' RawFileExtension, "{raw_handler}" RawHandler, '' ExtendedProperties, "{cleansed_handler}" CleansedHandler
    )"""
    select_list = [
        'SELECT "' + x + '" SourceTableName, * FROM _Base' for x in group_names[code]
    ]
    select_string = " UNION ".join(select_list)
    select_statement = select_string + " ORDER BY SourceSchema, SourceTableName"

    df = spark.sql(f"""{base_query} {select_statement}""")
    SYSTEM_CODE = code

    CleanConfig()

    AddIngestion(df)

# COMMAND ----------

 #ADD BUSINESS KEY
ExecuteStatement("""
update dbo.extractLoadManifest set
businessKeyColumn = case sourceTableName
when 'ACCOUNT' then 'account_number'
when 'ADDRESS_BOOK' then 'name'
when 'ALGAL_MNEMONICS' then 'name'
when 'ANALYSIS' then 'name,version'
when 'ANALYSIS_LIMITS' then 'limit_number,analysis,version'
when 'ANALYSIS_TYPES' then 'name'
when 'ANALYSIS_VARIATION' then 'analysis,variation,version'
when 'APPROVAL' then 'approval_id'
when 'APPROVAL_DETAILS' then 'approval_id,approval_step'
when 'BATCH' then 'name'
when 'BATCH_COMPONENT' then 'template,name,version'
when 'BATCH_HDR_TEMPLATE' then 'name,version'
when 'BATCH_LINK' then 'name'
when 'BATCH_RESULT' then 'result_number'
when 'BATCH_STANDARD' then 'batch_protocol,order_number'
when 'CATALOGUE' then 'name,version'
when 'CATALOGUE_ITEM' then 'catalogue,version,cost_item_no'
when 'CHARGES' then 'charge_entry'
when 'CHARGE_CODE' then 'name'
when 'COLLECTION_RUN' then 'name'
when 'COMMON_NAME' then 'name'
when 'COMPONENT' then 'analysis,name,version'
when 'COMPONENT_CODES' then 'analysis,component,version,code'
when 'COMP_VARIATION' then 'analysis,variation,component,version'
when 'CONDITION' then 'name'
when 'CONTACT' then 'contact_number'
when 'CONTAINER' then 'name'
when 'CONTAINER_LIST_ENTRY' then 'container_list,entry_name,order_number'
when 'CONTRACT_CURRENCY' then 'name'
when 'CONTRACT_QUOTE' then 'contract_quote_no'
when 'CONTRACT_QUOTE_ITM' then 'contract_quote_no,cntrct_qte_item_no'
when 'COST_ITEM' then 'cost_item_no'
when 'COST_ITEM_RPT' then 'cost_item_rpt_no'
when 'COUNTRY' then 'name'
when 'CUSTOMER' then 'name'
when 'C_CUSTOMER_GROUP' then 'name'
when 'C_ENV_CONSTANTS' then 'name'
when 'C_ENV_CONST_PARAM' then 'c_env_constants,seq_num'
when 'DB_FILES' then 'file_name'
when 'EXPERIMENT' then 'name,experiment_number'
when 'HAZARD' then 'name'
when 'HOLIDAY_SCHED_ENTRY' then 'name,month,year'
when 'HTML_FILES' then 'name'
when 'INSTRUMENTS' then 'name'
when 'INVENTORY_ITEM' then 'item_number'
when 'INVENTORY_LOCATION' then 'name'
when 'INVOICE' then 'invoice_number'
when 'INVOICE_ADJUSTMENT' then 'invoice_number,invoice_adjust_no'
when 'INVOICE_ITEM' then 'invoice_item_no'
when 'LIMS_CONSTANTS' then 'name'
when 'LIMS_NOTES' then 'note_id'
when 'LIST' then 'name'
when 'LIST_ENTRY' then 'list,name'
when 'LOCATION' then 'name'
when 'PERSON' then 'person_number'
when 'PERSON_LINKING' then 'counter,link_number'
when 'PRODUCT' then 'name,version'
when 'PRODUCT_GRADE' then 'product,version,sampling_point,grade'
when 'PRODUCT_SPEC' then 'product,entry_code'
when 'PROD_GRADE_STAGE' then 'product,version,sampling_point,grade,stage,analysis,spec_type'
when 'PROJECT' then 'name'
when 'PROJECT_RESULT' then 'result_number'
when 'QC_SAMPLES' then 'name'
when 'REPORTED_RESULT' then 'result_number,gu_id,revision_no'
when 'REPORTS' then 'report_number'
when 'RESULT' then 'result_number'
when 'RESULT_SPEC' then 'result_number,product_spec_code'
when 'SAMPLE' then 'sample_number'
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
when 'SWC_QUOTE_REVENUE' then 'contract_quote_no,seq_num'
when 'SWC_SAP_BTCH_CUS_HDR' then 'sap_btch_hdr_num,sap_btch_cus_hdr_num'
when 'SWC_SAP_BTCH_HDR' then 'sap_btch_hdr_num'
when 'SWC_SP_HAZARDS' then 'sampling_point,order_number'
when 'SWC_X_ACCRED_USERS' then 'lab,accredited_user,accredited_test'
when 'TAX_GROUP' then 'name'
when 'TAX_PLAN' then 'name'
when 'TAX_PLAN_ITEM' then 'tax_plan,tax_plan_item_no'
when 'TEST' then 'test_number'
when 'TEST_LIST' then 'name'
when 'TEST_LIST_COMP' then 'test_list,analysis,component,analysis_count'
when 'TEST_LIST_ENTRY' then 'name,analysis,analysis_count'
when 'TREATMENT_WORKS' then 'name'
when 'T_DISTRIBUTION_ITEM' then 't_distribution_list,person'
when 'T_DISTRIBUTION_LIST' then 'name'
when 'T_UNCERTAINTY' then 'analysis,version,entry_code'
when 'UNITS' then 'unit_code'
when 'VENDOR' then 'name'
when 'VERSIONS' then 'table_name,name'
when 'WATER_SOURCE' then 'name'
when 'WORKBOOK' then 'workbook_number'
when 'WORKBOOK_FILES' then 'file_name'
when 'WORKBOOK_IMAGE' then 'workbook_number,image_number'
when 'X_ERESULTS_TRANSLATE' then 'name'
when 'X_ERESULTS_TRANS_DET' then 'x_eresults_translate,item_id'
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
    SET WatermarkColumn = 'CHANGED_ON' 
    WHERE (SystemCode = 'labwaredata' and SourceTableName in ('ACCOUNT','BATCH','BATCH_RESULT','CONTRACT_QUOTE','DB_FILES','EXPERIMENT','INVENTORY_ITEM','INVENTORY_LOCATION','INVOICE','INVOICE_ITEM','LIMS_NOTES','PERSON','PROJECT_RESULT','RESULT','RESULT_SPEC','SAMPLE','SWC_SAP_BTCH_HDR','TEST','WORKBOOK','WORKBOOK_FILES')) OR (SystemCode = 'labwareref' and SourceTableName in ('ADDRESS_BOOK','ALGAL_MNEMONICS','ANALYSIS','ANALYSIS_TYPES','BATCH_HDR_TEMPLATE','BATCH_LINK','C_CUSTOMER_GROUP','C_ENV_CONSTANTS','CATALOGUE','CHARGE_CODE','COLLECTION_RUN','COMMON_NAME','CONDITION','CONTACT','CONTAINER','CONTRACT_CURRENCY','COST_ITEM','COST_ITEM_RPT','COUNTRY','CUSTOMER','HAZARD','HTML_FILES','INSTRUMENTS','LIMS_CONSTANTS','LIST','LOCATION','PRODUCT','QC_SAMPLES','SAMPLING_POINT','SERVICE_RESERVOIR','STANDARD_REAGENT','STOCK','SUPPLIER','SWC_611SITES','SWC_ABC_CODE','SWC_ANALYTE_MAP','SWC_COMP_TAUKEY','SWC_HAZARD_NAME','SWC_REPORT_TEMPLATE','SWC_TWAG','SWC_X_ACCRED_USERS','T_DISTRIBUTION_LIST','TAX_GROUP','TAX_PLAN','TEST', 'TEST_LIST','TREATMENT_WORKS','UNITS','VENDOR','WATER_SOURCE','WORKBOOK_IMAGE','X_ERESULTS_TRANSLATE','ZONE'))""")

# COMMAND ----------

ExecuteStatement("""
    UPDATE controldb.dbo.extractloadmanifest
    SET DestinationSchema = 'labware' 
    WHERE (SystemCode in ('labwaredata','labwareref'))""")

# COMMAND ----------

 #ADD SOURCE QUERY
ExecuteStatement("""
update dbo.extractLoadManifest set SourceQuery = case sourceTableName
when 'INVOICE_ITEM' then 'select I2.*,I.CHANGED_ON from lims.INVOICE_ITEM I2 Inner join lims.INVOICE I on I.INVOICE_NUMBER = I2.INVOICE_NUMBER'
when 'RESULT_SPEC' then 'select RS.*,R.CHANGED_ON from lims.RESULT_SPEC RS Inner join lims.RESULT R on R.RESULT_NUMBER = RS.RESULT_NUMBER'
when 'WORKBOOK_FILES' then 'select DIR_NUM,FILE_NAME,ORIGINAL_PATH,ORIGINAL_NAME,CHANGED_BY,CHANGED_ON,LAST_MODIFIED,FILE_SIZE,DESCRIPTION,GROUP_NAME,COMPUTER_NAME,HASH,FILE_LABEL,WORKBOOK_NUMBER,DOCUMENT_NUMBER from lims.WORKBOOK_FILES'
when 'RESULT' then 'SELECT r.* FROM lims.RESULT r INNER JOIN lims.sample s ON r.SAMPLE_NUMBER = s.SAMPLE_NUMBER WHERE s.CUSTOMER not like ''Z%'''
when 'SAMPLE' then 'SELECT * FROM lims.SAMPLE s WHERE s.CUSTOMER not like ''Z%'''
when 'TEST' then 'SELECT t.* FROM lims.TEST t INNER JOIN lims.sample s ON t.SAMPLE_NUMBER = s.SAMPLE_NUMBER WHERE s.CUSTOMER not like ''Z%'''
else SourceQuery
end
where systemCode in ('labwaredata','labwareref')
""")
