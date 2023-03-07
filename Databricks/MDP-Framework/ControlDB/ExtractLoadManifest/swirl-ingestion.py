# Databricks notebook source
# MAGIC %run ../common-controldb

# COMMAND ----------

swirldata_tables =['BMS_9999999_109',
'BMS_9999999_103',
'BMS_9999999_142',
'BMS_9999999_838',
'BMS_9999999_806',
'BMS_9999999_808',
'BMS_9999999_807',
'BMS_9999999_809',
'BMS_9999999_805',
'BMS_9999999_174',
'BMS_9999999_205',
'BMS_9999999_199',
'BMS_9999999_804',
'BMS_9999999_801',
'BMS_9999999_800',
'BMS_9999999_802',
'BMS_9999999_188',
'BMS_9999999_192',
'BMS_9999999_229',
'BMS_9999999_231',
'BMS_9999999_230',
'BMS_9999999_210',
'BMS_9999999_815',
'BMS_9999999_818',
'BMS_9999999_823',
'BMS_9999999_149',
'BMS_9999999_191',
'BMS_9999999_821',
'BMS_9999999_110',
'BMS_9999999_135',
'BMS_9999999_182',
'BMS_9999999_197',
'BMS_9999999_227',
'BMS_9999999_147',
'BMS_9999999_816',
'BMS_9999999_100',
'BMS_9999999_813',
'BMS_9999999_502',
'BMS_9999999_503',
'BMS_9999999_504',
'BMS_9999999_501',
'BMS_9999999_508',
'BMS_9999999_506',
'BMS_9999999_500',
'BMS_9999999_507',
'BMS_9999999_224',
'BMS_9999999_223',
'BMS_9999999_226',
'BMS_9999999_136',
'BMS_9999999_225',
'BMS_9999999_236',
'BMS_9999999_115',
'BMS_9999999_152',
'BMS_9999999_178',
'BMS_9999999_104',
'BMS_9999999_830',
'BMS_9999999_831',
'BMS_9999999_108',
'BMS_9999999_825',
'BMS_9999999_826',
'BMS_9999999_828',
'BMS_9999999_817',
'BMS_0002185_860',
'BMS_9999999_836',
'BMS_9999999_827',
'BMS_0002185_850',
'BMS_0002185_854',
'BMS_0002185_851',
'BMS_0002185_857',
'BMS_0002185_858',
'BMS_0002185_853',
'BMS_0002185_859',
'BMS_0002185_856',
'BMS_0002185_852',
'BMS_0002185_855',
'BMS_0002185_861',
'BMS_0002185_863',
'BMS_0002185_862',
'BMS_9999999_832',
'BMS_9999999_834',
'BMS_9999999_835',
'BMS_9999999_833',
'BMS_9999999_161',
'BMS_0002185_849',
'BMS_9999999_238',
'BMS_9999999_158',
'BMS_9999999_154',
'BMS_9999999_240',
'BMS_9999999_111',
'BMS_9999999_239',
'BMS_9999999_814',
'BMS_9999999_812',
'BMS_9999999_843',
'BMS_9999999_140',
'BMS_0002185_865',
'BMS_9999999_824',
'BMS_9999999_839',
'BMS_9999999_144',
'BMS_9999999_193',
'BMS_9999999_198',
'BMS_9999999_169',
'BMS_9999999_170',
'BMS_9999999_819',
'BMS_9999999_820',
'BMS_9999999_141',
'BMS_9999999_159',
'BMS_9999999_208',
'PENDINGINCIDENTS_TEMP',
'BMS_9999999_101',
'BMS_9999999_212',
'BMS_9999999_216',
'BMS_9999999_842',
'BMS_9999999_183',
'BMS_9999999_232',
'BMS_9999999_139',
'BMS_9999999_218',
'BMS_9999999_829',
'BMS_9999999_146',
'BMS_9999999_203',
'BMS_9999999_202',
'BMS_9999999_175',
'BMS_9999999_841',
'BMS_9999999_840',
'BMS_9999999_822',
'BMS_9999999_241',
'BMS_9999999_112',
'BMS_9999999_155',
'BMS_9999999_242',
'BMS_9999999_138',
'BMS_9999999_180',
'BMS_9999999_837',
'BMS_9999999_177',
'BMS_9999999_201',
'BMS_9999999_204',
'BMS_9999999_200',
'BMS_9999999_846',
'BMS_9999999_90',
'BMS_9999999_91',
'BMS_9999999_234',
'BMS_9999999_235',
'BMS_9999999_1',
'BMS_9999999_810',
'BMS_9999999_811',
'BMS_9999999_153',
'BMS_9999999_186',
'BMS_9999999_134',
'BMS_9999999_220',
'BMS_9999999_219',
'BMS_9999999_221',
'BMS_9999999_148',
'BMS_9999999_214',
'BMS_9999999_156',
'BMS_9999999_206',
'BMS_9999999_176',
'BMS_9999999_751',
'BMS_9999999_752',
'BMS_9999999_750',
'BMS_9999999_844',
'BMS_9999999_137',
'BMS_9999999_190',
'BMS_9999999_157',
'BMS_9999999_803',
'BMS_MEMOS'
]

swirlref_tables = ['BMS_LOOKUP_ITEMS']

# COMMAND ----------

group_names = {"swirldata":swirldata_tables
              ,"swirlref":swirlref_tables
              } 
source_schema = "cintel"
source_key_vault_secret = "daf-oracle-swirl-connectionstring"
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

    CleanConfig()

    AddIngestion(df)

# COMMAND ----------

 #Add Destination Table Name
# ExecuteStatement("""
# update dbo.extractLoadManifest set
# businessKeyColumn = case sourceTableName
# when 'ACCOUNT' then 'accountNumber'

# else businessKeyColumn
# end
# where systemCode in ('swirldata','swirlref')
# """)

# COMMAND ----------

 #ADD BUSINESS KEY
# ExecuteStatement("""
# update dbo.extractLoadManifest set
# businessKeyColumn = case sourceTableName
# when 'ACCOUNT' then 'accountNumber'
# when 'ADDRESS_BOOK' then 'name'
# when 'ALGAL_MNEMONICS' then 'name'
# when 'ANALYSIS' then 'analysisName,version'
# when 'ANALYSIS_LIMITS' then 'limitNumber,analysis,version'
# when 'ANALYSIS_TYPES' then 'name'
# when 'ANALYSIS_VARIATION' then 'analysis,variation,version'
# when 'APPROVAL' then 'approvalId'
# when 'APPROVAL_DETAILS' then 'approvalId,approvalStep'
# when 'BATCH' then 'name'
# when 'BATCH_COMPONENT' then 'template,name,version'
# when 'BATCH_HDR_TEMPLATE' then 'name,version'
# when 'BATCH_LINK' then 'name'
# when 'BATCH_RESULT' then 'resultNumber'
# when 'BATCH_STANDARD' then 'batchProtocol,orderNumber'
# when 'CATALOGUE' then 'name,version'
# when 'CATALOGUE_ITEM' then 'catalogue,version,costItemNo'
# when 'CHARGES' then 'chargeEntry'
# when 'CHARGE_CODE' then 'name'
# when 'COLLECTION_RUN' then 'name'
# when 'COMMON_NAME' then 'name'
# when 'COMPONENT' then 'analysis,name,version'
# when 'COMPONENT_CODES' then 'analysis,component,version,code'
# when 'COMP_VARIATION' then 'analysis,variation,component,version'
# when 'CONDITION' then 'name'
# when 'CONTACT' then 'contactNumber'
# when 'CONTAINER' then 'name'
# when 'CONTAINER_LIST_ENTRY' then 'containerList,entryName,orderNumber'
# when 'CONTRACT_CURRENCY' then 'name'
# when 'CONTRACT_QUOTE' then 'contractQuoteNo'
# when 'CONTRACT_QUOTE_ITM' then 'contractQuoteNo,cntrctQteItemNo'
# when 'COST_ITEM' then 'costItemNo'
# when 'COST_ITEM_RPT' then 'costItemRptNo'
# when 'COUNTRY' then 'name'
# when 'CUSTOMER' then 'name'
# when 'C_CUSTOMER_GROUP' then 'name'
# when 'C_ENV_CONSTANTS' then 'name'
# when 'C_ENV_CONST_PARAM' then 'cEnvConstants,seqNum'
# when 'DB_FILES' then 'fileName'
# when 'EXPERIMENT' then 'name,experimentNumber'
# when 'HAZARD' then 'name'
# when 'HOLIDAY_SCHED_ENTRY' then 'name,month,year'
# when 'HTML_FILES' then 'name'
# when 'INSTRUMENTS' then 'name'
# when 'INVENTORY_ITEM' then 'itemNumber'
# when 'INVENTORY_LOCATION' then 'name'
# when 'INVOICE' then 'invoiceNumber'
# when 'INVOICE_ADJUSTMENT' then 'invoiceNumber,invoiceAdjustNo'
# when 'INVOICE_ITEM' then 'invoiceItemNo'
# when 'LIMS_CONSTANTS' then 'name'
# when 'LIMS_NOTES' then 'noteId'
# when 'LIST' then 'name'
# when 'LIST_ENTRY' then 'list,name'
# when 'LOCATION' then 'name'
# when 'PERSON' then 'personNumber'
# when 'PERSON_LINKING' then 'counter,linkNumber'
# when 'PRODUCT' then 'name,version'
# when 'PRODUCT_GRADE' then 'product,version,samplingPoint,grade'
# when 'PRODUCT_SPEC' then 'product,entryCode'
# when 'PROD_GRADE_STAGE' then 'product,version,samplingPoint,grade,stage,analysis,specType'
# when 'PROJECT' then 'name'
# when 'PROJECT_RESULT' then 'resultNumber'
# when 'QC_SAMPLES' then 'name'
# when 'REPORT_OBJECTS' then 'reportNumber,objectId'
# when 'REPORTED_RESULT' then 'resultNumber,guId,revisionNo'
# when 'REPORTS' then 'reportNumber'
# when 'RESULT' then 'resultNumber'
# when 'RESULT_SPEC' then 'resultNumber,productSpecCode'
# when 'SAMPLE' then 'sampleNumber'
# when 'SAMPLING_POINT' then 'name'
# when 'SERVICE_RESERVOIR' then 'name'
# when 'STANDARD_REAGENT' then 'name'
# when 'STOCK' then 'name'
# when 'SUPPLIER' then 'name'
# when 'SWC_611SITES' then 'name'
# when 'SWC_ABC_CODE' then 'name'
# when 'SWC_ANALYTE_MAP' then 'name'
# when 'SWC_COMP_TAUKEY' then 'name'
# when 'SWC_HAZARD_NAME' then 'name'
# when 'SWC_QUOTE_REVENUE' then 'contractQuoteNo,seqNum'
# when 'SWC_REPORT_TEMPLATE' then 'name,changedOn'
# when 'SWC_SAP_BTCH_CUS_HDR' then 'sapBtchHdrNum,sapBtchCusHdrNum'
# when 'SWC_SAP_BTCH_HDR' then 'sapBtchHdrNum'
# when 'SWC_SP_HAZARDS' then 'samplingPoint,orderNumber'
# when 'SWC_TWAG' then 'name,changedOn'
# when 'SWC_X_ACCRED_USERS' then 'lab,accreditedUser,accreditedTest'
# when 'TAX_GROUP' then 'name'
# when 'TAX_PLAN' then 'name'
# when 'TAX_PLAN_ITEM' then 'taxPlan,taxPlanItemNo'
# when 'TEST' then 'testNumber'
# when 'TEST_LIST' then 'name'
# when 'TEST_LIST_COMP' then 'refToTestList,analysis,component,analysisCount'
# when 'TEST_LIST_ENTRY' then 'name,analysis,analysisCount'
# when 'TREATMENT_WORKS' then 'name'
# when 'T_DISTRIBUTION_ITEM' then 'tDistributionList,person'
# when 'T_DISTRIBUTION_LIST' then 'name'
# when 'T_UNCERTAINTY' then 'analysis,version,entryCode'
# when 'UNITS' then 'unitCode'
# when 'VENDOR' then 'name'
# when 'VERSIONS' then 'tableName,name'
# when 'WATER_SOURCE' then 'name'
# when 'WORKBOOK' then 'workbookNumber'
# when 'WORKBOOK_FILES' then 'fileName'
# when 'WORKBOOK_IMAGE' then 'workbookNumber,imageNumber'
# when 'X_ERESULTS_TRANSLATE' then 'name'
# when 'X_ERESULTS_TRANS_DET' then 'xEresultsTranslate,itemId'
# when 'ZONE' then 'name'
# else businessKeyColumn
# end
# where systemCode in ('swirldata','swirlref')
# """)

# COMMAND ----------

ExecuteStatement("""
    UPDATE controldb.dbo.extractloadmanifest
    SET DestinationSchema = 'swirl' 
    WHERE (SystemCode in ('swirldata','swirlref'))""")
