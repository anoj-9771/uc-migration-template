# Databricks notebook source
table = 'ZRE_DS_EDW_TIVBDCHARAC_TXT '
table1 = table.lower()
print(table1)

# COMMAND ----------

# DBTITLE 0,Writing Count Result in Database
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS test

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists test.table1

# COMMAND ----------

# DBTITLE 1,[Config] Connection Setup
from datetime import datetime

global fileCount

storage_account_name = "sablobdaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sablob-key")
container_name = "archive"
fileLocation = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/"
fileType = 'json'
print(storage_account_name)
spark.conf.set("fs.azure.account.key."+storage_account_name+".blob.core.windows.net",storage_account_access_key) 

# COMMAND ----------

def listDetails(inFile):
    global fileCount
    dfs[fileCount] = spark.read.format(fileType).option("inferSchema", "true").load(inFile.path)
    print(f'Results for {inFile.name.strip("/")}')
    dfs[fileCount].printSchema()
    tmpTable = f'{inFile.name.split(".")[0]}_file{str(fileCount)}'
    dfs[fileCount].createOrReplaceTempView(tmpTable)
    display(spark.sql(f'select * from {tmpTable}'))
    testdf = spark.sql(f'select * from {tmpTable}')
    testdf.write.format(fileType).mode("append").saveAsTable("test" + "." + table)
   

# COMMAND ----------

# DBTITLE 1,List folders in fileLocation
folders = dbutils.fs.ls(fileLocation)
fileNames = []
fileCount = 0
dfs = {}

for folder in folders:
    try:
        subfolders = dbutils.fs.ls(folder.path)
        prntDate = False
        for subfolder in subfolders:
            files = dbutils.fs.ls(subfolder.path)
            prntTime = False
            for myFile in files:
                if myFile.name[:len(table)] == table:
                    fileCount += 1
                    if not prntDate:
                        print(f'{datetime.strftime(datetime.strptime(folder.name.strip("/"),"%Y%m%d"),"%Y-%m-%d")}')
                        printDate = True
                    
                    if not prntTime:
                        print(f'\t{subfolder.name.split("_")[-1].strip("/")}')
                        prntTime = True
                        
                    print(f'\t\t{myFile.name.strip("/")}\t{myFile.size}')
                    
                    if myFile.size > 0:
                        fileNames.append(myFile)
    except:
        print(f'Invalid folder name: {folder.name.strip("/")}')

for myFile in fileNames:
    listDetails(myFile)

# COMMAND ----------

sourcedf = spark.sql(f"select * from test.{table1}")
display(sourcedf)

# COMMAND ----------

# DBTITLE 1,Source schema check
sourcedf.printSchema()

# COMMAND ----------

sourcedf.createOrReplaceTempView("Source")

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct EXTRACT_DATETIME from Source order by EXTRACT_DATETIME desc

# COMMAND ----------

# DBTITLE 1,[Source with mapping]
# MAGIC %sql
# MAGIC select
# MAGIC simulationPeriodID
# MAGIC ,billingDocumentNumber
# MAGIC ,companyCode
# MAGIC ,divisonCode
# MAGIC ,contractAccountNumber
# MAGIC ,contractId
# MAGIC ,billingTransactionCode
# MAGIC ,mainTransactionLineItemCode
# MAGIC ,contractAccountDeterminationID
# MAGIC ,portionNumber
# MAGIC ,numberOfContractDaysBilled
# MAGIC ,numberOfBilledContracts
# MAGIC ,numberOfBillingDocuments
# MAGIC ,billingDocumentLineItemID
# MAGIC ,lineItemTypeCode
# MAGIC ,billingClassCode
# MAGIC ,subtransactionForDocumentItem
# MAGIC ,rateCategoryCode
# MAGIC ,statisticalAnalysisRateType
# MAGIC ,statisticalRate
# MAGIC ,consumptionMonth
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,billingLineItemReleventPostingIndicator
# MAGIC ,quantityStatisticsGroupCode
# MAGIC ,amountStatisticsGroupCode
# MAGIC ,billedQuantityStatisticsCode
# MAGIC ,controllingArea
# MAGIC ,profitCenter
# MAGIC ,wbsElement
# MAGIC ,currencyKey
# MAGIC ,billingMeasurementUnitCode
# MAGIC ,billingLineItemNetAmount
# MAGIC ,billingQuantity
# MAGIC ,agreementNumber
# MAGIC ,priceAmount
# MAGIC ,crmProduct
# MAGIC ,lineItemType
# MAGIC ,printDocumentLineItemId
# MAGIC ,installationId
# MAGIC ,cityCode
# MAGIC ,countryShortName
# MAGIC ,stateCode
# MAGIC ,politicalRegionCode
# MAGIC ,salesEmployee from(
# MAGIC select
# MAGIC SIMRUNID as simulationPeriodID
# MAGIC ,BELNR as billingDocumentNumber
# MAGIC ,BUKRS as companyCode
# MAGIC ,SPARTE as divisonCode
# MAGIC ,VKONT as contractAccountNumber
# MAGIC ,VERTRAG as contractId
# MAGIC ,ABRVORG as billingTransactionCode
# MAGIC ,HVORG as mainTransactionLineItemCode
# MAGIC ,KOFIZ as contractAccountDeterminationID
# MAGIC ,PORTION as portionNumber
# MAGIC ,ANZTAGE as numberOfContractDaysBilled
# MAGIC ,ANZVERTR as numberOfBilledContracts
# MAGIC ,CNTBILLDOC as numberOfBillingDocuments
# MAGIC ,BELZEILE as billingDocumentLineItemID
# MAGIC ,BELZART as lineItemTypeCode
# MAGIC ,AKLASSE as billingClassCode
# MAGIC ,TVORG as subtransactionForDocumentItem
# MAGIC ,TARIFTYP as rateCategoryCode
# MAGIC ,STATTART as statisticalAnalysisRateType
# MAGIC ,STTARIF as statisticalRate
# MAGIC ,VBRMONAT as consumptionMonth
# MAGIC ,AB as validFromDate
# MAGIC ,BIS as validToDate
# MAGIC ,BUCHREL as billingLineItemReleventPostingIndicator
# MAGIC ,STGRQNT as quantityStatisticsGroupCode
# MAGIC ,STGRAMT as amountStatisticsGroupCode
# MAGIC ,ARTMENGE as billedQuantityStatisticsCode
# MAGIC ,KOKRS as controllingArea
# MAGIC ,PRCTR as profitCenter
# MAGIC ,PS_PSP_PNR as wbsElement
# MAGIC ,WAERS as currencyKey
# MAGIC ,MASSBILL as billingMeasurementUnitCode
# MAGIC ,BETRAG as billingLineItemNetAmount
# MAGIC ,MENGE as billingQuantity
# MAGIC ,ZZAGREEMENT_NUM as agreementNumber
# MAGIC ,PREISBTR as priceAmount
# MAGIC ,CRM_PRODUCT as crmProduct
# MAGIC ,BELZART_NAME as lineItemType
# MAGIC ,PRINTDOCLINE as printDocumentLineItemId
# MAGIC ,ANLAGE as installationId
# MAGIC ,CITY_CODE as cityCode
# MAGIC ,COUNTRY as countryShortName
# MAGIC ,REGION as stateCode
# MAGIC ,REGPOLIT as politicalRegionCode
# MAGIC ,SALESEMPLOYEE as salesEmployee
# MAGIC ,row_number() over (partition by simulationPeriodID
# MAGIC ,billingDocumentNumber,billingDocumentLineItemID,validToDate order by EXTRACT_DATETIME desc) as rn
# MAGIC from source) a where a.rn = 1

# COMMAND ----------

lakedf = spark.sql("select * from cleansed.tablename")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Verification] count
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.tablename
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC simulationPeriodID
# MAGIC ,billingDocumentNumber
# MAGIC ,companyCode
# MAGIC ,divisonCode
# MAGIC ,contractAccountNumber
# MAGIC ,contractId
# MAGIC ,billingTransactionCode
# MAGIC ,mainTransactionLineItemCode
# MAGIC ,contractAccountDeterminationID
# MAGIC ,portionNumber
# MAGIC ,numberOfContractDaysBilled
# MAGIC ,numberOfBilledContracts
# MAGIC ,numberOfBillingDocuments
# MAGIC ,billingDocumentLineItemID
# MAGIC ,lineItemTypeCode
# MAGIC ,billingClassCode
# MAGIC ,subtransactionForDocumentItem
# MAGIC ,rateCategoryCode
# MAGIC ,statisticalAnalysisRateType
# MAGIC ,statisticalRate
# MAGIC ,consumptionMonth
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,billingLineItemReleventPostingIndicator
# MAGIC ,quantityStatisticsGroupCode
# MAGIC ,amountStatisticsGroupCode
# MAGIC ,billedQuantityStatisticsCode
# MAGIC ,controllingArea
# MAGIC ,profitCenter
# MAGIC ,wbsElement
# MAGIC ,currencyKey
# MAGIC ,billingMeasurementUnitCode
# MAGIC ,billingLineItemNetAmount
# MAGIC ,billingQuantity
# MAGIC ,agreementNumber
# MAGIC ,priceAmount
# MAGIC ,crmProduct
# MAGIC ,lineItemType
# MAGIC ,printDocumentLineItemId
# MAGIC ,installationId
# MAGIC ,cityCode
# MAGIC ,countryShortName
# MAGIC ,stateCode
# MAGIC ,politicalRegionCode
# MAGIC ,salesEmployee from(
# MAGIC select
# MAGIC SIMRUNID as simulationPeriodID
# MAGIC ,BELNR as billingDocumentNumber
# MAGIC ,BUKRS as companyCode
# MAGIC ,SPARTE as divisonCode
# MAGIC ,VKONT as contractAccountNumber
# MAGIC ,VERTRAG as contractId
# MAGIC ,ABRVORG as billingTransactionCode
# MAGIC ,HVORG as mainTransactionLineItemCode
# MAGIC ,KOFIZ as contractAccountDeterminationID
# MAGIC ,PORTION as portionNumber
# MAGIC ,ANZTAGE as numberOfContractDaysBilled
# MAGIC ,ANZVERTR as numberOfBilledContracts
# MAGIC ,CNTBILLDOC as numberOfBillingDocuments
# MAGIC ,BELZEILE as billingDocumentLineItemID
# MAGIC ,BELZART as lineItemTypeCode
# MAGIC ,AKLASSE as billingClassCode
# MAGIC ,TVORG as subtransactionForDocumentItem
# MAGIC ,TARIFTYP as rateCategoryCode
# MAGIC ,STATTART as statisticalAnalysisRateType
# MAGIC ,STTARIF as statisticalRate
# MAGIC ,VBRMONAT as consumptionMonth
# MAGIC ,AB as validFromDate
# MAGIC ,BIS as validToDate
# MAGIC ,BUCHREL as billingLineItemReleventPostingIndicator
# MAGIC ,STGRQNT as quantityStatisticsGroupCode
# MAGIC ,STGRAMT as amountStatisticsGroupCode
# MAGIC ,ARTMENGE as billedQuantityStatisticsCode
# MAGIC ,KOKRS as controllingArea
# MAGIC ,PRCTR as profitCenter
# MAGIC ,PS_PSP_PNR as wbsElement
# MAGIC ,WAERS as currencyKey
# MAGIC ,MASSBILL as billingMeasurementUnitCode
# MAGIC ,BETRAG as billingLineItemNetAmount
# MAGIC ,MENGE as billingQuantity
# MAGIC ,ZZAGREEMENT_NUM as agreementNumber
# MAGIC ,PREISBTR as priceAmount
# MAGIC ,CRM_PRODUCT as crmProduct
# MAGIC ,BELZART_NAME as lineItemType
# MAGIC ,PRINTDOCLINE as printDocumentLineItemId
# MAGIC ,ANLAGE as installationId
# MAGIC ,CITY_CODE as cityCode
# MAGIC ,COUNTRY as countryShortName
# MAGIC ,REGION as stateCode
# MAGIC ,REGPOLIT as politicalRegionCode
# MAGIC ,SALESEMPLOYEE as salesEmployee
# MAGIC ,row_number() over (partition by simulationPeriodID
# MAGIC ,billingDocumentNumber,billingDocumentLineItemID,validToDate order by EXTRACT_DATETIME desc) as rn
# MAGIC from source) a where a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Duplicate checks]
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY simulationPeriodID
# MAGIC ,billingDocumentNumber,billingDocumentLineItemID,validToDate order by validFromDate) as rn
# MAGIC FROM  cleansed.tablename
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC simulationPeriodID
# MAGIC ,billingDocumentNumber
# MAGIC ,companyCode
# MAGIC ,divisonCode
# MAGIC ,contractAccountNumber
# MAGIC ,contractId
# MAGIC ,billingTransactionCode
# MAGIC ,mainTransactionLineItemCode
# MAGIC ,contractAccountDeterminationID
# MAGIC ,portionNumber
# MAGIC ,numberOfContractDaysBilled
# MAGIC ,numberOfBilledContracts
# MAGIC ,numberOfBillingDocuments
# MAGIC ,billingDocumentLineItemID
# MAGIC ,lineItemTypeCode
# MAGIC ,billingClassCode
# MAGIC ,subtransactionForDocumentItem
# MAGIC ,rateCategoryCode
# MAGIC ,statisticalAnalysisRateType
# MAGIC ,statisticalRate
# MAGIC ,consumptionMonth
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,billingLineItemReleventPostingIndicator
# MAGIC ,quantityStatisticsGroupCode
# MAGIC ,amountStatisticsGroupCode
# MAGIC ,billedQuantityStatisticsCode
# MAGIC ,controllingArea
# MAGIC ,profitCenter
# MAGIC ,wbsElement
# MAGIC ,currencyKey
# MAGIC ,billingMeasurementUnitCode
# MAGIC ,billingLineItemNetAmount
# MAGIC ,billingQuantity
# MAGIC ,agreementNumber
# MAGIC ,priceAmount
# MAGIC ,crmProduct
# MAGIC ,lineItemType
# MAGIC ,printDocumentLineItemId
# MAGIC ,installationId
# MAGIC ,cityCode
# MAGIC ,countryShortName
# MAGIC ,stateCode
# MAGIC ,politicalRegionCode
# MAGIC ,salesEmployee from(
# MAGIC select
# MAGIC SIMRUNID as simulationPeriodID
# MAGIC ,BELNR as billingDocumentNumber
# MAGIC ,BUKRS as companyCode
# MAGIC ,SPARTE as divisonCode
# MAGIC ,VKONT as contractAccountNumber
# MAGIC ,VERTRAG as contractId
# MAGIC ,ABRVORG as billingTransactionCode
# MAGIC ,HVORG as mainTransactionLineItemCode
# MAGIC ,KOFIZ as contractAccountDeterminationID
# MAGIC ,PORTION as portionNumber
# MAGIC ,ANZTAGE as numberOfContractDaysBilled
# MAGIC ,ANZVERTR as numberOfBilledContracts
# MAGIC ,CNTBILLDOC as numberOfBillingDocuments
# MAGIC ,BELZEILE as billingDocumentLineItemID
# MAGIC ,BELZART as lineItemTypeCode
# MAGIC ,AKLASSE as billingClassCode
# MAGIC ,TVORG as subtransactionForDocumentItem
# MAGIC ,TARIFTYP as rateCategoryCode
# MAGIC ,STATTART as statisticalAnalysisRateType
# MAGIC ,STTARIF as statisticalRate
# MAGIC ,VBRMONAT as consumptionMonth
# MAGIC ,AB as validFromDate
# MAGIC ,BIS as validToDate
# MAGIC ,BUCHREL as billingLineItemReleventPostingIndicator
# MAGIC ,STGRQNT as quantityStatisticsGroupCode
# MAGIC ,STGRAMT as amountStatisticsGroupCode
# MAGIC ,ARTMENGE as billedQuantityStatisticsCode
# MAGIC ,KOKRS as controllingArea
# MAGIC ,PRCTR as profitCenter
# MAGIC ,PS_PSP_PNR as wbsElement
# MAGIC ,WAERS as currencyKey
# MAGIC ,MASSBILL as billingMeasurementUnitCode
# MAGIC ,BETRAG as billingLineItemNetAmount
# MAGIC ,MENGE as billingQuantity
# MAGIC ,ZZAGREEMENT_NUM as agreementNumber
# MAGIC ,PREISBTR as priceAmount
# MAGIC ,CRM_PRODUCT as crmProduct
# MAGIC ,BELZART_NAME as lineItemType
# MAGIC ,PRINTDOCLINE as printDocumentLineItemId
# MAGIC ,ANLAGE as installationId
# MAGIC ,CITY_CODE as cityCode
# MAGIC ,COUNTRY as countryShortName
# MAGIC ,REGION as stateCode
# MAGIC ,REGPOLIT as politicalRegionCode
# MAGIC ,SALESEMPLOYEE as salesEmployee
# MAGIC ,row_number() over (partition by simulationPeriodID
# MAGIC ,billingDocumentNumber,billingDocumentLineItemID,validToDate order by EXTRACT_DATETIME desc) as rn
# MAGIC from source) a where a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC simulationPeriodID
# MAGIC ,billingDocumentNumber
# MAGIC ,companyCode
# MAGIC ,divisonCode
# MAGIC ,contractAccountNumber
# MAGIC ,contractId
# MAGIC ,billingTransactionCode
# MAGIC ,mainTransactionLineItemCode
# MAGIC ,contractAccountDeterminationID
# MAGIC ,portionNumber
# MAGIC ,numberOfContractDaysBilled
# MAGIC ,numberOfBilledContracts
# MAGIC ,numberOfBillingDocuments
# MAGIC ,billingDocumentLineItemID
# MAGIC ,lineItemTypeCode
# MAGIC ,billingClassCode
# MAGIC ,subtransactionForDocumentItem
# MAGIC ,rateCategoryCode
# MAGIC ,statisticalAnalysisRateType
# MAGIC ,statisticalRate
# MAGIC ,consumptionMonth
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,billingLineItemReleventPostingIndicator
# MAGIC ,quantityStatisticsGroupCode
# MAGIC ,amountStatisticsGroupCode
# MAGIC ,billedQuantityStatisticsCode
# MAGIC ,controllingArea
# MAGIC ,profitCenter
# MAGIC ,wbsElement
# MAGIC ,currencyKey
# MAGIC ,billingMeasurementUnitCode
# MAGIC ,billingLineItemNetAmount
# MAGIC ,billingQuantity
# MAGIC ,agreementNumber
# MAGIC ,priceAmount
# MAGIC ,crmProduct
# MAGIC ,lineItemType
# MAGIC ,printDocumentLineItemId
# MAGIC ,installationId
# MAGIC ,cityCode
# MAGIC ,countryShortName
# MAGIC ,stateCode
# MAGIC ,politicalRegionCode
# MAGIC ,salesEmployee
# MAGIC from
# MAGIC cleansed.tablename

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC simulationPeriodID
# MAGIC ,billingDocumentNumber
# MAGIC ,companyCode
# MAGIC ,divisonCode
# MAGIC ,contractAccountNumber
# MAGIC ,contractId
# MAGIC ,billingTransactionCode
# MAGIC ,mainTransactionLineItemCode
# MAGIC ,contractAccountDeterminationID
# MAGIC ,portionNumber
# MAGIC ,numberOfContractDaysBilled
# MAGIC ,numberOfBilledContracts
# MAGIC ,numberOfBillingDocuments
# MAGIC ,billingDocumentLineItemID
# MAGIC ,lineItemTypeCode
# MAGIC ,billingClassCode
# MAGIC ,subtransactionForDocumentItem
# MAGIC ,rateCategoryCode
# MAGIC ,statisticalAnalysisRateType
# MAGIC ,statisticalRate
# MAGIC ,consumptionMonth
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,billingLineItemReleventPostingIndicator
# MAGIC ,quantityStatisticsGroupCode
# MAGIC ,amountStatisticsGroupCode
# MAGIC ,billedQuantityStatisticsCode
# MAGIC ,controllingArea
# MAGIC ,profitCenter
# MAGIC ,wbsElement
# MAGIC ,currencyKey
# MAGIC ,billingMeasurementUnitCode
# MAGIC ,billingLineItemNetAmount
# MAGIC ,billingQuantity
# MAGIC ,agreementNumber
# MAGIC ,priceAmount
# MAGIC ,crmProduct
# MAGIC ,lineItemType
# MAGIC ,printDocumentLineItemId
# MAGIC ,installationId
# MAGIC ,cityCode
# MAGIC ,countryShortName
# MAGIC ,stateCode
# MAGIC ,politicalRegionCode
# MAGIC ,salesEmployee
# MAGIC from
# MAGIC cleansed.tablename
# MAGIC except
# MAGIC select
# MAGIC simulationPeriodID
# MAGIC ,billingDocumentNumber
# MAGIC ,companyCode
# MAGIC ,divisonCode
# MAGIC ,contractAccountNumber
# MAGIC ,contractId
# MAGIC ,billingTransactionCode
# MAGIC ,mainTransactionLineItemCode
# MAGIC ,contractAccountDeterminationID
# MAGIC ,portionNumber
# MAGIC ,numberOfContractDaysBilled
# MAGIC ,numberOfBilledContracts
# MAGIC ,numberOfBillingDocuments
# MAGIC ,billingDocumentLineItemID
# MAGIC ,lineItemTypeCode
# MAGIC ,billingClassCode
# MAGIC ,subtransactionForDocumentItem
# MAGIC ,rateCategoryCode
# MAGIC ,statisticalAnalysisRateType
# MAGIC ,statisticalRate
# MAGIC ,consumptionMonth
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,billingLineItemReleventPostingIndicator
# MAGIC ,quantityStatisticsGroupCode
# MAGIC ,amountStatisticsGroupCode
# MAGIC ,billedQuantityStatisticsCode
# MAGIC ,controllingArea
# MAGIC ,profitCenter
# MAGIC ,wbsElement
# MAGIC ,currencyKey
# MAGIC ,billingMeasurementUnitCode
# MAGIC ,billingLineItemNetAmount
# MAGIC ,billingQuantity
# MAGIC ,agreementNumber
# MAGIC ,priceAmount
# MAGIC ,crmProduct
# MAGIC ,lineItemType
# MAGIC ,printDocumentLineItemId
# MAGIC ,installationId
# MAGIC ,cityCode
# MAGIC ,countryShortName
# MAGIC ,stateCode
# MAGIC ,politicalRegionCode
# MAGIC ,salesEmployee from(
# MAGIC select
# MAGIC SIMRUNID as simulationPeriodID
# MAGIC ,BELNR as billingDocumentNumber
# MAGIC ,BUKRS as companyCode
# MAGIC ,SPARTE as divisonCode
# MAGIC ,VKONT as contractAccountNumber
# MAGIC ,VERTRAG as contractId
# MAGIC ,ABRVORG as billingTransactionCode
# MAGIC ,HVORG as mainTransactionLineItemCode
# MAGIC ,KOFIZ as contractAccountDeterminationID
# MAGIC ,PORTION as portionNumber
# MAGIC ,ANZTAGE as numberOfContractDaysBilled
# MAGIC ,ANZVERTR as numberOfBilledContracts
# MAGIC ,CNTBILLDOC as numberOfBillingDocuments
# MAGIC ,BELZEILE as billingDocumentLineItemID
# MAGIC ,BELZART as lineItemTypeCode
# MAGIC ,AKLASSE as billingClassCode
# MAGIC ,TVORG as subtransactionForDocumentItem
# MAGIC ,TARIFTYP as rateCategoryCode
# MAGIC ,STATTART as statisticalAnalysisRateType
# MAGIC ,STTARIF as statisticalRate
# MAGIC ,VBRMONAT as consumptionMonth
# MAGIC ,AB as validFromDate
# MAGIC ,BIS as validToDate
# MAGIC ,BUCHREL as billingLineItemReleventPostingIndicator
# MAGIC ,STGRQNT as quantityStatisticsGroupCode
# MAGIC ,STGRAMT as amountStatisticsGroupCode
# MAGIC ,ARTMENGE as billedQuantityStatisticsCode
# MAGIC ,KOKRS as controllingArea
# MAGIC ,PRCTR as profitCenter
# MAGIC ,PS_PSP_PNR as wbsElement
# MAGIC ,WAERS as currencyKey
# MAGIC ,MASSBILL as billingMeasurementUnitCode
# MAGIC ,BETRAG as billingLineItemNetAmount
# MAGIC ,MENGE as billingQuantity
# MAGIC ,ZZAGREEMENT_NUM as agreementNumber
# MAGIC ,PREISBTR as priceAmount
# MAGIC ,CRM_PRODUCT as crmProduct
# MAGIC ,BELZART_NAME as lineItemType
# MAGIC ,PRINTDOCLINE as printDocumentLineItemId
# MAGIC ,ANLAGE as installationId
# MAGIC ,CITY_CODE as cityCode
# MAGIC ,COUNTRY as countryShortName
# MAGIC ,REGION as stateCode
# MAGIC ,REGPOLIT as politicalRegionCode
# MAGIC ,SALESEMPLOYEE as salesEmployee
# MAGIC ,row_number() over (partition by simulationPeriodID
# MAGIC ,billingDocumentNumber,billingDocumentLineItemID,validToDate order by EXTRACT_DATETIME desc) as rn
# MAGIC from source) a where a.rn = 1
