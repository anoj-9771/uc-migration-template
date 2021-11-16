# Databricks notebook source
table = '0UC_SALES_STATS_03'
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
# MAGIC postingDate
# MAGIC ,documentEnteredDate
# MAGIC ,reconciliationKeyForGeneralLedger
# MAGIC ,isReversalTransaction
# MAGIC ,billingDocumentNumber
# MAGIC ,companyCode
# MAGIC ,divisonCode
# MAGIC ,businessPartnerNumber
# MAGIC ,contractAccountNumber
# MAGIC ,contractId
# MAGIC ,alternativeContractAccountForCollectiveBills
# MAGIC ,billingTransactionCode
# MAGIC ,applicationArea
# MAGIC ,mainTransactionLineItemCode
# MAGIC ,accountDeterminationCode
# MAGIC ,portionNumber
# MAGIC ,meterReadingUnit
# MAGIC ,billingAllocationDate
# MAGIC ,numberOfContractDaysBilled
# MAGIC ,numberOfBilledContracts
# MAGIC ,numberofInvoicingDocuments
# MAGIC ,numberOfBillingDocuments
# MAGIC ,billingDocumentLineItemID
# MAGIC ,lineItemTypeCode
# MAGIC ,billingClassCode
# MAGIC ,industryText
# MAGIC ,subtransactionLineItemCode
# MAGIC ,rateCategoryCode
# MAGIC ,statisticalAnalysisRateType
# MAGIC ,rateId
# MAGIC ,rateFactGroupCode
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
# MAGIC ,priceAmount
# MAGIC ,lineItemType
# MAGIC ,printDocumentId
# MAGIC ,printDocumentLineItemId
# MAGIC ,billingDocumentCreateDate
# MAGIC ,installationId
# MAGIC ,cityCode
# MAGIC ,streetCode
# MAGIC ,countryShortName
# MAGIC ,stateCode
# MAGIC ,politicalRegionCode
# MAGIC ,serviceProvider
# MAGIC ,salesEmployee
# MAGIC ,internalPointDeliveryKeyBW
# MAGIC ,taxSalesCode
# MAGIC ,taxBaseAmount
# MAGIC ,taxAmount
# MAGIC ,fiscalYear
# MAGIC ,fiscalYearVariant
# MAGIC from(SELECT
# MAGIC BUDAT as postingDate
# MAGIC ,CPUDT as documentEnteredDate
# MAGIC ,FIKEY as reconciliationKeyForGeneralLedger
# MAGIC ,XCANC as isReversalTransaction
# MAGIC ,BELNR as billingDocumentNumber
# MAGIC ,BUKRS as companyCode
# MAGIC ,SPARTE as divisonCode
# MAGIC ,GPARTNER as businessPartnerNumber
# MAGIC ,VKONT as contractAccountNumber
# MAGIC ,VERTRAG as contractId
# MAGIC ,ABWVK as alternativeContractAccountForCollectiveBills
# MAGIC ,ABRVORG as billingTransactionCode
# MAGIC ,APPLK as applicationArea
# MAGIC ,HVORG as mainTransactionLineItemCode
# MAGIC ,KOFIZ as accountDeterminationCode
# MAGIC ,PORTION as portionNumber
# MAGIC ,ABLEINH as meterReadingUnit
# MAGIC ,ZUORDDAA as billingAllocationDate
# MAGIC ,ANZTAGE as numberOfContractDaysBilled
# MAGIC ,ANZVERTR as numberOfBilledContracts
# MAGIC ,CNTINVDOC as numberofInvoicingDocuments
# MAGIC ,CNTBILLDOC as numberOfBillingDocuments
# MAGIC ,BELZEILE as billingDocumentLineItemID
# MAGIC ,BELZART as lineItemTypeCode
# MAGIC ,AKLASSE as billingClassCode
# MAGIC ,BRANCHE as industryText
# MAGIC ,TVORG as subtransactionLineItemCode
# MAGIC ,TARIFTYP as rateCategoryCode
# MAGIC ,STATTART as statisticalAnalysisRateType
# MAGIC ,TARIFNR as rateId
# MAGIC ,KONDIGR as rateFactGroupCode
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
# MAGIC ,PREISBTR as priceAmount
# MAGIC ,BELZART_NAME as lineItemType
# MAGIC ,PRINTDOC as printDocumentId
# MAGIC ,PRINTDOCLINE as printDocumentLineItemId
# MAGIC ,BELEGDAT as billingDocumentCreateDate
# MAGIC ,ANLAGE as installationId
# MAGIC ,CITY_CODE as cityCode
# MAGIC ,STREETCODE as streetCode
# MAGIC ,COUNTRY as countryShortName
# MAGIC ,REGION as stateCode
# MAGIC ,REGPOLIT as politicalRegionCode
# MAGIC ,SERVICEID as serviceProvider
# MAGIC ,SALESEMPLOYEE as salesEmployee
# MAGIC ,INT_UI_BW as internalPointDeliveryKeyBW
# MAGIC ,MWSKZ as taxSalesCode
# MAGIC ,SBASW as taxBaseAmount
# MAGIC ,SBETW as taxAmount
# MAGIC ,PERIOD as fiscalYear
# MAGIC ,PERIV as fiscalYearVariant
# MAGIC ,row_number() over (partition by billingDocumentNumber, billingDocumentLineItemID order by EXTRACT_DATETIME desc) as rn
# MAGIC from source) a where a.rn = 1

# COMMAND ----------

0UC_SALES_STATS_03lakedf = spark.sql("select * from cleansed.t_sapisu_0UC_DEVICEH_ATTR")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Verification] count
0UC_SALES_STATS_03%sql
select count (*) as RecordCount, 'Target' as TableName from cleansed.t_sapisu_0UC_SALES_STATS_03
union all
select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Duplicate checks]
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY billingDocumentNumber, billingDocumentLineItemID order by validFromDate) as rn
# MAGIC FROM  cleansed.t_sapisu_0UC_SALES_STATS_03
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC postingDate
# MAGIC ,documentEnteredDate
# MAGIC ,reconciliationKeyForGeneralLedger
# MAGIC ,isReversalTransaction
# MAGIC ,billingDocumentNumber
# MAGIC ,companyCode
# MAGIC ,divisonCode
# MAGIC ,businessPartnerNumber
# MAGIC ,contractAccountNumber
# MAGIC ,contractId
# MAGIC ,alternativeContractAccountForCollectiveBills
# MAGIC ,billingTransactionCode
# MAGIC ,applicationArea
# MAGIC ,mainTransactionLineItemCode
# MAGIC ,accountDeterminationCode
# MAGIC ,portionNumber
# MAGIC ,meterReadingUnit
# MAGIC ,billingAllocationDate
# MAGIC ,numberOfContractDaysBilled
# MAGIC ,numberOfBilledContracts
# MAGIC ,numberofInvoicingDocuments
# MAGIC ,numberOfBillingDocuments
# MAGIC ,billingDocumentLineItemID
# MAGIC ,lineItemTypeCode
# MAGIC ,billingClassCode
# MAGIC ,industryText
# MAGIC ,subtransactionLineItemCode
# MAGIC ,rateCategoryCode
# MAGIC ,statisticalAnalysisRateType
# MAGIC ,rateId
# MAGIC ,rateFactGroupCode
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
# MAGIC ,priceAmount
# MAGIC ,lineItemType
# MAGIC ,printDocumentId
# MAGIC ,printDocumentLineItemId
# MAGIC ,billingDocumentCreateDate
# MAGIC ,installationId
# MAGIC ,cityCode
# MAGIC ,streetCode
# MAGIC ,countryShortName
# MAGIC ,stateCode
# MAGIC ,politicalRegionCode
# MAGIC ,serviceProvider
# MAGIC ,salesEmployee
# MAGIC ,internalPointDeliveryKeyBW
# MAGIC ,taxSalesCode
# MAGIC ,taxBaseAmount
# MAGIC ,taxAmount
# MAGIC ,fiscalYear
# MAGIC ,fiscalYearVariant
# MAGIC from(SELECT
# MAGIC BUDAT as postingDate
# MAGIC ,CPUDT as documentEnteredDate
# MAGIC ,FIKEY as reconciliationKeyForGeneralLedger
# MAGIC ,XCANC as isReversalTransaction
# MAGIC ,BELNR as billingDocumentNumber
# MAGIC ,BUKRS as companyCode
# MAGIC ,SPARTE as divisonCode
# MAGIC ,GPARTNER as businessPartnerNumber
# MAGIC ,VKONT as contractAccountNumber
# MAGIC ,VERTRAG as contractId
# MAGIC ,ABWVK as alternativeContractAccountForCollectiveBills
# MAGIC ,ABRVORG as billingTransactionCode
# MAGIC ,APPLK as applicationArea
# MAGIC ,HVORG as mainTransactionLineItemCode
# MAGIC ,KOFIZ as accountDeterminationCode
# MAGIC ,PORTION as portionNumber
# MAGIC ,ABLEINH as meterReadingUnit
# MAGIC ,ZUORDDAA as billingAllocationDate
# MAGIC ,ANZTAGE as numberOfContractDaysBilled
# MAGIC ,ANZVERTR as numberOfBilledContracts
# MAGIC ,CNTINVDOC as numberofInvoicingDocuments
# MAGIC ,CNTBILLDOC as numberOfBillingDocuments
# MAGIC ,BELZEILE as billingDocumentLineItemID
# MAGIC ,BELZART as lineItemTypeCode
# MAGIC ,AKLASSE as billingClassCode
# MAGIC ,BRANCHE as industryText
# MAGIC ,TVORG as subtransactionLineItemCode
# MAGIC ,TARIFTYP as rateCategoryCode
# MAGIC ,STATTART as statisticalAnalysisRateType
# MAGIC ,TARIFNR as rateId
# MAGIC ,KONDIGR as rateFactGroupCode
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
# MAGIC ,PREISBTR as priceAmount
# MAGIC ,BELZART_NAME as lineItemType
# MAGIC ,PRINTDOC as printDocumentId
# MAGIC ,PRINTDOCLINE as printDocumentLineItemId
# MAGIC ,BELEGDAT as billingDocumentCreateDate
# MAGIC ,ANLAGE as installationId
# MAGIC ,CITY_CODE as cityCode
# MAGIC ,STREETCODE as streetCode
# MAGIC ,COUNTRY as countryShortName
# MAGIC ,REGION as stateCode
# MAGIC ,REGPOLIT as politicalRegionCode
# MAGIC ,SERVICEID as serviceProvider
# MAGIC ,SALESEMPLOYEE as salesEmployee
# MAGIC ,INT_UI_BW as internalPointDeliveryKeyBW
# MAGIC ,MWSKZ as taxSalesCode
# MAGIC ,SBASW as taxBaseAmount
# MAGIC ,SBETW as taxAmount
# MAGIC ,PERIOD as fiscalYear
# MAGIC ,PERIV as fiscalYearVariant
# MAGIC ,row_number() over (partition by billingDocumentNumber, billingDocumentLineItemID order by EXTRACT_DATETIME desc) as rn
# MAGIC from source) a where a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC postingDate
# MAGIC ,documentEnteredDate
# MAGIC ,reconciliationKeyForGeneralLedger
# MAGIC ,isReversalTransaction
# MAGIC ,billingDocumentNumber
# MAGIC ,companyCode
# MAGIC ,divisonCode
# MAGIC ,businessPartnerNumber
# MAGIC ,contractAccountNumber
# MAGIC ,contractId
# MAGIC ,alternativeContractAccountForCollectiveBills
# MAGIC ,billingTransactionCode
# MAGIC ,applicationArea
# MAGIC ,mainTransactionLineItemCode
# MAGIC ,accountDeterminationCode
# MAGIC ,portionNumber
# MAGIC ,meterReadingUnit
# MAGIC ,billingAllocationDate
# MAGIC ,numberOfContractDaysBilled
# MAGIC ,numberOfBilledContracts
# MAGIC ,numberofInvoicingDocuments
# MAGIC ,numberOfBillingDocuments
# MAGIC ,billingDocumentLineItemID
# MAGIC ,lineItemTypeCode
# MAGIC ,billingClassCode
# MAGIC ,industryText
# MAGIC ,subtransactionLineItemCode
# MAGIC ,rateCategoryCode
# MAGIC ,statisticalAnalysisRateType
# MAGIC ,rateId
# MAGIC ,rateFactGroupCode
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
# MAGIC ,priceAmount
# MAGIC ,lineItemType
# MAGIC ,printDocumentId
# MAGIC ,printDocumentLineItemId
# MAGIC ,billingDocumentCreateDate
# MAGIC ,installationId
# MAGIC ,cityCode
# MAGIC ,streetCode
# MAGIC ,countryShortName
# MAGIC ,stateCode
# MAGIC ,politicalRegionCode
# MAGIC ,serviceProvider
# MAGIC ,salesEmployee
# MAGIC ,internalPointDeliveryKeyBW
# MAGIC ,taxSalesCode
# MAGIC ,taxBaseAmount
# MAGIC ,taxAmount
# MAGIC ,fiscalYear
# MAGIC ,fiscalYearVariant
# MAGIC from
# MAGIC cleansed.t_sapisu_0UC_SALES_STATS_03

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC postingDate
# MAGIC ,documentEnteredDate
# MAGIC ,reconciliationKeyForGeneralLedger
# MAGIC ,isReversalTransaction
# MAGIC ,billingDocumentNumber
# MAGIC ,companyCode
# MAGIC ,divisonCode
# MAGIC ,businessPartnerNumber
# MAGIC ,contractAccountNumber
# MAGIC ,contractId
# MAGIC ,alternativeContractAccountForCollectiveBills
# MAGIC ,billingTransactionCode
# MAGIC ,applicationArea
# MAGIC ,mainTransactionLineItemCode
# MAGIC ,accountDeterminationCode
# MAGIC ,portionNumber
# MAGIC ,meterReadingUnit
# MAGIC ,billingAllocationDate
# MAGIC ,numberOfContractDaysBilled
# MAGIC ,numberOfBilledContracts
# MAGIC ,numberofInvoicingDocuments
# MAGIC ,numberOfBillingDocuments
# MAGIC ,billingDocumentLineItemID
# MAGIC ,lineItemTypeCode
# MAGIC ,billingClassCode
# MAGIC ,industryText
# MAGIC ,subtransactionLineItemCode
# MAGIC ,rateCategoryCode
# MAGIC ,statisticalAnalysisRateType
# MAGIC ,rateId
# MAGIC ,rateFactGroupCode
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
# MAGIC ,priceAmount
# MAGIC ,lineItemType
# MAGIC ,printDocumentId
# MAGIC ,printDocumentLineItemId
# MAGIC ,billingDocumentCreateDate
# MAGIC ,installationId
# MAGIC ,cityCode
# MAGIC ,streetCode
# MAGIC ,countryShortName
# MAGIC ,stateCode
# MAGIC ,politicalRegionCode
# MAGIC ,serviceProvider
# MAGIC ,salesEmployee
# MAGIC ,internalPointDeliveryKeyBW
# MAGIC ,taxSalesCode
# MAGIC ,taxBaseAmount
# MAGIC ,taxAmount
# MAGIC ,fiscalYear
# MAGIC ,fiscalYearVariant
# MAGIC from
# MAGIC cleansed.t_sapisu_0UC_SALES_STATS_03
# MAGIC except
# MAGIC select
# MAGIC postingDate
# MAGIC ,documentEnteredDate
# MAGIC ,reconciliationKeyForGeneralLedger
# MAGIC ,isReversalTransaction
# MAGIC ,billingDocumentNumber
# MAGIC ,companyCode
# MAGIC ,divisonCode
# MAGIC ,businessPartnerNumber
# MAGIC ,contractAccountNumber
# MAGIC ,contractId
# MAGIC ,alternativeContractAccountForCollectiveBills
# MAGIC ,billingTransactionCode
# MAGIC ,applicationArea
# MAGIC ,mainTransactionLineItemCode
# MAGIC ,accountDeterminationCode
# MAGIC ,portionNumber
# MAGIC ,meterReadingUnit
# MAGIC ,billingAllocationDate
# MAGIC ,numberOfContractDaysBilled
# MAGIC ,numberOfBilledContracts
# MAGIC ,numberofInvoicingDocuments
# MAGIC ,numberOfBillingDocuments
# MAGIC ,billingDocumentLineItemID
# MAGIC ,lineItemTypeCode
# MAGIC ,billingClassCode
# MAGIC ,industryText
# MAGIC ,subtransactionLineItemCode
# MAGIC ,rateCategoryCode
# MAGIC ,statisticalAnalysisRateType
# MAGIC ,rateId
# MAGIC ,rateFactGroupCode
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
# MAGIC ,priceAmount
# MAGIC ,lineItemType
# MAGIC ,printDocumentId
# MAGIC ,printDocumentLineItemId
# MAGIC ,billingDocumentCreateDate
# MAGIC ,installationId
# MAGIC ,cityCode
# MAGIC ,streetCode
# MAGIC ,countryShortName
# MAGIC ,stateCode
# MAGIC ,politicalRegionCode
# MAGIC ,serviceProvider
# MAGIC ,salesEmployee
# MAGIC ,internalPointDeliveryKeyBW
# MAGIC ,taxSalesCode
# MAGIC ,taxBaseAmount
# MAGIC ,taxAmount
# MAGIC ,fiscalYear
# MAGIC ,fiscalYearVariant
# MAGIC from(SELECT
# MAGIC BUDAT as postingDate
# MAGIC ,CPUDT as documentEnteredDate
# MAGIC ,FIKEY as reconciliationKeyForGeneralLedger
# MAGIC ,XCANC as isReversalTransaction
# MAGIC ,BELNR as billingDocumentNumber
# MAGIC ,BUKRS as companyCode
# MAGIC ,SPARTE as divisonCode
# MAGIC ,GPARTNER as businessPartnerNumber
# MAGIC ,VKONT as contractAccountNumber
# MAGIC ,VERTRAG as contractId
# MAGIC ,ABWVK as alternativeContractAccountForCollectiveBills
# MAGIC ,ABRVORG as billingTransactionCode
# MAGIC ,APPLK as applicationArea
# MAGIC ,HVORG as mainTransactionLineItemCode
# MAGIC ,KOFIZ as accountDeterminationCode
# MAGIC ,PORTION as portionNumber
# MAGIC ,ABLEINH as meterReadingUnit
# MAGIC ,ZUORDDAA as billingAllocationDate
# MAGIC ,ANZTAGE as numberOfContractDaysBilled
# MAGIC ,ANZVERTR as numberOfBilledContracts
# MAGIC ,CNTINVDOC as numberofInvoicingDocuments
# MAGIC ,CNTBILLDOC as numberOfBillingDocuments
# MAGIC ,BELZEILE as billingDocumentLineItemID
# MAGIC ,BELZART as lineItemTypeCode
# MAGIC ,AKLASSE as billingClassCode
# MAGIC ,BRANCHE as industryText
# MAGIC ,TVORG as subtransactionLineItemCode
# MAGIC ,TARIFTYP as rateCategoryCode
# MAGIC ,STATTART as statisticalAnalysisRateType
# MAGIC ,TARIFNR as rateId
# MAGIC ,KONDIGR as rateFactGroupCode
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
# MAGIC ,PREISBTR as priceAmount
# MAGIC ,BELZART_NAME as lineItemType
# MAGIC ,PRINTDOC as printDocumentId
# MAGIC ,PRINTDOCLINE as printDocumentLineItemId
# MAGIC ,BELEGDAT as billingDocumentCreateDate
# MAGIC ,ANLAGE as installationId
# MAGIC ,CITY_CODE as cityCode
# MAGIC ,STREETCODE as streetCode
# MAGIC ,COUNTRY as countryShortName
# MAGIC ,REGION as stateCode
# MAGIC ,REGPOLIT as politicalRegionCode
# MAGIC ,SERVICEID as serviceProvider
# MAGIC ,SALESEMPLOYEE as salesEmployee
# MAGIC ,INT_UI_BW as internalPointDeliveryKeyBW
# MAGIC ,MWSKZ as taxSalesCode
# MAGIC ,SBASW as taxBaseAmount
# MAGIC ,SBETW as taxAmount
# MAGIC ,PERIOD as fiscalYear
# MAGIC ,PERIV as fiscalYearVariant
# MAGIC ,row_number() over (partition by billingDocumentNumber, billingDocumentLineItemID order by EXTRACT_DATETIME desc) as rn
# MAGIC from source) a where a.rn = 1
