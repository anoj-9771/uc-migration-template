# Databricks notebook source
#config parameters
source = 'CRM' #either CRM or ISU
table = 'UTILITIESCONTRACT'

environment = 'test'
storage_account_name = "sablobdaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sablob-key")
containerName = "archive"


# COMMAND ----------

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

# MAGIC %run ../../includes/tableEvaluation

# COMMAND ----------

lakedf = spark.sql("select * from cleansed.${vars.table}")

# COMMAND ----------

lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC select
# MAGIC sapClient
# MAGIC ,ItemUUID
# MAGIC ,podUUID
# MAGIC ,headerUUID
# MAGIC ,utilitiesContract
# MAGIC ,businessPartnerGroupNumber
# MAGIC ,businessPartnerGroupName
# MAGIC ,businessAgreement
# MAGIC ,businessAgreementUUID
# MAGIC ,incomingPaymentMethod
# MAGIC ,incomingPaymentMethodName
# MAGIC ,paymentTerms
# MAGIC ,paymentTermsName
# MAGIC ,soldToParty
# MAGIC ,businessPartnerFullName
# MAGIC ,divisionCode
# MAGIC ,division
# MAGIC ,contractStartDate
# MAGIC ,contractEndDate
# MAGIC ,creationDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,changedBy
# MAGIC ,itemCategory
# MAGIC ,itemCategoryName
# MAGIC ,product
# MAGIC ,productDescription
# MAGIC ,itemType
# MAGIC ,itemTypeName
# MAGIC ,productType
# MAGIC ,headerType
# MAGIC ,headerTypeName
# MAGIC ,headerCategory
# MAGIC ,headerCategoryName
# MAGIC ,headerDescription
# MAGIC ,isDeregulationPod
# MAGIC ,premise
# MAGIC ,numberOfPersons
# MAGIC ,cityName
# MAGIC ,streetName
# MAGIC ,houseNumber
# MAGIC ,postalCode
# MAGIC ,building
# MAGIC ,addressTimeZone
# MAGIC ,countryName
# MAGIC ,regionName
# MAGIC ,numberOfContractChanges
# MAGIC ,isOpen
# MAGIC ,isDistributed
# MAGIC ,hasError
# MAGIC ,isToBeDistributed
# MAGIC ,isIncomplete
# MAGIC ,isStartedDueToProductChange
# MAGIC ,isInActivation
# MAGIC ,isInDeactivation
# MAGIC ,isCancelled
# MAGIC ,cancellationMessageIsCreated
# MAGIC ,supplyEndCanclInMsgIsCreated
# MAGIC ,activationIsRejected
# MAGIC ,deactivationIsRejected
# MAGIC ,numberOfContracts
# MAGIC ,numberOfActiveContracts
# MAGIC ,numberOfIncompleteContracts
# MAGIC ,numberOfDistributedContracts
# MAGIC ,numberOfContractsToBeDistributed
# MAGIC ,numberOfBlockedContracts
# MAGIC ,numberOfCancelledContracts
# MAGIC ,numberOfProductChanges
# MAGIC ,numberOfContractsWProductChanges
# MAGIC ,numberOfContrWthEndOfSupRjctd
# MAGIC ,numberOfContrWthStartOfSupRjctd
# MAGIC ,numberOfContrWaitingForEndOfSup
# MAGIC ,numberOfContrWaitingForStrtOfSup
# MAGIC ,creationDateE
# MAGIC ,lastChangedDateE
# MAGIC ,contractStartDateE
# MAGIC ,contractEndDateE
# MAGIC from
# MAGIC (select
# MAGIC SAPClient as sapClient
# MAGIC ,ItemUUID as ItemUUID
# MAGIC ,PodUUID as podUUID
# MAGIC ,HeaderUUID as headerUUID
# MAGIC ,UtilitiesContract as utilitiesContract
# MAGIC ,BusinessPartner as businessPartnerGroupNumber
# MAGIC ,BusinessPartnerFullName as businessPartnerGroupName
# MAGIC ,BusinessAgreement as businessAgreement
# MAGIC ,BusinessAgreementUUID as businessAgreementUUID
# MAGIC ,IncomingPaymentMethod as incomingPaymentMethod
# MAGIC ,IncomingPaymentMethodName as incomingPaymentMethodName
# MAGIC ,PaymentTerms as paymentTerms
# MAGIC ,PaymentTermsName as paymentTermsName
# MAGIC ,SoldToParty as soldToParty
# MAGIC ,SoldToPartyName as businessPartnerFullName
# MAGIC ,Division as divisionCode
# MAGIC ,DivisionName as division
# MAGIC ,ContractStartDate as contractStartDate
# MAGIC ,ContractEndDate as contractEndDate
# MAGIC ,CreationDate as creationDate
# MAGIC ,CreatedByUser as createdBy
# MAGIC ,LastChangeDate as lastChangedDate
# MAGIC ,LastChangedByUser as changedBy
# MAGIC ,ItemCategory as itemCategory
# MAGIC ,ItemCategoryName as itemCategoryName
# MAGIC ,Product as product
# MAGIC ,ProductDescription as productDescription
# MAGIC ,ItemType as itemType
# MAGIC ,ItemTypeName as itemTypeName
# MAGIC ,ProductType as productType
# MAGIC ,HeaderType as headerType
# MAGIC ,HeaderTypeName as headerTypeName
# MAGIC ,HeaderCategory as headerCategory
# MAGIC ,HeaderCategoryName as headerCategoryName
# MAGIC ,HeaderDescription as headerDescription
# MAGIC ,IsDeregulationPod as isDeregulationPod
# MAGIC ,UtilitiesPremise as premise
# MAGIC ,NumberOfPersons as numberOfPersons
# MAGIC ,CityName as cityName
# MAGIC ,StreetName as streetName
# MAGIC ,HouseNumber as houseNumber
# MAGIC ,PostalCode as postalCode
# MAGIC ,Building as building
# MAGIC ,AddressTimeZone as addressTimeZone
# MAGIC ,CountryName as countryName
# MAGIC ,RegionName as regionName
# MAGIC ,NumberOfContractChanges as numberOfContractChanges
# MAGIC ,IsOpen as isOpen
# MAGIC ,IsDistributed as isDistributed
# MAGIC ,HasError as hasError
# MAGIC ,IsToBeDistributed as isToBeDistributed
# MAGIC ,IsIncomplete as isIncomplete
# MAGIC ,IsStartedDueToProductChange as isStartedDueToProductChange
# MAGIC ,IsInActivation as isInActivation
# MAGIC ,IsInDeactivation as isInDeactivation
# MAGIC ,IsCancelled as isCancelled
# MAGIC ,CancellationMessageIsCreated as cancellationMessageIsCreated
# MAGIC ,SupplyEndCanclnMsgIsCreated as supplyEndCanclInMsgIsCreated
# MAGIC ,ActivationIsRejected as activationIsRejected
# MAGIC ,DeactivationIsRejected as deactivationIsRejected
# MAGIC ,NumberOfContracts as numberOfContracts
# MAGIC ,NumberOfActiveContracts as numberOfActiveContracts
# MAGIC ,NumberOfIncompleteContracts as numberOfIncompleteContracts
# MAGIC ,NumberOfDistributedContracts as numberOfDistributedContracts
# MAGIC ,NmbrOfContractsToBeDistributed as numberOfContractsToBeDistributed
# MAGIC ,NumberOfBlockedContracts as numberOfBlockedContracts
# MAGIC ,NumberOfCancelledContracts as numberOfCancelledContracts
# MAGIC ,NumberOfProductChanges as numberOfProductChanges
# MAGIC ,NmbrOfContractsWProductChanges as numberOfContractsWProductChanges
# MAGIC ,NmbrOfContrWthEndOfSupRjctd as numberOfContrWthEndOfSupRjctd
# MAGIC ,NmbrOfContrWthStartOfSupRjctd as numberOfContrWthStartOfSupRjctd
# MAGIC ,NmbrOfContrWaitingForEndOfSup as numberOfContrWaitingForEndOfSup
# MAGIC ,NmbrOfContrWaitingForStrtOfSup as numberOfContrWaitingForStrtOfSup
# MAGIC ,CreationDate_E as creationDateE
# MAGIC ,LastChangeDate_E as lastChangedDateE
# MAGIC ,ContractStartDate_E as contractStartDateE
# MAGIC ,ContractEndDate_E as contractEndDateE
# MAGIC 
# MAGIC ,row_number() over (partition by UtilitiesContract,ContractEndDate_E order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC 
# MAGIC --and f.DDLANGUAGE ='E' 
# MAGIC )a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC sapClient
# MAGIC ,ItemUUID
# MAGIC ,podUUID
# MAGIC ,headerUUID
# MAGIC ,utilitiesContract
# MAGIC ,businessPartnerGroupNumber
# MAGIC ,businessPartnerGroupName
# MAGIC ,businessAgreement
# MAGIC ,businessAgreementUUID
# MAGIC ,incomingPaymentMethod
# MAGIC ,incomingPaymentMethodName
# MAGIC ,paymentTerms
# MAGIC ,paymentTermsName
# MAGIC ,soldToParty
# MAGIC ,businessPartnerFullName
# MAGIC ,divisionCode
# MAGIC ,division
# MAGIC ,contractStartDate
# MAGIC ,contractEndDate
# MAGIC ,creationDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,changedBy
# MAGIC ,itemCategory
# MAGIC ,itemCategoryName
# MAGIC ,product
# MAGIC ,productDescription
# MAGIC ,itemType
# MAGIC ,itemTypeName
# MAGIC ,productType
# MAGIC ,headerType
# MAGIC ,headerTypeName
# MAGIC ,headerCategory
# MAGIC ,headerCategoryName
# MAGIC ,headerDescription
# MAGIC ,isDeregulationPod
# MAGIC ,premise
# MAGIC ,numberOfPersons
# MAGIC ,cityName
# MAGIC ,streetName
# MAGIC ,houseNumber
# MAGIC ,postalCode
# MAGIC ,building
# MAGIC ,addressTimeZone
# MAGIC ,countryName
# MAGIC ,regionName
# MAGIC ,numberOfContractChanges
# MAGIC ,isOpen
# MAGIC ,isDistributed
# MAGIC ,hasError
# MAGIC ,isToBeDistributed
# MAGIC ,isIncomplete
# MAGIC ,isStartedDueToProductChange
# MAGIC ,isInActivation
# MAGIC ,isInDeactivation
# MAGIC ,isCancelled
# MAGIC ,cancellationMessageIsCreated
# MAGIC ,supplyEndCanclInMsgIsCreated
# MAGIC ,activationIsRejected
# MAGIC ,deactivationIsRejected
# MAGIC ,numberOfContracts
# MAGIC ,numberOfActiveContracts
# MAGIC ,numberOfIncompleteContracts
# MAGIC ,numberOfDistributedContracts
# MAGIC ,numberOfContractsToBeDistributed
# MAGIC ,numberOfBlockedContracts
# MAGIC ,numberOfCancelledContracts
# MAGIC ,numberOfProductChanges
# MAGIC ,numberOfContractsWProductChanges
# MAGIC ,numberOfContrWthEndOfSupRjctd
# MAGIC ,numberOfContrWthStartOfSupRjctd
# MAGIC ,numberOfContrWaitingForEndOfSup
# MAGIC ,numberOfContrWaitingForStrtOfSup
# MAGIC ,creationDateE
# MAGIC ,lastChangedDateE
# MAGIC ,contractStartDateE
# MAGIC ,contractEndDateE
# MAGIC from
# MAGIC (select
# MAGIC SAPClient as sapClient
# MAGIC ,ItemUUID as ItemUUID
# MAGIC ,PodUUID as podUUID
# MAGIC ,HeaderUUID as headerUUID
# MAGIC ,UtilitiesContract as utilitiesContract
# MAGIC ,BusinessPartner as businessPartnerGroupNumber
# MAGIC ,BusinessPartnerFullName as businessPartnerGroupName
# MAGIC ,BusinessAgreement as businessAgreement
# MAGIC ,BusinessAgreementUUID as businessAgreementUUID
# MAGIC ,IncomingPaymentMethod as incomingPaymentMethod
# MAGIC ,IncomingPaymentMethodName as incomingPaymentMethodName
# MAGIC ,PaymentTerms as paymentTerms
# MAGIC ,PaymentTermsName as paymentTermsName
# MAGIC ,SoldToParty as soldToParty
# MAGIC ,SoldToPartyName as businessPartnerFullName
# MAGIC ,Division as divisionCode
# MAGIC ,DivisionName as division
# MAGIC ,ContractStartDate as contractStartDate
# MAGIC ,ContractEndDate as contractEndDate
# MAGIC ,CreationDate as creationDate
# MAGIC ,CreatedByUser as createdBy
# MAGIC ,LastChangeDate as lastChangedDate
# MAGIC ,LastChangedByUser as changedBy
# MAGIC ,ItemCategory as itemCategory
# MAGIC ,ItemCategoryName as itemCategoryName
# MAGIC ,Product as product
# MAGIC ,ProductDescription as productDescription
# MAGIC ,ItemType as itemType
# MAGIC ,ItemTypeName as itemTypeName
# MAGIC ,ProductType as productType
# MAGIC ,HeaderType as headerType
# MAGIC ,HeaderTypeName as headerTypeName
# MAGIC ,HeaderCategory as headerCategory
# MAGIC ,HeaderCategoryName as headerCategoryName
# MAGIC ,HeaderDescription as headerDescription
# MAGIC ,IsDeregulationPod as isDeregulationPod
# MAGIC ,UtilitiesPremise as premise
# MAGIC ,NumberOfPersons as numberOfPersons
# MAGIC ,CityName as cityName
# MAGIC ,StreetName as streetName
# MAGIC ,HouseNumber as houseNumber
# MAGIC ,PostalCode as postalCode
# MAGIC ,Building as building
# MAGIC ,AddressTimeZone as addressTimeZone
# MAGIC ,CountryName as countryName
# MAGIC ,RegionName as regionName
# MAGIC ,NumberOfContractChanges as numberOfContractChanges
# MAGIC ,IsOpen as isOpen
# MAGIC ,IsDistributed as isDistributed
# MAGIC ,HasError as hasError
# MAGIC ,IsToBeDistributed as isToBeDistributed
# MAGIC ,IsIncomplete as isIncomplete
# MAGIC ,IsStartedDueToProductChange as isStartedDueToProductChange
# MAGIC ,IsInActivation as isInActivation
# MAGIC ,IsInDeactivation as isInDeactivation
# MAGIC ,IsCancelled as isCancelled
# MAGIC ,CancellationMessageIsCreated as cancellationMessageIsCreated
# MAGIC ,SupplyEndCanclnMsgIsCreated as supplyEndCanclInMsgIsCreated
# MAGIC ,ActivationIsRejected as activationIsRejected
# MAGIC ,DeactivationIsRejected as deactivationIsRejected
# MAGIC ,NumberOfContracts as numberOfContracts
# MAGIC ,NumberOfActiveContracts as numberOfActiveContracts
# MAGIC ,NumberOfIncompleteContracts as numberOfIncompleteContracts
# MAGIC ,NumberOfDistributedContracts as numberOfDistributedContracts
# MAGIC ,NmbrOfContractsToBeDistributed as numberOfContractsToBeDistributed
# MAGIC ,NumberOfBlockedContracts as numberOfBlockedContracts
# MAGIC ,NumberOfCancelledContracts as numberOfCancelledContracts
# MAGIC ,NumberOfProductChanges as numberOfProductChanges
# MAGIC ,NmbrOfContractsWProductChanges as numberOfContractsWProductChanges
# MAGIC ,NmbrOfContrWthEndOfSupRjctd as numberOfContrWthEndOfSupRjctd
# MAGIC ,NmbrOfContrWthStartOfSupRjctd as numberOfContrWthStartOfSupRjctd
# MAGIC ,NmbrOfContrWaitingForEndOfSup as numberOfContrWaitingForEndOfSup
# MAGIC ,NmbrOfContrWaitingForStrtOfSup as numberOfContrWaitingForStrtOfSup
# MAGIC ,CreationDate_E as creationDateE
# MAGIC ,LastChangeDate_E as lastChangedDateE
# MAGIC ,ContractStartDate_E as contractStartDateE
# MAGIC ,ContractEndDate_E as contractEndDateE
# MAGIC 
# MAGIC ,row_number() over (partition by UtilitiesContract,ContractEndDate_E order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC 
# MAGIC --and f.DDLANGUAGE ='E' 
# MAGIC )a where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT utilitiesContract,contractEndDateE
# MAGIC , COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY utilitiesContract,contractEndDateE
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY utilitiesContract,contractEndDateE  order by utilitiesContract,contractEndDateE) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC --sapClient
# MAGIC ItemUUID
# MAGIC ,podUUID
# MAGIC ,headerUUID
# MAGIC ,utilitiesContract
# MAGIC ,businessPartnerGroupNumber
# MAGIC ,businessPartnerGroupName
# MAGIC ,businessAgreement
# MAGIC ,businessAgreementUUID
# MAGIC ,incomingPaymentMethod
# MAGIC ,incomingPaymentMethodName
# MAGIC ,paymentTerms
# MAGIC ,paymentTermsName
# MAGIC ,soldToParty
# MAGIC ,businessPartnerFullName
# MAGIC ,divisionCode
# MAGIC ,division
# MAGIC ,contractStartDate
# MAGIC ,contractEndDate
# MAGIC --,creationDate
# MAGIC ,createdBy
# MAGIC --,lastChangedDate
# MAGIC ,changedBy
# MAGIC ,itemCategory
# MAGIC ,itemCategoryName
# MAGIC ,product
# MAGIC ,productDescription
# MAGIC ,itemType
# MAGIC ,itemTypeName
# MAGIC ,productType
# MAGIC ,headerType
# MAGIC ,headerTypeName
# MAGIC ,headerCategory
# MAGIC ,headerCategoryName
# MAGIC ,headerDescription
# MAGIC ,isDeregulationPod
# MAGIC ,premise
# MAGIC ,numberOfPersons
# MAGIC ,cityName
# MAGIC ,streetName
# MAGIC ,houseNumber
# MAGIC ,postalCode
# MAGIC ,building
# MAGIC ,addressTimeZone
# MAGIC ,countryName
# MAGIC ,regionName
# MAGIC ,numberOfContractChanges
# MAGIC ,isOpen
# MAGIC ,isDistributed
# MAGIC ,hasError
# MAGIC ,isToBeDistributed
# MAGIC ,isIncomplete
# MAGIC ,isStartedDueToProductChange
# MAGIC ,isInActivation
# MAGIC ,isInDeactivation
# MAGIC ,isCancelled
# MAGIC ,cancellationMessageIsCreated
# MAGIC ,supplyEndCanclInMsgIsCreated
# MAGIC ,activationIsRejected
# MAGIC ,deactivationIsRejected
# MAGIC ,numberOfContracts
# MAGIC ,numberOfActiveContracts
# MAGIC ,numberOfIncompleteContracts
# MAGIC ,numberOfDistributedContracts
# MAGIC ,numberOfContractsToBeDistributed
# MAGIC ,numberOfBlockedContracts
# MAGIC ,numberOfCancelledContracts
# MAGIC ,numberOfProductChanges
# MAGIC ,numberOfContractsWProductChanges
# MAGIC ,numberOfContrWthEndOfSupRjctd
# MAGIC ,numberOfContrWthStartOfSupRjctd
# MAGIC ,numberOfContrWaitingForEndOfSup
# MAGIC ,numberOfContrWaitingForStrtOfSup
# MAGIC ,creationDateE
# MAGIC ,lastChangedDateE
# MAGIC ,contractStartDateE
# MAGIC ,contractEndDateE
# MAGIC from
# MAGIC (select
# MAGIC SAPClient as sapClient
# MAGIC ,ItemUUID as ItemUUID
# MAGIC ,PodUUID as podUUID
# MAGIC ,HeaderUUID as headerUUID
# MAGIC ,UtilitiesContract as utilitiesContract
# MAGIC ,BusinessPartner as businessPartnerGroupNumber
# MAGIC ,BusinessPartnerFullName as businessPartnerGroupName
# MAGIC ,BusinessAgreement as businessAgreement
# MAGIC ,BusinessAgreementUUID as businessAgreementUUID
# MAGIC ,IncomingPaymentMethod as incomingPaymentMethod
# MAGIC ,IncomingPaymentMethodName as incomingPaymentMethodName
# MAGIC ,PaymentTerms as paymentTerms
# MAGIC ,PaymentTermsName as paymentTermsName
# MAGIC ,SoldToParty as soldToParty
# MAGIC ,SoldToPartyName as businessPartnerFullName
# MAGIC ,Division as divisionCode
# MAGIC ,DivisionName as division
# MAGIC ,ContractStartDate as contractStartDate
# MAGIC ,ContractEndDate as contractEndDate
# MAGIC ,CreationDate as creationDate
# MAGIC ,CreatedByUser as createdBy
# MAGIC ,LastChangeDate as lastChangedDate
# MAGIC ,LastChangedByUser as changedBy
# MAGIC ,ItemCategory as itemCategory
# MAGIC ,ItemCategoryName as itemCategoryName
# MAGIC ,Product as product
# MAGIC ,ProductDescription as productDescription
# MAGIC ,ItemType as itemType
# MAGIC ,ItemTypeName as itemTypeName
# MAGIC ,ProductType as productType
# MAGIC ,HeaderType as headerType
# MAGIC ,HeaderTypeName as headerTypeName
# MAGIC ,HeaderCategory as headerCategory
# MAGIC ,HeaderCategoryName as headerCategoryName
# MAGIC ,HeaderDescription as headerDescription
# MAGIC ,IsDeregulationPod as isDeregulationPod
# MAGIC ,UtilitiesPremise as premise
# MAGIC ,NumberOfPersons as numberOfPersons
# MAGIC ,CityName as cityName
# MAGIC ,StreetName as streetName
# MAGIC ,HouseNumber as houseNumber
# MAGIC ,PostalCode as postalCode
# MAGIC ,Building as building
# MAGIC ,AddressTimeZone as addressTimeZone
# MAGIC ,CountryName as countryName
# MAGIC ,RegionName as regionName
# MAGIC ,NumberOfContractChanges as numberOfContractChanges
# MAGIC ,IsOpen as isOpen
# MAGIC ,IsDistributed as isDistributed
# MAGIC ,HasError as hasError
# MAGIC ,IsToBeDistributed as isToBeDistributed
# MAGIC ,IsIncomplete as isIncomplete
# MAGIC ,IsStartedDueToProductChange as isStartedDueToProductChange
# MAGIC ,IsInActivation as isInActivation
# MAGIC ,IsInDeactivation as isInDeactivation
# MAGIC ,IsCancelled as isCancelled
# MAGIC ,CancellationMessageIsCreated as cancellationMessageIsCreated
# MAGIC ,SupplyEndCanclnMsgIsCreated as supplyEndCanclInMsgIsCreated
# MAGIC ,ActivationIsRejected as activationIsRejected
# MAGIC ,DeactivationIsRejected as deactivationIsRejected
# MAGIC ,NumberOfContracts as numberOfContracts
# MAGIC ,NumberOfActiveContracts as numberOfActiveContracts
# MAGIC ,NumberOfIncompleteContracts as numberOfIncompleteContracts
# MAGIC ,NumberOfDistributedContracts as numberOfDistributedContracts
# MAGIC ,NmbrOfContractsToBeDistributed as numberOfContractsToBeDistributed
# MAGIC ,NumberOfBlockedContracts as numberOfBlockedContracts
# MAGIC ,NumberOfCancelledContracts as numberOfCancelledContracts
# MAGIC ,NumberOfProductChanges as numberOfProductChanges
# MAGIC ,NmbrOfContractsWProductChanges as numberOfContractsWProductChanges
# MAGIC ,NmbrOfContrWthEndOfSupRjctd as numberOfContrWthEndOfSupRjctd
# MAGIC ,NmbrOfContrWthStartOfSupRjctd as numberOfContrWthStartOfSupRjctd
# MAGIC ,NmbrOfContrWaitingForEndOfSup as numberOfContrWaitingForEndOfSup
# MAGIC ,NmbrOfContrWaitingForStrtOfSup as numberOfContrWaitingForStrtOfSup
# MAGIC ,CreationDate_E as creationDateE
# MAGIC ,LastChangeDate_E as lastChangedDateE
# MAGIC ,ContractStartDate_E as contractStartDateE
# MAGIC ,ContractEndDate_E as contractEndDateE
# MAGIC 
# MAGIC ,row_number() over (partition by UtilitiesContract,ContractEndDate_E order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC 
# MAGIC --and f.DDLANGUAGE ='E' 
# MAGIC )a where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC --sapClient
# MAGIC ItemUUID
# MAGIC ,podUUID
# MAGIC ,headerUUID
# MAGIC ,utilitiesContract
# MAGIC ,businessPartnerGroupNumber
# MAGIC ,businessPartnerGroupName
# MAGIC ,businessAgreement
# MAGIC ,businessAgreementUUID
# MAGIC ,incomingPaymentMethod
# MAGIC ,incomingPaymentMethodName
# MAGIC ,paymentTerms
# MAGIC ,paymentTermsName
# MAGIC ,soldToParty
# MAGIC ,businessPartnerFullName
# MAGIC ,divisionCode
# MAGIC ,division
# MAGIC ,contractStartDate
# MAGIC ,contractEndDate
# MAGIC --,creationDate
# MAGIC ,createdBy
# MAGIC --,lastChangedDate
# MAGIC ,changedBy
# MAGIC ,itemCategory
# MAGIC ,itemCategoryName
# MAGIC ,product
# MAGIC ,productDescription
# MAGIC ,itemType
# MAGIC ,itemTypeName
# MAGIC ,productType
# MAGIC ,headerType
# MAGIC ,headerTypeName
# MAGIC ,headerCategory
# MAGIC ,headerCategoryName
# MAGIC ,headerDescription
# MAGIC ,isDeregulationPod
# MAGIC ,premise
# MAGIC ,numberOfPersons
# MAGIC ,cityName
# MAGIC ,streetName
# MAGIC ,houseNumber
# MAGIC ,postalCode
# MAGIC ,building
# MAGIC ,addressTimeZone
# MAGIC ,countryName
# MAGIC ,regionName
# MAGIC ,numberOfContractChanges
# MAGIC ,isOpen
# MAGIC ,isDistributed
# MAGIC ,hasError
# MAGIC ,isToBeDistributed
# MAGIC ,isIncomplete
# MAGIC ,isStartedDueToProductChange
# MAGIC ,isInActivation
# MAGIC ,isInDeactivation
# MAGIC ,isCancelled
# MAGIC ,cancellationMessageIsCreated
# MAGIC ,supplyEndCanclInMsgIsCreated
# MAGIC ,activationIsRejected
# MAGIC ,deactivationIsRejected
# MAGIC ,numberOfContracts
# MAGIC ,numberOfActiveContracts
# MAGIC ,numberOfIncompleteContracts
# MAGIC ,numberOfDistributedContracts
# MAGIC ,numberOfContractsToBeDistributed
# MAGIC ,numberOfBlockedContracts
# MAGIC ,numberOfCancelledContracts
# MAGIC ,numberOfProductChanges
# MAGIC ,numberOfContractsWProductChanges
# MAGIC ,numberOfContrWthEndOfSupRjctd
# MAGIC ,numberOfContrWthStartOfSupRjctd
# MAGIC ,numberOfContrWaitingForEndOfSup
# MAGIC ,numberOfContrWaitingForStrtOfSup
# MAGIC ,creationDateE
# MAGIC ,lastChangedDateE
# MAGIC ,contractStartDateE
# MAGIC ,contractEndDateE
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC sapClient
# MAGIC ,ItemUUID
# MAGIC ,podUUID
# MAGIC ,headerUUID
# MAGIC ,utilitiesContract
# MAGIC ,businessPartnerGroupNumber
# MAGIC ,businessPartnerGroupName
# MAGIC ,businessAgreement
# MAGIC ,businessAgreementUUID
# MAGIC ,incomingPaymentMethod
# MAGIC ,incomingPaymentMethodName
# MAGIC ,paymentTerms
# MAGIC ,paymentTermsName
# MAGIC ,soldToParty
# MAGIC ,businessPartnerFullName
# MAGIC ,divisionCode
# MAGIC ,division
# MAGIC ,contractStartDate
# MAGIC ,contractEndDate
# MAGIC ,creationDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,changedBy
# MAGIC ,itemCategory
# MAGIC ,itemCategoryName
# MAGIC ,product
# MAGIC ,productDescription
# MAGIC ,itemType
# MAGIC ,itemTypeName
# MAGIC ,productType
# MAGIC ,headerType
# MAGIC ,headerTypeName
# MAGIC ,headerCategory
# MAGIC ,headerCategoryName
# MAGIC ,headerDescription
# MAGIC ,isDeregulationPod
# MAGIC ,premise
# MAGIC ,numberOfPersons
# MAGIC ,cityName
# MAGIC ,streetName
# MAGIC ,houseNumber
# MAGIC ,postalCode
# MAGIC ,building
# MAGIC ,addressTimeZone
# MAGIC ,countryName
# MAGIC ,regionName
# MAGIC ,numberOfContractChanges
# MAGIC ,isOpen
# MAGIC ,isDistributed
# MAGIC ,hasError
# MAGIC ,isToBeDistributed
# MAGIC ,isIncomplete
# MAGIC ,isStartedDueToProductChange
# MAGIC ,isInActivation
# MAGIC ,isInDeactivation
# MAGIC ,isCancelled
# MAGIC ,cancellationMessageIsCreated
# MAGIC ,supplyEndCanclInMsgIsCreated
# MAGIC ,activationIsRejected
# MAGIC ,deactivationIsRejected
# MAGIC ,numberOfContracts
# MAGIC ,numberOfActiveContracts
# MAGIC ,numberOfIncompleteContracts
# MAGIC ,numberOfDistributedContracts
# MAGIC ,numberOfContractsToBeDistributed
# MAGIC ,numberOfBlockedContracts
# MAGIC ,numberOfCancelledContracts
# MAGIC ,numberOfProductChanges
# MAGIC ,numberOfContractsWProductChanges
# MAGIC ,numberOfContrWthEndOfSupRjctd
# MAGIC ,numberOfContrWthStartOfSupRjctd
# MAGIC ,numberOfContrWaitingForEndOfSup
# MAGIC ,numberOfContrWaitingForStrtOfSup
# MAGIC ,creationDateE
# MAGIC ,lastChangedDateE
# MAGIC ,contractStartDateE
# MAGIC ,contractEndDateE
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC sapClient
# MAGIC ,ItemUUID
# MAGIC ,podUUID
# MAGIC ,headerUUID
# MAGIC ,utilitiesContract
# MAGIC ,businessPartnerGroupNumber
# MAGIC ,businessPartnerGroupName
# MAGIC ,businessAgreement
# MAGIC ,businessAgreementUUID
# MAGIC ,incomingPaymentMethod
# MAGIC ,incomingPaymentMethodName
# MAGIC ,paymentTerms
# MAGIC ,paymentTermsName
# MAGIC ,soldToParty
# MAGIC ,businessPartnerFullName
# MAGIC ,divisionCode
# MAGIC ,division
# MAGIC ,contractStartDate
# MAGIC ,contractEndDate
# MAGIC ,creationDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,changedBy
# MAGIC ,itemCategory
# MAGIC ,itemCategoryName
# MAGIC ,product
# MAGIC ,productDescription
# MAGIC ,itemType
# MAGIC ,itemTypeName
# MAGIC ,productType
# MAGIC ,headerType
# MAGIC ,headerTypeName
# MAGIC ,headerCategory
# MAGIC ,headerCategoryName
# MAGIC ,headerDescription
# MAGIC ,isDeregulationPod
# MAGIC ,premise
# MAGIC ,numberOfPersons
# MAGIC ,cityName
# MAGIC ,streetName
# MAGIC ,houseNumber
# MAGIC ,postalCode
# MAGIC ,building
# MAGIC ,addressTimeZone
# MAGIC ,countryName
# MAGIC ,regionName
# MAGIC ,numberOfContractChanges
# MAGIC ,isOpen
# MAGIC ,isDistributed
# MAGIC ,hasError
# MAGIC ,isToBeDistributed
# MAGIC ,isIncomplete
# MAGIC ,isStartedDueToProductChange
# MAGIC ,isInActivation
# MAGIC ,isInDeactivation
# MAGIC ,isCancelled
# MAGIC ,cancellationMessageIsCreated
# MAGIC ,supplyEndCanclInMsgIsCreated
# MAGIC ,activationIsRejected
# MAGIC ,deactivationIsRejected
# MAGIC ,numberOfContracts
# MAGIC ,numberOfActiveContracts
# MAGIC ,numberOfIncompleteContracts
# MAGIC ,numberOfDistributedContracts
# MAGIC ,numberOfContractsToBeDistributed
# MAGIC ,numberOfBlockedContracts
# MAGIC ,numberOfCancelledContracts
# MAGIC ,numberOfProductChanges
# MAGIC ,numberOfContractsWProductChanges
# MAGIC ,numberOfContrWthEndOfSupRjctd
# MAGIC ,numberOfContrWthStartOfSupRjctd
# MAGIC ,numberOfContrWaitingForEndOfSup
# MAGIC ,numberOfContrWaitingForStrtOfSup
# MAGIC ,creationDateE
# MAGIC ,lastChangedDateE
# MAGIC ,contractStartDateE
# MAGIC ,contractEndDateE
# MAGIC from
# MAGIC (select
# MAGIC SAPClient as sapClient
# MAGIC ,ItemUUID as ItemUUID
# MAGIC ,PodUUID as podUUID
# MAGIC ,HeaderUUID as headerUUID
# MAGIC ,UtilitiesContract as utilitiesContract
# MAGIC ,BusinessPartner as businessPartnerGroupNumber
# MAGIC ,BusinessPartnerFullName as businessPartnerGroupName
# MAGIC ,BusinessAgreement as businessAgreement
# MAGIC ,BusinessAgreementUUID as businessAgreementUUID
# MAGIC ,IncomingPaymentMethod as incomingPaymentMethod
# MAGIC ,IncomingPaymentMethodName as incomingPaymentMethodName
# MAGIC ,PaymentTerms as paymentTerms
# MAGIC ,PaymentTermsName as paymentTermsName
# MAGIC ,SoldToParty as soldToParty
# MAGIC ,SoldToPartyName as businessPartnerFullName
# MAGIC ,Division as divisionCode
# MAGIC ,DivisionName as division
# MAGIC ,ContractStartDate as contractStartDate
# MAGIC ,ContractEndDate as contractEndDate
# MAGIC ,CreationDate as creationDate
# MAGIC ,CreatedByUser as createdBy
# MAGIC ,LastChangeDate as lastChangedDate
# MAGIC ,LastChangedByUser as changedBy
# MAGIC ,ItemCategory as itemCategory
# MAGIC ,ItemCategoryName as itemCategoryName
# MAGIC ,Product as product
# MAGIC ,ProductDescription as productDescription
# MAGIC ,ItemType as itemType
# MAGIC ,ItemTypeName as itemTypeName
# MAGIC ,ProductType as productType
# MAGIC ,HeaderType as headerType
# MAGIC ,HeaderTypeName as headerTypeName
# MAGIC ,HeaderCategory as headerCategory
# MAGIC ,HeaderCategoryName as headerCategoryName
# MAGIC ,HeaderDescription as headerDescription
# MAGIC ,IsDeregulationPod as isDeregulationPod
# MAGIC ,UtilitiesPremise as premise
# MAGIC ,NumberOfPersons as numberOfPersons
# MAGIC ,CityName as cityName
# MAGIC ,StreetName as streetName
# MAGIC ,HouseNumber as houseNumber
# MAGIC ,PostalCode as postalCode
# MAGIC ,Building as building
# MAGIC ,AddressTimeZone as addressTimeZone
# MAGIC ,CountryName as countryName
# MAGIC ,RegionName as regionName
# MAGIC ,NumberOfContractChanges as numberOfContractChanges
# MAGIC ,IsOpen as isOpen
# MAGIC ,IsDistributed as isDistributed
# MAGIC ,HasError as hasError
# MAGIC ,IsToBeDistributed as isToBeDistributed
# MAGIC ,IsIncomplete as isIncomplete
# MAGIC ,IsStartedDueToProductChange as isStartedDueToProductChange
# MAGIC ,IsInActivation as isInActivation
# MAGIC ,IsInDeactivation as isInDeactivation
# MAGIC ,IsCancelled as isCancelled
# MAGIC ,CancellationMessageIsCreated as cancellationMessageIsCreated
# MAGIC ,SupplyEndCanclnMsgIsCreated as supplyEndCanclInMsgIsCreated
# MAGIC ,ActivationIsRejected as activationIsRejected
# MAGIC ,DeactivationIsRejected as deactivationIsRejected
# MAGIC ,NumberOfContracts as numberOfContracts
# MAGIC ,NumberOfActiveContracts as numberOfActiveContracts
# MAGIC ,NumberOfIncompleteContracts as numberOfIncompleteContracts
# MAGIC ,NumberOfDistributedContracts as numberOfDistributedContracts
# MAGIC ,NmbrOfContractsToBeDistributed as numberOfContractsToBeDistributed
# MAGIC ,NumberOfBlockedContracts as numberOfBlockedContracts
# MAGIC ,NumberOfCancelledContracts as numberOfCancelledContracts
# MAGIC ,NumberOfProductChanges as numberOfProductChanges
# MAGIC ,NmbrOfContractsWProductChanges as numberOfContractsWProductChanges
# MAGIC ,NmbrOfContrWthEndOfSupRjctd as numberOfContrWthEndOfSupRjctd
# MAGIC ,NmbrOfContrWthStartOfSupRjctd as numberOfContrWthStartOfSupRjctd
# MAGIC ,NmbrOfContrWaitingForEndOfSup as numberOfContrWaitingForEndOfSup
# MAGIC ,NmbrOfContrWaitingForStrtOfSup as numberOfContrWaitingForStrtOfSup
# MAGIC ,CreationDate_E as creationDateE
# MAGIC ,LastChangeDate_E as lastChangedDateE
# MAGIC ,ContractStartDate_E as contractStartDateE
# MAGIC ,ContractEndDate_E as contractEndDateE
# MAGIC 
# MAGIC ,row_number() over (partition by UtilitiesContract,ContractEndDate_E order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC 
# MAGIC --and f.DDLANGUAGE ='E' 
# MAGIC )a where  a.rn = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC SAPClient
# MAGIC from 
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC sapClient
# MAGIC from 
# MAGIC cleansed.${vars.table}
