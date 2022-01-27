# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = '0ARCHOBJECT_ATTR'

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
# MAGIC architecturalObjectFunction
# MAGIC ,architecturalObjectId
# MAGIC ,architecturalObjectNumber
# MAGIC ,architecturalObjectTypeCode
# MAGIC ,authorizationGroup
# MAGIC ,cityName
# MAGIC ,district
# MAGIC ,countryShortName
# MAGIC ,houseNumber
# MAGIC ,houseNumber2
# MAGIC ,objectNumber
# MAGIC ,parentArchitecturalObjectId
# MAGIC ,parentArchitecturalObjectTypeCode
# MAGIC ,poBoxCode
# MAGIC ,postalCode
# MAGIC ,poBoxPostalCode
# MAGIC ,stateCode
# MAGIC ,responsiblePerson
# MAGIC ,maintenanceDistrict
# MAGIC ,entityLocationNumber
# MAGIC ,districtLocationIndicator
# MAGIC ,streetName
# MAGIC ,businessEntityTransportConnectionsIndicator
# MAGIC ,commonUsage
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC from
# MAGIC (select
# MAGIC AOFUNCTION as architecturalObjectFunction
# MAGIC ,AOID as architecturalObjectId
# MAGIC ,AONR as architecturalObjectNumber
# MAGIC ,AOTYPE as architecturalObjectTypeCode
# MAGIC ,AUTHGRP as authorizationGroup
# MAGIC ,CITY1 as cityName
# MAGIC ,CITY2 as district
# MAGIC ,COUNTRY as countryShortName
# MAGIC ,HOUSE_NUM1 as houseNumber
# MAGIC ,HOUSE_NUM2 as houseNumber2
# MAGIC ,OBJNR as objectNumber
# MAGIC ,PARENTAOID as parentArchitecturalObjectId
# MAGIC ,PARENTAOTYPE as parentArchitecturalObjectTypeCode
# MAGIC ,PO_BOX as poBoxCode
# MAGIC ,POST_CODE1 as postalCode
# MAGIC ,POST_CODE2 as poBoxPostalCode
# MAGIC ,REGION as stateCode
# MAGIC ,RESPONSIBLE as responsiblePerson
# MAGIC ,SINSTBEZ as maintenanceDistrict
# MAGIC ,SLAGEWE as entityLocationNumber
# MAGIC ,SOBJLAGE as districtLocationIndicator
# MAGIC ,STREET as streetName
# MAGIC ,SVERKEHR as businessEntityTransportConnectionsIndicator
# MAGIC ,USAGECOMMON as commonUsage
# MAGIC ,VALIDFROM as validFromDate
# MAGIC ,VALIDTO as validToDate
# MAGIC ,row_number() over (partition by AOID  order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} -- WHERE LANGU = 'E'
# MAGIC )a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC architecturalObjectFunction
# MAGIC ,architecturalObjectId
# MAGIC ,architecturalObjectNumber
# MAGIC ,architecturalObjectTypeCode
# MAGIC ,authorizationGroup
# MAGIC ,cityName
# MAGIC ,district
# MAGIC ,countryShortName
# MAGIC ,houseNumber
# MAGIC ,houseNumber2
# MAGIC ,objectNumber
# MAGIC ,parentArchitecturalObjectId
# MAGIC ,parentArchitecturalObjectTypeCode
# MAGIC ,poBoxCode
# MAGIC ,postalCode
# MAGIC ,poBoxPostalCode
# MAGIC ,stateCode
# MAGIC ,responsiblePerson
# MAGIC ,maintenanceDistrict
# MAGIC ,entityLocationNumber
# MAGIC ,districtLocationIndicator
# MAGIC ,streetName
# MAGIC ,businessEntityTransportConnectionsIndicator
# MAGIC ,commonUsage
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC from
# MAGIC (select
# MAGIC AOFUNCTION as architecturalObjectFunction
# MAGIC ,AOID as architecturalObjectId
# MAGIC ,AONR as architecturalObjectNumber
# MAGIC ,AOTYPE as architecturalObjectTypeCode
# MAGIC ,AUTHGRP as authorizationGroup
# MAGIC ,CITY1 as cityName
# MAGIC ,CITY2 as district
# MAGIC ,COUNTRY as countryShortName
# MAGIC ,HOUSE_NUM1 as houseNumber
# MAGIC ,HOUSE_NUM2 as houseNumber2
# MAGIC ,OBJNR as objectNumber
# MAGIC ,PARENTAOID as parentArchitecturalObjectId
# MAGIC ,PARENTAOTYPE as parentArchitecturalObjectTypeCode
# MAGIC ,PO_BOX as poBoxCode
# MAGIC ,POST_CODE1 as postalCode
# MAGIC ,POST_CODE2 as poBoxPostalCode
# MAGIC ,REGION as stateCode
# MAGIC ,RESPONSIBLE as responsiblePerson
# MAGIC ,SINSTBEZ as maintenanceDistrict
# MAGIC ,SLAGEWE as entityLocationNumber
# MAGIC ,SOBJLAGE as districtLocationIndicator
# MAGIC ,STREET as streetName
# MAGIC ,SVERKEHR as businessEntityTransportConnectionsIndicator
# MAGIC ,USAGECOMMON as commonUsage
# MAGIC ,VALIDFROM as validFromDate
# MAGIC ,VALIDTO as validToDate
# MAGIC ,row_number() over (partition by AOID  order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} -- WHERE LANGU = 'E'
# MAGIC )a where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT architecturalObjectId, COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY architecturalObjectId
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY architecturalObjectId order by architecturalObjectId) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC architecturalObjectFunction
# MAGIC ,architecturalObjectId
# MAGIC ,architecturalObjectNumber
# MAGIC ,architecturalObjectTypeCode
# MAGIC ,authorizationGroup
# MAGIC ,cityName
# MAGIC ,district
# MAGIC ,countryShortName
# MAGIC ,houseNumber
# MAGIC ,houseNumber2
# MAGIC ,objectNumber
# MAGIC ,parentArchitecturalObjectId
# MAGIC ,parentArchitecturalObjectTypeCode
# MAGIC ,poBoxCode
# MAGIC ,postalCode
# MAGIC ,poBoxPostalCode
# MAGIC ,stateCode
# MAGIC ,responsiblePerson
# MAGIC ,maintenanceDistrict
# MAGIC ,entityLocationNumber
# MAGIC ,districtLocationIndicator
# MAGIC ,streetName
# MAGIC ,businessEntityTransportConnectionsIndicator
# MAGIC ,commonUsage
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC from
# MAGIC (select
# MAGIC AOFUNCTION as architecturalObjectFunction
# MAGIC ,AOID as architecturalObjectId
# MAGIC ,AONR as architecturalObjectNumber
# MAGIC ,AOTYPE as architecturalObjectTypeCode
# MAGIC ,AUTHGRP as authorizationGroup
# MAGIC ,CITY1 as cityName
# MAGIC ,CITY2 as district
# MAGIC ,COUNTRY as countryShortName
# MAGIC ,HOUSE_NUM1 as houseNumber
# MAGIC ,HOUSE_NUM2 as houseNumber2
# MAGIC ,OBJNR as objectNumber
# MAGIC ,PARENTAOID as parentArchitecturalObjectId
# MAGIC ,PARENTAOTYPE as parentArchitecturalObjectTypeCode
# MAGIC ,PO_BOX as poBoxCode
# MAGIC ,POST_CODE1 as postalCode
# MAGIC ,POST_CODE2 as poBoxPostalCode
# MAGIC ,REGION as stateCode
# MAGIC ,RESPONSIBLE as responsiblePerson
# MAGIC ,SINSTBEZ as maintenanceDistrict
# MAGIC ,SLAGEWE as entityLocationNumber
# MAGIC ,SOBJLAGE as districtLocationIndicator
# MAGIC ,STREET as streetName
# MAGIC ,SVERKEHR as businessEntityTransportConnectionsIndicator
# MAGIC ,USAGECOMMON as commonUsage
# MAGIC ,VALIDFROM as validFromDate
# MAGIC ,VALIDTO as validToDate
# MAGIC ,row_number() over (partition by AOID  order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} -- WHERE LANGU = 'E'
# MAGIC )a where  a.rn = 1 --and a.architecturalObjectFunction is not null
# MAGIC except
# MAGIC select
# MAGIC architecturalObjectFunction
# MAGIC ,architecturalObjectId
# MAGIC ,architecturalObjectNumber
# MAGIC ,architecturalObjectTypeCode
# MAGIC ,authorizationGroup
# MAGIC ,cityName
# MAGIC ,district
# MAGIC ,countryShortName
# MAGIC ,houseNumber
# MAGIC ,houseNumber2
# MAGIC ,objectNumber
# MAGIC ,parentArchitecturalObjectId
# MAGIC ,parentArchitecturalObjectTypeCode
# MAGIC ,poBoxCode
# MAGIC ,postalCode
# MAGIC ,poBoxPostalCode
# MAGIC ,stateCode
# MAGIC ,responsiblePerson
# MAGIC ,maintenanceDistrict
# MAGIC ,entityLocationNumber
# MAGIC ,districtLocationIndicator
# MAGIC ,streetName
# MAGIC ,businessEntityTransportConnectionsIndicator
# MAGIC ,commonUsage
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC architecturalObjectFunction
# MAGIC ,architecturalObjectId
# MAGIC ,architecturalObjectNumber
# MAGIC ,architecturalObjectTypeCode
# MAGIC ,authorizationGroup
# MAGIC ,cityName
# MAGIC ,district
# MAGIC ,countryShortName
# MAGIC ,houseNumber
# MAGIC ,houseNumber2
# MAGIC ,objectNumber
# MAGIC ,parentArchitecturalObjectId
# MAGIC ,parentArchitecturalObjectTypeCode
# MAGIC ,poBoxCode
# MAGIC ,postalCode
# MAGIC ,poBoxPostalCode
# MAGIC ,stateCode
# MAGIC ,responsiblePerson
# MAGIC ,maintenanceDistrict
# MAGIC ,entityLocationNumber
# MAGIC ,districtLocationIndicator
# MAGIC ,streetName
# MAGIC ,businessEntityTransportConnectionsIndicator
# MAGIC ,commonUsage
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC architecturalObjectFunction
# MAGIC ,architecturalObjectId
# MAGIC ,architecturalObjectNumber
# MAGIC ,architecturalObjectTypeCode
# MAGIC ,authorizationGroup
# MAGIC ,cityName
# MAGIC ,district
# MAGIC ,countryShortName
# MAGIC ,houseNumber
# MAGIC ,houseNumber2
# MAGIC ,objectNumber
# MAGIC ,parentArchitecturalObjectId
# MAGIC ,parentArchitecturalObjectTypeCode
# MAGIC ,poBoxCode
# MAGIC ,postalCode
# MAGIC ,poBoxPostalCode
# MAGIC ,stateCode
# MAGIC ,responsiblePerson
# MAGIC ,maintenanceDistrict
# MAGIC ,entityLocationNumber
# MAGIC ,districtLocationIndicator
# MAGIC ,streetName
# MAGIC ,businessEntityTransportConnectionsIndicator
# MAGIC ,commonUsage
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC from
# MAGIC (select
# MAGIC AOFUNCTION as architecturalObjectFunction
# MAGIC ,AOID as architecturalObjectId
# MAGIC ,AONR as architecturalObjectNumber
# MAGIC ,AOTYPE as architecturalObjectTypeCode
# MAGIC ,AUTHGRP as authorizationGroup
# MAGIC ,CITY1 as cityName
# MAGIC ,CITY2 as district
# MAGIC ,COUNTRY as countryShortName
# MAGIC ,HOUSE_NUM1 as houseNumber
# MAGIC ,HOUSE_NUM2 as houseNumber2
# MAGIC ,OBJNR as objectNumber
# MAGIC ,PARENTAOID as parentArchitecturalObjectId
# MAGIC ,PARENTAOTYPE as parentArchitecturalObjectTypeCode
# MAGIC ,PO_BOX as poBoxCode
# MAGIC ,POST_CODE1 as postalCode
# MAGIC ,POST_CODE2 as poBoxPostalCode
# MAGIC ,REGION as stateCode
# MAGIC ,RESPONSIBLE as responsiblePerson
# MAGIC ,SINSTBEZ as maintenanceDistrict
# MAGIC ,SLAGEWE as entityLocationNumber
# MAGIC ,SOBJLAGE as districtLocationIndicator
# MAGIC ,STREET as streetName
# MAGIC ,SVERKEHR as businessEntityTransportConnectionsIndicator
# MAGIC ,USAGECOMMON as commonUsage
# MAGIC ,VALIDFROM as validFromDate
# MAGIC ,VALIDTO as validToDate
# MAGIC ,row_number() over (partition by AOID  order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} -- WHERE LANGU = 'E'
# MAGIC )a where  a.rn = 1
