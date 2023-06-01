# Databricks notebook source
# MAGIC %run ../../Common/common-helpers

# COMMAND ----------

# MAGIC %run ../../Common/common-transform

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {get_table_namespace(f'{DEFAULT_TARGET}', 'viewRefAssetPerformanceAssetTypeClass')} AS
(
   select 
lookup1Code as assetFacilityTypeCode, 
lookup2Code as assetFacilityTypeDescription, 
return1Code as assetTypeClass,
return2Code as assetTypeProduct
from {get_table_namespace(f'{DEFAULT_TARGET}', 'refreportconfiguration')}
where mapTypeCode = 'Facility Type'
)
""")


# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {get_table_namespace(f'{DEFAULT_TARGET}', 'viewRefAssetPerformanceServiceType')} AS
(
   select 
lookup1Code as serviceTypeCode,
return1Code as serviceTypeGroup
from {get_table_namespace(f'{DEFAULT_TARGET}', 'refreportconfiguration')}
where mapTypeCode = 'Service Category'
)
""")


# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {get_table_namespace(f'{DEFAULT_TARGET}', 'viewRefAssetPerformanceServiceType')} AS
(
   select 
lookup1Code as serviceTypeCode,
return1Code as serviceTypeGroup
from {get_table_namespace(f'{DEFAULT_TARGET}', 'refreportconfiguration')}
where mapTypeCode = 'Service Category'
)
""")


# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {get_table_namespace(f'{DEFAULT_TARGET}', 'viewRefAssetPerformanceWorkType')} AS
(
   select 
lookup1Code as workTypeCode,
return1Code as workTypeDescription
from curated.refreportconfiguration
where mapTypeCode = 'Asset Performance Work Type Filter'
)
""")

# COMMAND ----------


