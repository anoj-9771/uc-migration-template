# Databricks notebook source
# MAGIC %run ../../atf-common

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimContractHistory

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
target_df = spark.sql("select * from curated_v2.dimContractHistory")
target_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Source with mapping for active and deleted records
#fetching data from isu data by performing inner join to fetch the common data for active records only . 
source_isu=spark.sql("""
Select
'ISU' as sourceSystemCode,
a.contractId as contractId,
a.validFromDate as validFromDate,
a.validToDate as validToDate,
a.CRMProduct as CRMProduct,
a.CRMObjectId as CRMObjectId,
a.CRMDocumentItemNumber as CRMDocumentItemNumber,
a.marketingCampaign as marketingCampaign,
a.individualContractId as individualContractId,
a.productBeginFlag as productBeginFlag,
a.productChangeFlag as productChangeFlag,
a.replicationControlsCode as replicationControlsCode,
a.replicationControls as replicationControls,
b.podUUID as podUUID,
b.headerTypeCode as headerTypeCode,
b.headerType as headerType,
b.isCancelledFlag as isCancelledFlag,
a.installationNumber as installationNumber,
a.contractHeadGUID as contractHeadGUID,
a.contractPosGUID as contractPosGUID,
a.productId as productId,
a.productGUID as productGUID,
a.createdDate as createdDate,
a.createdBy as createdBy,
a.lastChangedDate as lastChangedDate,
a.lastChangedBy as lastChangedBy

, a._RecordStart as _RecordStart
, a._RecordEnd as _RecordEnd
, a._RecordCurrent as _RecordCurrent
, a._RecordDeleted as _RecordDeleted
from cleansed.isu_0uccontracth_attr_2 a
left join cleansed.crm_utilitiescontract b
on a.contractId =b.utilitiesContract and a.validToDate=b.contractEndDateE
where a._RecordCurrent = 1 and a._RecordDeleted = 0
and b._RecordCurrent = 1 and b._RecordDeleted = 0
""")
source_isu.createOrReplaceTempView("source_view")
#display(source_isu)
#source_isu.count()

src_a = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=1 and _recordDeleted=0 ")
#src_d = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=0 and _recordDeleted=1 ")
src_a.createOrReplaceTempView("src_a")
#src_d.createOrReplaceTempView("src_d")


# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Define Variables for ATF
keyColumns = 'contractId,validToDate'
mandatoryColumns = 'contractId,validToDate'

columns = ("""
sourceSystemCode,
contractId,
validFromDate,
validToDate,
CRMProduct,
CRMObjectId,
CRMDocumentItemNumber,
marketingCampaign,
individualContractId,
productBeginFlag,
productChangeFlag,
replicationControlsCode,
replicationControls,
podUUID,
headerTypeCode,
headerType,
isCancelledFlag,
installationNumber,
contractHeadGUID,
contractPosGUID,
productId,
productGUID,
createdDate,
createdBy,
lastChangedDate,
lastChangedBy


""")

source_a = spark.sql(f"""
Select {columns}
From src_a
""")

source_d = spark.sql(f"""
Select {columns}
From src_d
""")

# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()

# COMMAND ----------

# MAGIC %md
# MAGIC #Investigation for failed tests

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimContractHistory where contractId='3101000331'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from src_d 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from src_d where contractId='3101055183'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimContractHistory where  _RecordCurrent=0 and _recordDeleted=1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from curated_v2.dimContractHistory

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from src_a

# COMMAND ----------


