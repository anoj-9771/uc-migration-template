# Databricks notebook source
# DBTITLE 1,Parameters
if 
dbutils.widgets.text('srcKvSecret','','')
dbutils.widgets.text('dstKvSecret','','')
dbutils.widgets.text('srcAccount','','')
dbutils.widgets.text('dstAccount','','')
dbutils.widgets.text('srcContainerName','','')
dbutils.widgets.text('srcDirectoryName','','')
dbutils.widgets.text('srcType','','')
dbutils.widgets.text('dstType','','')
dbutils.widgets.text('dstContainerName','','')
dbutils.widgets.text('dstDirectoryName','','')
dbutils.widgets.text('srcBlobName','','')
dbutils.widgets.text('dstBlobName','','')
dbutils.widgets.text('dstTableName','','')
dbutils.widgets.text('srcFormat','','')
dbutils.widgets.text('dstFormat','','')
dbutils.widgets.text('prcType','','')
dbutils.widgets.text('query','','')
dbutils.widgets.text('catalogName','','')
dbutils.widgets.text('catalogSecret','','')



parameters = dict(
    srcKvSecret = dbutils.widgets.get('srcKvSecret')
    ,srcAccName = dbutils.widgets.get('srcAccount')
    ,srcContainerName = dbutils.widgets.get('srcContainerName')
    ,srcDirectoryName = dbutils.widgets.get('srcDirectoryName')
    ,srcType = dbutils.widgets.get('srcType')
    ,dstType = dbutils.widgets.get('dstType')
    ,dstKvSecret = dbutils.widgets.get('dstKvSecret')
    ,dstAccName = dbutils.widgets.get('dstAccount')
    ,dstContainerName = dbutils.widgets.get('dstContainerName')
    ,dstDirectoryName = dbutils.widgets.get('dstDirectoryName')
    ,srcBlobName = dbutils.widgets.get('srcBlobName')
    ,dstBlobName = dbutils.widgets.get('dstBlobName')
    ,dstTableName = dbutils.widgets.get('dstTableName')
    ,srcFormat = dbutils.widgets.get('srcFormat')
    ,dstFormat = dbutils.widgets.get('dstFormat')
    ,prcType = dbutils.widgets.get('prcType')
    ,query = dbutils.widgets.get('query')
    ,catalogName = dbutils.widgets.get('catalogName')
    ,catalogSecret = dbutils.widgets.get('catalogSecret')
)
  

# COMMAND ----------

# DBTITLE 1,Blob Library
# MAGIC %run "/build/Util/BlobHelper-ut.py"

# COMMAND ----------

# DBTITLE 1,Schema Create Library
# MAGIC %run "/build/Util/DbHelperCreateTableSQL-ut.py"

# COMMAND ----------

# MAGIC %run "/build/Util/Helper_getDataFrame-ut.py"

# COMMAND ----------

# DBTITLE 1,Data Catalog
# MAGIC %run "/build/Util/DbHelper_DataCatalog-ut.py"

# COMMAND ----------

if paramenters["srcType"].split('_')[0].lower() == 'azure storage':
    srcAccountName = BlobStoreAccount(parameters["srcKvSecret"])

    srcBlobPath = GetBlobStoreFiles(parameters["srcKvSecret"],srcAccountName,\
                                    parameters["srcContainerName"],parameters["srcDirectoryName"],parameters["srcBlobName"], "wasbs")
else:
    

if paramenters["dstType"].split('_')[0].lower() == 'azure storage':
    dstAccountName = BlobStoreAccount(parameters["dstKvSecret"])

    dstBlobPath = GetBlobStoreFiles(parameters["dstKvSecret"],dstAccountName,\
                                    parameters["dstContainerName"],parameters["dstDirectoryName"],parameters["dstBlobName"], "wasbs")

    dstPath = GetBlobStoreFiles(parameters["dstKvSecret"],dstAccountName,\
                                    parameters["dstContainerName"],parameters["dstDirectoryName"],'', "wasbs")
    
elif paramenters["dstType"].split('_')[0].lower() == 'azure sql server':
    DatabaseConn_analytics = DbHelper(parameters["dstKvSecret"])

# COMMAND ----------

# DBTITLE 1,Extract Dataset
#extracts dataframe for csv, json and excel from blob storage

# COMMAND ----------

# DBTITLE 1,Set Source Data Asset
if parameters["prcType"].split('_')[0].lower() in ['sql server', 'oracle']:
    src_dsl = getDSL(parameters["prcType"],parameters["dstTableName"], server = parameters["srcAccName"], database = parameters["srcDirectoryName"], schema = parameters["dstTableName"].split('.')[0], table = parameters["dstTableName"].split('.')[1])
  
elif parameters["prcType"].split('_')[0].lower() == 'azure storage':
    src_dsl = getDSL(parameters["prcType"],parameters["dstTableName"], account = parameters["srcAccName"], container = parameters["dstContainerName"])
    
elif parameters["prcType"].split('_')[0].lower() == 'azure data lake store':
    src_dsl = getDSL(parameters["prcType"], parameters["dstTableName"], url = parameters["srcType"])

# COMMAND ----------

if parameters["dstType"].split('_')[0].lower() in ['sql server', 'oracle']:
    dst_dsl = getDSL(parameters["srcType"],parameters["dstTableName"], server = parameters["srcAccName"], database = parameters["srcDirectoryName"], schema = parameters["dstTableName"].split('.')[0], table = parameters["dstTableName"].split('.')[1])
  
elif parameters["dstType"].split('_')[0].lower() == 'azure storage':
    dst_dsl = getDSL(parameters["srcType"],parameters["dstTableName"], account = parameters["srcAccName"], container = parameters["dstContainerName"])
    
elif parameters["dstType"].split('_')[0].lower() == 'azure data lake store':
    dst_dsl = getDSL(parameters["srcType"], parameters["dstTableName"], url = parameters["srcType"])

# COMMAND ----------

# DBTITLE 1,Set Data Frame Data Asset
if parameters["dstType"].split('_')[0].lower() in ['sql server', 'oracle']:
    dst_dsl = getDSL(parameters["srcType"],parameters["dstTableName"], server = parameters["srcAccName"], database = parameters["srcDirectoryName"], schema = parameters["dstTableName"].split('.')[0], table = parameters["dstTableName"].split('.')[1])
  
elif parameters["dstType"].split('_')[0].lower() == 'azure storage':
    dst_dsl = getDSL(parameters["srcType"],parameters["dstTableName"], account = parameters["srcAccName"], container = parameters["dstContainerName"])
    
elif parameters["dstType"].split('_')[0].lower() == 'azure data lake store':
    dst_dsl = getDSL(parameters["srcType"], parameters["dstTableName"], url = parameters["srcType"])

# COMMAND ----------

# DBTITLE 1,Create and Write Data Frame
df = CreateDataFrame(srcBlobPath, parameters["srcFormat"], parameters["query"], parameters["srcBlobName"])

MakeDirectory(parameters["dstKvSecret"],parameters["dstAccName"],parameters["dstContainerName"],dstPath)
df.write.mode('overwrite').parquet(dstBlobPath + parameters["dstFormat"])

dstSchemaName = parameters["dstTableName"].split('.')[0]
dstTableName = parameters["dstTableName"].split('.')[1]

##Added in if statement to move schema blob from raw to stage
if parameters["prcType"] in ['SQL Server']:#, 'Oracle']:
  schm = spark.read.format('csv').options(sep='|', header='true',quote='"', escape='\'').option("inferSchema", "true").load(srcBlobPath + '_schema' + parameters["srcFormat"])
  if len(schm.head(1)) == 0:
    schm = createSchemaDataFrame(df, dstSchemaName, dstTableName)
  schm.write.mode('overwrite').parquet(dstBlobPath + '_schema' + parameters["dstFormat"])
# elif parameters["prcType"] in ['Oracle']:
#   None
else:
  schm = createSchemaDataFrame(df, dstSchemaName, dstTableName)
  schm.write.mode('overwrite').parquet(dstBlobPath + '_schema' + parameters["dstFormat"])

# COMMAND ----------

catalogName = "Lundalls"
clientIDFromAzureAppRegistration = "f2e51206-33cb-4706-8407-59e4d0745017"
tenantId = "1cea56f5-9f48-413d-a7bd-1c7f033f3977"
registerUri = "https://api.azuredatacatalog.com/catalogs/{0}/views/tables?api-version=2016-03-30".format(parameters["catalogName"])
accessToken = getAccessToken(parameters["catalogSecret"])
upn = getUPN()

# COMMAND ----------

connection = "tenant_id=1cea56f5-9f48-413d-a7bd-1c7f033f3977;client_id=f2e51206-33cb-4706-8407-59e4d0745017;secret=6vpDy/66bt5A.[fDyGrG?n2Hg]GsyRv?"
cons = connection.split(';')
cons = cons
for con in cons:
    value = con.split('=')
    try:
      creds[value[0]] = value[1]
    except : False
url_auth = "https://login.microsoftonline.com/%s/oauth2/v2.0/token" % creds["tenant_id"]
headers = {'Content-Type': "application/x-www-form-urlencoded"}
payload = {'grant_type' : 'client_credentials', 'scope':'https://datacatalog.azure.com/.default'}
res = requests.get(url_auth,headers=headers,data=payload,auth=HTTPBasicAuth(creds["client_id"], creds["secret"]))
accessToken = res.json()['access_token']
print(accessToken)

# COMMAND ----------

print(creds)

# COMMAND ----------

from pyspark.sql import functions as F
dsamp = df.select("*").limit(10).toPandas()
sample = dsamp.to_json(orient='records')#.replace('{','{{').replace('}','}}')
dprof = dataCatalogProfile(df, df.columns)
data_profile = dprof.to_json(orient='records')#.replace('{','{{').replace('}','}}')
dschm_01 = schm.select(col("Column_Name").alias("name"),col("Column_DataType").alias("type"), F.when(schm.Column_Nullable == "",False).otherwise(True).alias("isNullable")).toPandas()
dschm_02 = pd.DataFrame(df.dtypes)
dschm_02.columns = ['name','type']
dschm_02['isNullable'] = True
src_columns = dschm_01.to_json(orient='records').replace('[','').replace(']','')#.replace('{','{{').replace('}','}}')
df_columns_02 = dschm_02.to_json(orient='records').replace('[','').replace(']','')#.replace('{','{{').replace('}','}}')
row_count = df.count()
table_size = df.toPandas().memory_usage(deep=True).sum()

# COMMAND ----------

#Registration or Update Data Catalog for Source System

scr_asset = bodyJson(upn, columns_02, sample, row_count, table_size, data_profile, src_dsl)
id = registerDataAsset(asset)
print("REG:" + id)

# COMMAND ----------

#Registration or Update Data Catalog For Dataframe
df_asset = bodyJson(upn, columns_02, sample, row_count, table_size, data_profile, df_dsl)
id = registerDataAsset(asset)
print("REG:" + id)

# COMMAND ----------

#Search
src_searchJson = searchDataAsset(parameters["dstTableName"])
print("SER:" + src_searchJson)

# COMMAND ----------

#Search
df_searchJson = searchDataAsset(parameters["dstTableName"].replace('.','_'))
print("SER:" + df_searchJson)

# COMMAND ----------

fullUri = "https://api.azuredatacatalog.com/catalogs/{0}/search/search?searchTerms={1}&count=100&api-version=2016-03-30".format(catalogName, 'InvoiceLines AND has:tableDataProfiles AND has:columnsDataProfiles')
resp = setRequestAndGetResponse(fullUri, None)

# COMMAND ----------

print(resp.text)

# COMMAND ----------

from pyspark.sql.functions import explode, explode_outer, array, col, first, monotonically_increasing_id
df0 = df_search.select('results.content.annotations.experts')
df1 = df0.select('experts')
# df1 = df0.select(explode_outer(df0.element))
display(df0)

# COMMAND ----------

print(asset)

# COMMAND ----------

dataProfile(df, df.columns).head(20)
