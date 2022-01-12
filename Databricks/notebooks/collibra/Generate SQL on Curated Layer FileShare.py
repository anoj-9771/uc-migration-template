# Databricks notebook source
pip install azure-storage-file-share

# COMMAND ----------

import json
import requests
import base64

DOMAIN = 'adb-7004525605760210.10.azuredatabricks.net'
TOKEN = 'dapi9c75f6a8b6d9c8b1fa0fd9455c5d03ec' #dbutils.secrets.get(scope="daf-databricks-secret-scope",key="daf-databricks-token")
#BASE_URL = f'https://{DOMAIN}/api/2.0/dbfs/'
BASE_URL = f'https://{DOMAIN}/api/2.0/workspace/'

def dbfs_rpc(action, operation, body):
    """ A helper function to make the DBFS API request, request/response is encoded/decoded as JSON """
    response = requests.get(
        BASE_URL + operation,
        headers={'Authorization': f'Bearer {TOKEN}'},
        json=json.loads(body)
    )

    #print(response.text)
    return response.json()

#print(dbfs_rpc('get','{"path": "/Users/ydm@sydneywater.com.au/"}'))



# COMMAND ----------

# storage_account_name = "sadafdev01"
# storage_account_access_key = dbutils.secrets.get(scope="daf-databricks-secret-scope",key="daf-sa-collibra-sql-sas")
# fileLocation = "wasbs://collibra@sadafdev01.blob.core.windows.net/SQL"
# file_type = "csv"
# print(storage_account_name)
# spark.conf.set(
#   "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
#   storage_account_access_key)
# dbutils.fs.ls(fileLocation)
# dbutils.fs.mount(source=fileLocation,mount_point='/mnt/collibra/SQL',extra_configs={'fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net':dbutils.secrets.get(scope='daf-databricks-secret-scope', key='daf-sa-collibra-sql-sas')})

# COMMAND ----------

# sorted(dbutils.fs.mounts())

# COMMAND ----------

from azure.storage.fileshare import ShareFileClient
from azure.storage.fileshare import ShareServiceClient
from azure.storage.fileshare import ShareClient 
service = ShareServiceClient(account_url="https://sadafdev01.file.core.windows.net", credential='8oo6u8ksHAOBebyhs5gtIYn/EgOiS1RUNsqsX31XcB/UbiOtgYNRowYTTk5QvmHTDtlL/SgtCrYwXFF3+oboOQ==')

allShares = list(service.list_shares())

for share in allShares:
    print(share)

# connection_string = "DefaultEndpointsProtocol=https;AccountName=sadafdev01;AccountKey=8oo6u8ksHAOBebyhs5gtIYn/EgOiS1RUNsqsX31XcB/UbiOtgYNRowYTTk5QvmHTDtlL/SgtCrYwXFF3+oboOQ==;EndpointSuffix=core.windows.net"
# service = ShareServiceClient.from_connection_string(conn_str=connection_string)

#share = ShareClient(account_url="https://sadafdev01.file.core.windows.net/", share_name='collibra', credential='https://sadafdev01.file.core.windows.net/?sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupx&se=2022-11-30T08:38:57Z&st=2021-11-23T00:38:57Z&spr=https,http&sig=XiYBdiUsuZDOwt37UpTI3FEZkcfzAhnXGTdZGs1OFv4%3D') #.from_connection_string(conn_str=connection_string, share_name="SQL")
#share.create_share()

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://sadafdev01.file.core.windows.net?credential='8oo6u8ksHAOBebyhs5gtIYn/EgOiS1RUNsqsX31XcB/UbiOtgYNRowYTTk5QvmHTDtlL/SgtCrYwXFF3+oboOQ=='

# COMMAND ----------

# from azure.storage.fileshare import ShareFileClient
# from azure.storage.fileshare import ShareClient 
# #file_client = ShareFileClient.from_connection_string(conn_str="DefaultEndpointsProtocol=https;AccountName=sadafdev01;AccountKey=8oo6u8ksHAOBebyhs5gtIYn/EgOiS1RUNsqsX31XcB/UbiOtgYNRowYTTk5QvmHTDtlL/SgtCrYwXFF3+oboOQ==;EndpointSuffix=core.windows.net", share_name="collibra", file_path="SQLtest.sql")
# connection_string = "DefaultEndpointsProtocol=https;AccountName=sadafdev01;AccountKey=8oo6u8ksHAOBebyhs5gtIYn/EgOiS1RUNsqsX31XcB/UbiOtgYNRowYTTk5QvmHTDtlL/SgtCrYwXFF3+oboOQ==;EndpointSuffix=core.windows.net"
# service = ShareServiceClient.from_connection_string(conn_str=connection_string)

# share = ShareClient.from_connection_string(conn_str=connection_string, share_name="SQL")
# share.create_share()
# #to write a file...
# # with open("/dbfs/tmp/summary_to_upload.csv", "rb") as source_file:
# #     file_client.upload_file(source_file)
    
# #to read a file
# #file_client = ShareFileClient.from_connection_string(conn_str="AZURE_STORAGE_CONNECTION_STRING", share_name="AZURE_STORAGE_FILE_SHARE_NAME", file_path="summary_to_download.csv")
 
# # with open("/dbfs/tmp/summary_downloaded.csv", "wb") as file_handle:
# #     data = file_client.download_file()
# #     data.readinto(file_handle)


# COMMAND ----------

# DBTITLE 1,Produce SQL files and write to file share
#get list of notebooks
listResp = dbfs_rpc('get','list','{"path": "/build/cleansed"}')
for object in listResp['objects']:
    if object['object_type'] == 'DIRECTORY':
        if object['path'].split('/')[-1] not in ['ACCESS Data', 'ACCESS Ref', 'CRM Data', 'CRM Ref', 'ISU Data', 'ISU Ref', 'ISU SLT', 'HYDRA Data']:
            print(object['path'] + ' skipped')
            continue

        listResp2 = dbfs_rpc('get','list','{"path": "' + object['path'] + '"}')
        
        for object2 in listResp2['objects']:
            if object2['object_type'] == 'NOTEBOOK':
                
                fileName = object2['path'].split('/')[-1]
                #print(object)
                #get file content
                exportResp = dbfs_rpc('get','export','{"path": "' + object2['path'] + '"}')
                #convert response to text
                nbText = base64.b64decode(exportResp['content'].encode('ASCII')).decode('ASCII')
                #find start and end of SQL
                startSQL = nbText.find('spark.sql(') + (10 if nbText[nbText.find('spark.sql(')+10:].startswith('"SELECT') else 11)
                endSQL = nbText.find('")\n',startSQL)
                #sometimes endSQL isn't quite right
                if nbText.find('display(df_cleansed)') > -1 and nbText.find('display(df_cleansed)') < endSQL:
                    endSQL = nbText.find('display(df_cleansed)') - 4

                SQL = nbText[startSQL:endSQL+2]
                SQL = SQL.replace('\\','')
                #get database name
                folder = object2['path'].split('/')[-2]
                if folder in ('ACCESS Ref', 'ACCESS Data'):
                    database = 'access'
                elif folder[:3].lower() in ['crm','isu']:
                    database = folder[:3].lower()
                else:
                    database = folder.split()[0].lower()

                #there are a few notebooks with environment specific code. remove that for now and only take the dev environment code
                SQL = SQL.replace('    ','\t')
                envStart = max(SQL.find('" + ("'),SQL.find('" + \n\t\t("'))
                
                while envStart > -1:
                    if fileName == 'Z309_TPROPERTYADDRESS':
                        print(envStart)
                        print(SQL)
                    devPart = SQL.find('else "',envStart) + 6
                    SQL = (SQL[:envStart-1] + SQL[devPart:])
                    print(SQL)
                    endDevPart = SQL.find('")', envStart)
                    startNextPart = SQL.find('"',endDevPart + 1)
                    print(endDevPart,startNextPart)
                    SQL = SQL[:endDevPart] + SQL[startNextPart + 1:]
                    envStart = max(SQL.find('" + ("'),SQL.find('" + \n\t\t("'))
                    print(envStart)

                SQL = SQL[1:-2].replace('        ','\t').replace('\t\t','\t').replace('{ADS_DATABASE_STAGE}.{source_object}',f'CLEANSED.{database}_{fileName}')
                SQL = SQL.replace('CLEANSED.STG_" + source_object',f'CLEANSED.{database}_{fileName}')
                
                #save a local copy
                with open(f'/dbfs/mnt/collibra/SQL/{database}_{fileName}.sql', "w") as sqlFile:
                    sqlFile.write(SQL)
                
                #save a copoy on a file share
                with open(f'/dbfs/mnt/collibra/SQL/{database}_{fileName}.sql', "rb") as sqlFile:
                    file_client = ShareFileClient.from_connection_string(conn_str="DefaultEndpointsProtocol=https;AccountName=sadafdev01;AccountKey=8oo6u8ksHAOBebyhs5gtIYn/EgOiS1RUNsqsX31XcB/UbiOtgYNRowYTTk5QvmHTDtlL/SgtCrYwXFF3+oboOQ==;EndpointSuffix=core.windows.net", share_name="collibra",file_path=f"SQL/{database}_{fileName}.sql")
                    file_client.upload_file(sqlFile)
    

# COMMAND ----------


