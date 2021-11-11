# Databricks notebook source
import json
import requests
import base64

DOMAIN = 'adb-6510910994889.9.azuredatabricks.net'
TOKEN = 'dapid3182eace390ab69155a9592d67dbe5a'
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

storage_account_name = "sadafdev01"
storage_account_access_key = dbutils.secrets.get(scope="daf-databricks-secret-scope",key="daf-sa-collibra-sql-sas")
fileLocation = "wasbs://collibra@sadafdev01.blob.core.windows.net/SQL"
file_type = "csv"
print(storage_account_name)
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)
dbutils.fs.ls(fileLocation)
dbutils.fs.mount(source=fileLocation,mount_point='/mnt/collibra/SQL',extra_configs={'fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net':dbutils.secrets.get(scope='daf-databricks-secret-scope', key='daf-sa-collibra-sql-sas')})

# COMMAND ----------

+#dbutils.secrets.list("daf-databricks-secret-scope")

# COMMAND ----------

#get list of notebooks
listResp = dbfs_rpc('get','list','{"path": "/build/cleansed"}')
for object in listResp['objects']:
    if object['object_type'] == 'DIRECTORY':
        if object['path'].split('/')[-1] not in ['ACCESS Data', 'ACCESS Ref', 'crmdata', 'crmref', 'isudata', 'isuref', 'isuslt', 'HYDRA Data']:
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

                with open(f'/dbfs/mnt/collibra/SQL/{database}_{fileName}.sql', "w") as sqlFile:
                    sqlFile.write(SQL)

# COMMAND ----------


