# Databricks notebook source
import json
import requests
import base64

DOMAIN = 'australiaeast.azuredatabricks.net'
TOKEN = dbutils.secrets.get(scope='vwazr-dp-keyvault',key='databricks-Token')

# Run jobs
def runJob(jobId):  
  params = json.dumps(parameters)
  response = requests.post(
    'https://%s/api/2.0/jobs/run-now' % (DOMAIN),
    headers={'Authorization': 'Bearer %s' % TOKEN},
    json= {
      "job_id": jobId,
      "notebook_params": {
        "parameters": params
      }
    }
  )

def createJob():
  response = requests.post(
    'https://%s/api/2.0/jobs/create' % (DOMAIN),
    headers={'Authorization': 'Bearer %s' % TOKEN},
    json= {
      "name": "Nightly model training",
      "new_cluster": {
        "spark_version": "6.6.x-scala2.11",
        "node_type_id": "Standard_F4s",
        "num_workers": 1
      },
      "libraries": [
        {
          "jar": "dbfs:/my-jar.jar"
        },
        {
          "maven": {
            "coordinates": "org.jsoup:jsoup:1.7.2"
          }
        }
      ],
      "timeout_seconds": 3600,
      "max_retries": 1,
      "spark_jar_task": {
        "main_class_name": "com.databricks.ComputeModels"
      }
    }
  )
  
def listJob():
  response = requests.get(
    'https://%s/api/2.0/jobs/list' % (DOMAIN),
    headers={'Authorization': 'Bearer %s' % TOKEN}
  )
