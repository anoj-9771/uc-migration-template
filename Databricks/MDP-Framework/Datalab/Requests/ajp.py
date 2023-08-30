# Databricks notebook source
# MAGIC %run ../datalab-provisioning

# COMMAND ----------

dataLabRequest = {
    "name" : "ajp"
    ,"use_compute": True
    ,"use_files": False
    ,"install_libraries": [
    ]
    ,"group_members": [
        "ajp"
    ]
}
CreateDatalab(dataLabRequest)

# COMMAND ----------


