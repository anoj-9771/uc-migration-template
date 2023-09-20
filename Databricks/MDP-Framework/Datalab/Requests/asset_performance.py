# Databricks notebook source
# MAGIC %run ../datalab-provisioning

# COMMAND ----------

dataLabRequest = {
    "name" : "asset_performance"
    ,"use_compute": True
    ,"use_files": True
    ,"install_libraries": [
        { "pypi": { "package": "bcrypt==3.1.5" } }
    ]
    ,"group_members": [
        "i2k"
        ,"lpv"
        ,"wiu"
        ,"vx5"
        ,"wiu"
        ,"3bj"
    ]
}
CreateDatalab(dataLabRequest)

# COMMAND ----------


