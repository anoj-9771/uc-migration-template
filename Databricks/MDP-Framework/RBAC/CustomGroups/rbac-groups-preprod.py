# Databricks notebook source
CUSTOM_GROUPS = [
    {
        "Name": "EPPM-Users",
        "TableFilter": [
            "cleansed.eppm_*"
        ],
        "Users": [
            "8w6",
            "0un",
            "lqa",
            "0id",
            "3bj"
        ]
    }
]

# COMMAND ----------

dbutils.notebook.exit({ "CUSTOM_GROUPS" : CUSTOM_GROUPS })
