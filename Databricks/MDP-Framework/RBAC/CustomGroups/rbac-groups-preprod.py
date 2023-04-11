# Databricks notebook source
CUSTOM_GROUPS = [
#    {
#        "Name": "maximo-users",
#        "TableFilter": [
#            "cleansed.maximo_*",
#            "cleansed.vw_maximo_*",
#            "curated.vw_maximo_*"
#        ],
#        "Users": [ ]
#    },
#    {
#        "Name": "iicats-users",
#        "TableFilter": [
#            "cleansed.iicats_*",
#            "curated.vw_iicats_*"
#        ],
#        "Users": [ ]
#    },
#    {
#        "Name": "labware-users",
#        "TableFilter": [
#            "cleansed.labware_*"
#        ],
#        "Users": [ ]
#    },
#    {
#        "Name": "art-users",
#        "TableFilter": [
#            "cleansed.art_*"
#        ],
#        "Users": [ ]
#    },
#    {
#        "Name": "eppm-Users",
#        "TableFilter": [
#            "cleansed.eppm_*"
#        ],
#        "Users": [ ]
#    },
#    {
#        "Name": "scada-Users",
#        "TableFilter": [
#            "cleansed.scada_*"
#        ],
#        "Users": [ ]
#    }
]

# COMMAND ----------

dbutils.notebook.exit({ "CUSTOM_GROUPS" : CUSTOM_GROUPS })
