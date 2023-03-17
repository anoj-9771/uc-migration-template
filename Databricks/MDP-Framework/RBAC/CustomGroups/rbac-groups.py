# Databricks notebook source
CUSTOM_GROUPS = [
    {
        "Name" : "AI-ML"
        ,"TableFilter" : [
            "raw.cctv_*",
            "raw.bom_*",
            "raw.beachwatch_*",
            "raw.iicats_*",
            "raw.labware_*",
            "raw.maximo_*",
            "stage.cctv_*",
            "cleansed.cctv_*",
            "cleansed.bom_*",
            "cleansed.beachwatch_*",
            "cleansed.iicats_*",
            "cleansed.urbanplunge_*",
            "cleansed.labware_*",
            "cleansed.maximo_*",
            "sewercctv.*"
        ]
        ,"Users" : ["onyu", "o2pr", "v4y", "49f", "39A", "9T9"] 
    }
]

# COMMAND ----------

dbutils.notebook.exit({ "CUSTOM_GROUPS" : CUSTOM_GROUPS })
