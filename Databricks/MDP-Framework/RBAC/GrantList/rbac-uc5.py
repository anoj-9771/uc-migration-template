# Databricks notebook source
RBAC_UC5 = [
    {
        "Name" : "L1-Official"
        ,"Level" : 1
        ,"OtherCommands" : [
        ]
        ,"TableFilter" : [
            "cleansed.hydstra_tsv_verified"
            ,"cleansed.hydstra_metadata"
        ]
        ,"Users" : []
        ,"AADGroups" : [ "cleansed-official" ]
    },
    {
        "Name" : "L2-Sensitive"
        ,"Level" : 2
        ,"ParentGroup" : "L1-Official"
        ,"TableFilter" : [
            "cleansed.hydstra_gaugedetails"
            ,"cleansed.hydstra_tsv_provisional"
            ,"cleansed.viewmaximosr"
            ,"cleansed.viewmaximowoactivity"
            ,"cleansed.maximo_workorder"
            ,"cleansed.maximo_longdescription"
            ,"cleansed.maximo_multiassetlocci"
        ]
        ,"Users" : []
        ,"AADGroups" : [ "cleansed-sensitive" ]
    }
]

# COMMAND ----------


