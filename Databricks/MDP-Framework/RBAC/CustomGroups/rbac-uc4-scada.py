# Databricks notebook source
RBAC_UC4 = [
    {
        "Name" : "L1-Official"
        ,"Level" : 1
        ,"OtherCommands" : [
        ]
        ,"TableFilter" : [
            "cleansed.scada_point_cnfgn"
            ,"cleansed.scada_qlty_config"
            ,"cleansed.scada_rtu"
            ,"cleansed.scada_scxfield"
            ,"cleansed.scada_tsv"
            ,"cleansed.scada_tsv_point_cnfgn"
        ]
        ,"Users" : []
        ,"AADGroups" : [ "cleansed-official" ]
    },
    {
        "Name" : "L2-Sensitive"
        ,"Level" : 2
        ,"ParentGroup" : "L1-Official"
        ,"TableFilter" : [
            "cleansed.scada_event"
            ,"cleansed.scada_hierarchy_cnfgn"
            ,"cleansed.scada_point_limit"
            ,"cleansed.scada_scxuser"
        ]
        ,"Users" : []
        ,"AADGroups" : [ "cleansed-sensitive" ]
    },
]

# COMMAND ----------


