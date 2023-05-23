# Databricks notebook source
RBAC_UC2 = [
    {
        "Name" : "L1-Official"
        ,"Level" : 1
        ,"OtherCommands" : [
        ]
        ,"TableFilter" : [
        ]
        ,"Users" : []
        ,"AADGroups" : [ "cleansed-official" ]
    },
    {
        "Name" : "L2-Sensitive"
        ,"Level" : 2
        ,"ParentGroup" : "L1-Official"
        ,"TableFilter" : [
            "cleansed.sharepointlist_demandcalculationadjustment"
            ,"curated_v3.factAssetDemandValue"
            ,"curated_v3.factDemand"
            ,"curated_v3.viewDemandWaterNetwork"
            ,"curated_v3.viewFactWaterNetworkDemand"
            ,"curated_v3.viewSystemExportVolume"
            ,"curated_v3.viewSWCDemand"
            ,"curated_v3.viewManualDemand"
        ]
        ,"Users" : []
        ,"AADGroups" : [ "cleansed-sensitive" ]
    }
]

# COMMAND ----------


