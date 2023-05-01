# Databricks notebook source
# MAGIC %run ./rbac-maximo

# COMMAND ----------

# MAGIC %run ./rbac-uc1

# COMMAND ----------

CUSTOM_GROUPS = [
    {
        "Name" : "L1-Official"
        ,"Level" : 1
        ,"OtherCommands" : [
            "DENY ALL PRIVILEGES ON TABLE `cleansed`.`iicats_rw_hierarchy_cnfgn` TO `L1-Official`"
            ,"DENY ALL PRIVILEGES ON TABLE `cleansed`.`iicats_rw_tsv` TO `L1-Official`"
            ,"DENY ALL PRIVILEGES ON TABLE `cleansed`.`iicats_rw_tsv_point_cnfgn` TO `L1-Official`"
        ]
        ,"TableFilter" : [
            "cleansed.*"
        ]
        ,"Users" : []
        ,"AADGroups" : [ "cleansed-official" ]
    },
    {
        "Name" : "L2-Sensitive"
        ,"Level" : 2
        ,"ParentGroup" : "L1-Official"
        ,"TableFilter" : [
            "cleansed.art_*"
            ,"cleansed.hydra_*"
            ,"cleansed.labware_*"
            ,"cleansed.eppm_*"
            ,"cleansed.Hydstra_TSV_Proviosional"
            ,"cleansed.Hydstra_GaugeDetails"
            ,"cleansed.iicats_site_hierarchy"
            ,"cleansed.iicats_scxuser"
            ,"cleansed.iicats_work_orders"
            ,"cleansed.iicats_hierarchy_cnfgn"
            ,"cleansed.iicats_event"
            ,*L2_Maximo
            ,"cleansed.scada_event"
            ,"cleansed.scada_hierarchy_cnfgn"
            ,"cleansed.scada_point_limit"
            ,"cleansed.scada_scxuser"
        ]
        ,"Users" : []
        ,"AADGroups" : [ "cleansed-sensitive" ]
    },
    {
        "Name" : "L3-Sensitive-Other"
        ,"Level" : 3
        ,"ParentGroup" : "L2-Sensitive"
        ,"TableFilter" : [
            "cleansed.maximo_servrectrans"
            ,"cleansed.swirl_*"
        ]
        ,"Users" : []
        ,"AADGroups" : [ "sensitive-others" ]
    },
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
    ,*RBAC_UC1
]

# COMMAND ----------

dbutils.notebook.exit({ "CUSTOM_GROUPS" : CUSTOM_GROUPS })
