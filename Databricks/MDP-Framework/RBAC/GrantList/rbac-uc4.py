# Databricks notebook source
# MAGIC %run ../common-rbac

# COMMAND ----------

RBAC_UC4 = [
    {
        "Name" : "L1-Official"
        ,"Level" : 1
        ,"OtherCommands" : [
        ]
        ,"TableFilter" : [
            "cleansed.scada.point_cnfgn"
            ,"cleansed.scada.qlty_config"
            ,"cleansed.scada.rtu"
            ,"cleansed.scada.scxfield"
            ,"cleansed.scada.tsv"
            ,"cleansed.scada.tsv_point_cnfgn"
        ]
        ,"Users" : []
        ,"AADGroups" : [ "cleansed-official" ]
    },
    {
        "Name" : "L2-Sensitive"
        ,"Level" : 2
        ,"ParentGroup" : "L1-Official"
        ,"TableFilter" : [
            "cleansed.scada.event"
            ,"cleansed.scada.hierarchy_cnfgn"
            ,"cleansed.scada.point_limit"
            ,"cleansed.scada.scxuser"
            ,"curated.asset_performance.eppmselfserviceclassificationhierarchy"
            ,"curated.asset_performance.eppmselfservicereport"
            ,"curated.asset_performance.eppmSelfServiceMaster"
            ,"curated.asset_performance.eppmProjectStatusCommentary"
            ,"curated.dim.asset"
            ,"curated.dim.assetlocation"
            ,"curated.dim.assetlocationancestor"
            ,"curated.dim.locationspec"
            ,"curated.dim.assetcontract"
            ,"curated.dim.assetmeter"
            ,"curated.dim.assetspec"
            ,"curated.dim.workorderjobplan"
            ,"curated.dim.workorderproblemtype"
            ,"curated.dim.workorderproblemtypecurrent"
            ,"curated.fact.assetperformanceindex"
            ,"curated.fact.preventivemaintenance"
            ,"curated.fact.workorder"
            ,"curated.fact.workorderfailurereport"
            ,"curated.brg.workorderpreventivemaintenance"
            ,"curated.asset_performance.referenceAssetTypeClass"
            ,"curated.asset_performance.referenceServiceType"
            ,"curated.asset_performance.referenceWorkType"
            ,"curated.dim.assetcurrent"
            ,"curated.dim.assetcontractcurrent"
            ,"curated.dim.assetlocationancestorcurrent"
            ,"curated.dim.locationspeccurrent"
            ,"curated.dim.assetcontractcurrent"
            ,"curated.dim.assetmetercurrent"
            ,"curated.dim.assetspeccurrent"
            ,"curated.dim.workorderjobplancurrent"
            ,"curated.fact.assetperformanceindexcurrent"
            ,"curated.fact.preventivemaintenancecurrent"
            ,"curated.fact.workordercurrent"
            ,"curated.fact.workorderfailurereportcurrent"
            ,"curated.fact.workordercurrent"
            ,"curated.dim.assetlocationancestorhierarchypivotcurrent"
            ,"curated.asset_performance.referenceassettypeclass"
            ,"curated.asset_performance.referencereportconfiguration"
            ,"curated.asset_performance.referenceservicetype"
            ,"curated.asset_performance.referenceworktype"
        ]
        ,"Users" : []
        ,"AADGroups" : [ "cleansed-sensitive" ]
    }
]

# COMMAND ----------

GenerateRbacCommands(RBAC_UC4)
