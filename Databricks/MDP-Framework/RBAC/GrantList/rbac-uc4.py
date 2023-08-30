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
            ,"cleansed.ppm.0rpm_decision_guid_attr"
            ,"cleansed.ppm.rpm_item_d"
            ,"cleansed.ppm.ihpa"
            ,"cleansed.ppm.0project_attr"
            ,"cleansed.ppm.0wbs_elemt_attr"
            ,"cleansed.ppm.0inm_initiative_guid_attr"
            ,"cleansed.ppm.0rpm_port_guid_attr"
            ,"cleansed.ppm.rpm_relation_d"
            ,"cleansed.ppm.0rpm_decision_guid_id_text"
            ,"cleansed.ppm.0project_text"
            ,"cleansed.ppm.0rpm_bucket_guid_id_text"
            ,"cleansed.ppm.0rpm_port_guid_id_text"
            ,"cleansed.ppm.0wbs_elemt_text"
            ,"cleansed.ppm.0rpm_decision_id_text"
            ,"cleansed.ppm.0inm_initiative_guid_text"
            ,"cleansed.ppm.0rpm_item_type_text"
            ,"cleansed.ppm.rpm_status_t"
            ,"cleansed.ppm.tcj4t"
            ,"cleansed.ppm.zept_sub_delpart"
            ,"cleansed.art.art_assessment_comment"
            ,"cleansed.art.art_assessment_mxes_data"
            ,"cleansed.art.art_assessment_ws"
            ,"cleansed.art.art_attachment"
            ,"cleansed.art.art_concept"
            ,"cleansed.art.art_concept_asset"
            ,"cleansed.art.art_concept_comment"
            ,"cleansed.art.art_project_cashflow"
            ,"cleansed.art.art_project_details"
            ,"cleansed.art.attachment"
            ,"cleansed.art.renewals_assessment_mxes_data"
            ,"cleansed.art.renewals_assessment_workshop"
            ,"cleansed.art.renewals_assessment_ws_comment"
            ,"cleansed.art.renewals_concept"
            ,"cleansed.art.renewals_concept_asset"
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
            ,"curated.dim.assetcurrent"
            ,"curated.dim.assetcontractcurrent"
            ,"curated.dim.assetlocationancestorcurrent"
            ,"curated.dim.locationspeccurrent"
            ,"curated.dim.assetcontractcurrent"
            ,"curated.dim.assetmetercurrent"
            ,"curated.dim.assetspeccurrent"
            ,"curated.dim.workorderjobplancurrent"
            ,"curated.brg.workorderpreventivemaintenance"
            ,"curated.fact.assetperformanceindex"
            ,"curated.fact.preventivemaintenance"
            ,"curated.fact.workorder"
            ,"curated.fact.workorderfailurereport"
            ,"curated.fact.assetperformanceindexcurrent"
            ,"curated.fact.preventivemaintenancecurrent"
            ,"curated.fact.workordercurrent"
            ,"curated.fact.workorderfailurereportcurrent"
            ,"curated.fact.workordercurrent"
            ,"curated.dim.assetlocationancestorhierarchypivotcurrent"
            ,"curated.asset_performance.eppmselfserviceclassificationhierarchy"
            ,"curated.asset_performance.eppmselfservicereport"
            ,"curated.asset_performance.eppmSelfServiceMaster"
            ,"curated.asset_performance.eppmProjectStatusCommentary"
            ,"curated.asset_performance.referenceAssetTypeClass"
            ,"curated.asset_performance.referenceServiceType"
            ,"curated.asset_performance.referenceWorkType"
            ,"curated.asset_performanceasset_performance.referenceassettypeclass"
            ,"curated.asset_performance.referencereportconfiguration"
            ,"curated.asset_performance.referenceservicetype"
            ,"curated.asset_performance.referenceworktype"
        ]
        ,"Users" : []
        ,"AADGroups" : [ "cleansed-sensitive" ]
    },
    {
        "Name" : "L3-Sensitive-Other"
        ,"Level" : 3
        ,"ParentGroup" : "L2-Sensitive"
        ,"TableFilter" : [
            "cleansed.ppm.rpsco"
        ]
        ,"Users" : []
        ,"AADGroups" : [ "sensitive-others" ]
    }]

# COMMAND ----------

GenerateRbacCommands(RBAC_UC4)
