# Databricks notebook source
# MAGIC %run ../common-rbac

# COMMAND ----------

RBAC_UC2 = [
    {
        "Name" : "L2-Sensitive"
        ,"Level" : 2
        ,"ParentGroup" : "L1-Official"
        ,"TableFilter" : [
            "cleansed.sharepointlistedp.demandcalculationadjustment"
            ,"curated.water_balance.demandwaternetwork"
            ,"curated.water_balance.factwaternetworkdemand"
            ,"curated.water_balance.factwaternetworkdemandwithexports"
            ,"curated.water_balance.manualdemand"
            ,"curated.water_balance.swcdemand"
            ,"curated.water_balance.systemexportvolume"
            ,"curated.fact.assetdemandvalue"
            ,"curated.fact.demand"
        ]
        ,"Users" : []
        ,"AADGroups" : [ "cleansed-sensitive" ]
    },
    {
        "Name" : "UC2-UAT"
        ,"TableFilter" : [
            "curated.water_balance.demandassetconfig"
            ,"curated.water_balance.demandcalculateconfig"
            ,"curated.water_balance.demandcalculatepriorityconfig"
            ,"curated.fact.dailysupplyapportionedaccruedconsumption"
            ,"curated.fact.dailysupplyapportionedconsumption"
            ,"curated.fact.monthlysupplyapportionedaggregate"
            ,"curated.fact.monthlysupplyapportionedaccruedconsumption"
            ,"curated.fact.monthlysupplyapportionedconsumption"
            ,"curated.fact.monthlyleakageaggregate"
            ,"curated.water_balance.monthlyLeakageAggregate"
            ,"curated.water_balance.recondailysupplyapportionedconsumption"
            ,"curated.water_balance.recondailysupplyapportionedaccruedconsumption"
            ,"curated.water_balance.monthlySupplyApportionedAggregate"
            ,"curated.fact.stoppedmeteraggregate"
            ,"curated.fact.stoppedmeteraggregate"
            ,"curated.fact.stoppedmeterconsumption"
            ,"curated.fact.consumptionaggregate"
            ,"curated.fact.unmeteredconsumption"
            ,"curated.water_balance.unmeteredconsumptionpropertytype"
            ,"curated.water_balance.unmeteredconsumptionsystemlevel"
            ,"curated.water_balance.unmeteredconsumptionwaternetwork"
            ,"curated.water_balance.manualdemandhistorical"
        ]
        ,"Users" : []
        ,"AADGroups" : []
    }
]
GenerateRbacCommands(RBAC_UC2)
