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
        ]
        ,"Users" : []
        ,"AADGroups" : [ "cleansed-sensitive" ]
    },
    {
        "Name" : "UC2-UAT"
        ,"TableFilter" : [
            "curated.fact.assetdemandvalue"
            ,"curated.fact.demand"
            ,"curated.fact.stoppedmeteraggregate"
            ,"curated.fact.stoppedmeteraggregate"
            ,"curated.fact.stoppedmeterconsumption"
            ,"curated.fact.consumptionaggregate"
            ,"curated.fact.unmeteredconsumption"
            ,"curated.water_balance.unmeteredconsumptionpropertytype"
            ,"curated.water_balance.unmeteredconsumptionsystemlevel"
            ,"curated.water_balance.unmeteredconsumptionwaternetwork"
        ]
        ,"Users" : []
        ,"AADGroups" : []
    }
]
GenerateRbacCommands(RBAC_UC2)

# COMMAND ----------


