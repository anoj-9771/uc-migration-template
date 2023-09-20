# Databricks notebook source
# MAGIC %run ../common-rbac

# COMMAND ----------

RBAC_UC3 = [
    {
        "Name" : "L1-Official"
        ,"Level" : 1
        ,"OtherCommands" : [
        ]
        ,"TableFilter" : [
            "curated.dim.customerinteractionstatus"
            ,"curated.dim.customerserviceattachmentinfo"
            ,"curated.dim.customerservicecategory"
            ,"curated.dim.customerserviceprocesstype"
            ,"curated.dim.customerservicerequeststatus"
            ,"curated.dim.communicationchannel"
            ,"curated.dim.survey"
            ,"curated.dim.surveyquestion"
            ,"curated.dim.surveyresponseinformation"
            ,"curated.dim.date"
            ,"curated.dim.sewernetwork"
            ,"curated.dim.stormwaternetwork"
            ,"curated.dim.waternetwork"
            ,"curated.brg.customerinteractionattachment"
            ,"curated.brg.customerinteractionemail"
            ,"curated.brg.customerinteractionservicerequest"
            ,"curated.brg.customerinteractionworknotesummary"
            ,"curated.brg.customerservicerequestattachment"
            ,"curated.brg.customerservicerequestemail"
            ,"curated.brg.customerservicerequestinteraction"
            ,"curated.brg.customerservicerequestsurvey"
            ,"curated.brg.customerservicerequesttoservicerequest"
            ,"curated.brg.customerservicerequestworknoteresolution"
            ,"curated.brg.customerservicerequestworknotesummary"
        ]
        ,"Users" : []
        ,"AADGroups" : [ "cleansed-official" ]
    },
    {
        "Name" : "L2-Sensitive"
        ,"Level" : 2
        ,"ParentGroup" : "L1-Official"
        ,"TableFilter" : [
            "curated.dim.customerserviceemailheader"
            ,"curated.fact.customerinteraction"
            ,"curated.fact.customerserviceemailevent"
            ,"curated.fact.customerservicerequest"
            ,"curated.fact.customerserviceworknote"
            ,"curated.dim.surveyparticipant"
            ,"curated.fact.surveymiscellaneousinformation"
            ,"curated.fact.surveyresponse"
            ,"curated.dim.contract"
            ,"curated.dim.location"
            ,"curated.dim.property"
            ,"curated.dim.businesspartner"
            ,"curated.dim.businesspartnerrelation"
            ,"curated.dim.businesspartnergroupl2"
        ]
        ,"Users" : []
        ,"AADGroups" : [ "cleansed-sensitive" ]
    },    
    {
        "Name" : "L3-Sensitive-Other"
        ,"Level" : 3
        ,"ParentGroup" : "L2-Sensitive"
        ,"TableFilter" : [
            "cleansed.swirl_*"
            ,"curated.asset_performance.swirlallincidentsandevents"
            ,"curated.asset_performance.swirlopenaction"
            ,"curated.asset_performance.swirlopenincident"
        ]
        ,"Users" : []
        ,"AADGroups" : [ "sensitive-others" ]
    }
]

# COMMAND ----------

GenerateRbacCommands(RBAC_UC3)
