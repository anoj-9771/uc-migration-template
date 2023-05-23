# Databricks notebook source
RBAC_UC3 = [
    {
        "Name" : "L1-Official"
        ,"Level" : 1
        ,"OtherCommands" : [
        ]
        ,"TableFilter" : [
            "curated.dimdate"
            "curated_v3.dimdate"
            ,"curated_v3.dimsewernetwork"
            ,"curated_v3.dimstormwaternetwork"
            ,"curated_v3.dimwaternetwork"
            ,"curated_v3.dimcustomerinteractionstatus"
            ,"curated_v3.dimcustomerserviceattachmentinfo"
            ,"curated_v3.dimcustomerservicecategory"
            ,"curated_v3.dimcustomerserviceprocesstype"
            ,"curated_v3.dimcustomerservicerequeststatus"
            ,"curated_v3.dimcommunicationchannel"
            ,"curated_v3.dimsurvey"
            ,"curated_v3.dimsurveyquestion"
            ,"curated_v3.dimsurveyresponseinformation"
            ,"curated_v3.bridgecustomerinteractionattachment"
            ,"curated_v3.bridgecustomerinteractionemail"
            ,"curated_v3.bridgecustomerinteractionservicerequest"
            ,"curated_v3.bridgecustomerinteractionworknotesummary"
            ,"curated_v3.bridgecustomerservicerequestattachment"
            ,"curated_v3.bridgecustomerservicerequestemail"
            ,"curated_v3.bridgecustomerservicerequestinteraction"
            ,"curated_v3.bridgecustomerservicerequestsurvey"
            ,"curated_v3.bridgecustomerservicerequesttoservicerequest"
            ,"curated_v3.bridgecustomerservicerequestworknoteresolution"
            ,"curated_v3.bridgecustomerservicerequestworknotesummary"
        ]
        ,"Users" : []
        ,"AADGroups" : [ "cleansed-official" ]
    },
    {
        "Name" : "L2-Sensitive"
        ,"Level" : 2
        ,"ParentGroup" : "L1-Official"
        ,"TableFilter" : [
            "curated.dimcontract"
            ,"curated.dimlocation"
            ,"curated.dimproperty"
            ,"curated.dimcontract"
            ,"curated_v3.dimbusinesspartner"
            ,"curated_v3.dimlocation"
            ,"curated_v3.dimproperty"
            ,"curated_v3.dimbusinesspartner"
            ,"curated_v3.factcustomerinteraction"
            ,"curated_v3.factcustomerservicerequest"
            ,"curated_v3.dimsurveyparticipant"
            ,"curated_v3.factsurveymiscellaneousinformation"
            ,"curated_v3.factsurveyresponse"
        ]
        ,"Users" : []
        ,"AADGroups" : [ "cleansed-sensitive" ]
    },    
    {
        "Name" : "L3-Sensitive-Other"
        ,"Level" : 3
        ,"ParentGroup" : "L2-Sensitive"
        ,"TableFilter" : [
            "cleansed.swirl*"
            ,"cleansed.vw_swirl*"
            ,"curated.viewswirl*"
            ,"curated_v3.dimcustomerserviceemailheader"
            ,"curated_v3.factcustomerserviceemailevent"
            ,"curated_v3.factcustomerserviceworknote"

        ]
        ,"Users" : []
        ,"AADGroups" : [ "sensitive-others" ]
    }
]

# COMMAND ----------


