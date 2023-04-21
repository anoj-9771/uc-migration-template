# Databricks notebook source
#Curated view SWIRL Network
spark.sql("""
CREATE OR REPLACE VIEW curated.vw_swirl_network_incident AS
SELECT DISTINCT 
 inc.incidentNumber as incidentNumber
, cast(inc.incidentDate as date) as incidentDate
,inc.incidentStatus as incidentStatus
,inc.reasonForRecycledCancelledStatus as incRecycledOrCancelledStatus
,Net.incidentTypeStatus as incidentTypeStatus
,Net.ifIncidentStatusIsCancelledPleaseProvideReasonWhy as netCancelledReason
,inc.incidentShortDescription as shortDescription
,Net.confirmedAddressLocation
,Net.weather as weather
,Maxio.workOrderNumber
,Net.customerImpact
,Net.communityImpact
,Net.swcBusinessOperationsImpact
,Sys.systemName as network
,Net.incidentLocation as scamp
,Net.incidentSubType
,Net.impactType
,Net.waterwaysAffected
,null as waterways
,Net.incidentClass as product
,Assetin.numberOfProperties
,Assetin.manualVolumeCalculation
,Assetin.volumeCalculation
,Assetin.assetDescription
,Assetin.assetNumber
,Assetin.userAssetNumber
,Maxio.operationalArea
,Maxio.causeDescription
,Maxio.taskCode
,Maxio.scampSupplyZone as scampSupplyZone
,Maxio.networkDeliverySystemReceivingWaters as maximoDeliverySystem
,Net.swcResponsible
,Assetin.duration
,Maxio.workOrderPriority
,Net.doesTheIncidentResultInLicenceNonCompliance as isItNonCompliance
,StakeHolder.externalAgencyRegulator
,IEnv.incidentTypeId
,cast(Assetin.leakOverflowCeasedDate as date) as leakOverflowCeasedDate
,date_format(Assetin.leakOverflowCeasedTime2,'HH:mm') as leakOverflowCeasedTime2
,date_format(inc.incidentTime ,'HH:mm') as incidentTime
FROM cleansed.swirl_incident inc
INNER JOIN cleansed.swirl_incident_network Net on inc.id = Net.incident_FK 
LEFT JOIN cleansed.swirl_incident_environment IEnv on inc.id = IEnv.incident_FK  
LEFT JOIN cleansed.swirl_asset_information Assetin on inc.id = Assetin.incident_FK
LEFT JOIN cleansed.swirl_maximo_work_order_details Maxio on inc.id = Maxio.incident_FK  
LEFT JOIN cleansed.swirl_system sys on sys.id = Net.sydneyWaterSystemWhereIncidentOccurred_FK
LEFT JOIN cleansed.swirl_stakeholder_notification StakeHolder on Net.id = StakeHolder.networkIncident_FK
""")

# COMMAND ----------

#Curated view SWIRL licence noncompliance
spark.sql("""
CREATE OR REPLACE VIEW curated.vw_swirl_licence_noncompliance AS
SELECT DISTINCT 
 cast(inc.reportedDate as date) as reportedDate
,inc.incidentNumber as incidentNumber
, cast(inc.incidentDate as date) as incidentDate
,date_format(inc.incidentTime ,'HH:mm') as incidentTime
,inc.incidentShortDescription
,Maxio.workOrderNumber
,inc.incidentOwnerOrganisation as organisation
,IFNULL(concat(per.lastName,',',per.firstName,',',per.userName),'') as owner
,inc.incidentStatus as incidentStatus
,lic.licenceTitle as licenceName
,Maxio.networkDeliverySystemReceivingWaters as maximoDeliverySystem
,Net.doesTheIncidentResultInLicenceNonCompliance as isItNonCompliance
,liccl.title as licenceTitle
,liccl.description
FROM cleansed.swirl_incident inc
INNER JOIN cleansed.swirl_licence_noncompliance licnc on inc.id = licnc.incident_FK 
LEFT JOIN cleansed.swirl_licence lic on lic.id = licnc.licence_FK  
LEFT JOIN cleansed.swirl_maximo_work_order_details Maxio on inc.id = Maxio.incident_FK  
LEFT JOIN cleansed.swirl_incident_network Net on inc.id = Net.incident_FK 
LEFT JOIN cleansed.swirl_licence_clause liccl  on liccl.id = licnc.newLicenceClause_FK
LEFT JOIN cleansed.swirl_person per on inc.incidentProcessor_FK = per.id 
""")

# COMMAND ----------

#Curated view SWIRL open action report
spark.sql("""
CREATE OR REPLACE VIEW curated.vw_swirl_open_action AS
SELECT DISTINCT Dept.businessArea AS businessArea ,
       Dept.subDivision AS subDivision ,
       act.actionNumber AS actionNumber ,
       inc.incidentNumber AS incidentNumber ,
       cast(act.dateRaised AS date) AS dateRaised ,
       cast(act.dueDate AS date) AS dueDate ,
       act.priority AS actionPriority ,
       Imp.consequenceCategory AS consequenceCategory ,
       act.actionStatus as actionStatus , 
       inc.incidentShortDescription AS incShortDescription ,
       act.actionDescription AS actionDescription ,
       inc.initialActionTaken AS actionTaken ,
       act.completionDate AS dateClosed ,
       IFNULL(Concat(per.Lastname, ',', per.Firstname, ',', per.userName), '') AS actionsAsAssignedToPerson ,
       Imp.consequenceRating AS consequenceRatingWord ,
       Imp.consequenceRatingInteger AS consequenceRating ,
       Invest.typeOfInvestigation AS typeOfInvestigation ,
       Invest.investigationFindings AS investigationFindings ,
       Invest.investigationNumber AS investigationNumber ,
       act.actionTitle AS actionTitle ,
       Lesson.description AS lessonsLearnedDescription ,
       act.completionCategory AS completionCategory
FROM cleansed.swirl_action act
INNER JOIN cleansed.swirl_incident inc ON act.incidentAsSourceOfAction_FK = inc.id
LEFT JOIN
  (SELECT DISTINCT incident_FK,
                   consequenceCategory,
                   consequenceRating,
                   consequenceRatingInteger
   FROM cleansed.swirl_incident_impact) Imp ON Imp.incident_FK = inc.id
LEFT JOIN cleansed.swirl_person per ON inc.incidentProcessor_FK = per.id
LEFT JOIN
  (SELECT DISTINCT incidentAsSourceOfInvestigation_FK,
                   typeOfInvestigation,
                   investigationFindings,
                   investigationNumber
   FROM cleansed.swirl_investigation)Invest ON Invest.incidentAsSourceOfInvestigation_FK = inc.id
LEFT JOIN
  (SELECT DISTINCT incidentAsSourceOfLessonsLearned_FK,
                   description
   FROM cleansed.swirl_lessons_learned) Lesson ON Lesson.incidentAsSourceOfLessonsLearned_FK = Inc.id
LEFT JOIN
  (SELECT DISTINCT BA.id AS BAID,
                   BA.organisationalUnit AS businessArea,
                   SD.id AS SDID,
                   SD.organisationalUnit AS subDivision
   FROM cleansed.swirl_department BA
   LEFT JOIN cleansed.swirl_department SD ON BA.parentOrganisationalUnit_FK = SD.id) Dept ON Inc.organisationalUnitPrimaryResponsibility_FK = Dept.BAID
""")

# COMMAND ----------

#Curated view SWIRL open incident report
spark.sql("""
CREATE OR REPLACE VIEW curated.vw_swirl_open_incident AS
SELECT DISTINCT inc.incidentNumber AS incidentNumber ,
     inc.incidentStatus AS incidentStatus ,
     NULL as incidentEventType,
     NULL as severity,
     cast(inc.incidentDate as date) as incidentDate,
     cast(inc.reportedDate as date) as reportedDate,
     inc.incidentShortDescription AS incShortDescription ,
     inc.summary as incidentSummary ,
     loc.location as incidentLocation ,
     inc.incidentLocation as locationOther , 
     per.sydneyWaterDepartmentId as incidentOwnerResponsible ,   
     IFNULL(Concat(per.Lastname, ',', per.Firstname, ',', per.userName), '') AS owner ,
     IFNULL(Concat(ent.Lastname, ',', ent.Firstname, ',', ent.userName), '') AS enteredBy ,
     inc.incidentOwnerOrganisation as incidentOwnerOrganisation , 
     inc.itemsToCompletePriorToIncidentClose as itemsToCompletePriorToIncidentClose , 
     per.sydneyWaterBusinessUnit as businessUnit ,
     imp.consequenceCategory as consequenceCategory
FROM cleansed.swirl_incident inc 
LEFT JOIN cleansed.swirl_person per ON per.id = inc.incidentProcessor_FK
LEFT JOIN cleansed.swirl_person ent ON ent.id = inc.enteredBy_FK
LEFT JOIN cleansed.swirl_location loc ON loc.id = inc.location_FK
LEFT JOIN 
 (SELECT DISTINCT incident_FK,
                   consequenceCategory 
  FROM cleansed.swirl_incident_impact) imp ON imp.incident_FK = inc.id
--LEFT JOIN 
--cleansed.swirl_injury_details inj ON inj.incident_FK = inc.id
""")

# COMMAND ----------

#Curated view SWIRL bypass and reportable incidents
spark.sql("""
CREATE OR REPLACE VIEW curated.vw_swirl_bypass_and_reportable_incident AS
SELECT DISTINCT 
 inc.incidentNumber
,NULL as incidentEventType
,inc.incidentShortDescription as incidentShortDescription
,bps.doesTheIncidentResultInLicenceNonCompliance as isItNonCompliance
, cast(inc.incidentDate as date) as incidentDate
,cast(stnf.dateNotificationReported as date) as dateNotificationReported
,date_format(inc.incidentTime ,'HH:mm') as incidentTime
,date_format(stnf.timeNotificationReported ,'HH:mm') as timeNotificationReported
,stnf.externalAgencyRegulator as externalAgencyRegulator
,stnf.externalStakeholderContactChannel as externalStakeholderContactChannel
,stnf.externalStakeholderReferenceNumber as externalStakeholderReferenceNumber
,stnf.notificationNumber as notificationNumber
,stnf.notificationDetails as notificationDetails
,stnf.notificationType as notificationType
,bps.sendEmailNotificationToDepartmentOfHealthCode as sendEmailNotificationToDepartmentOfHealthCode
,licnc.commentsToExternalStakeholders as commentsToExternalStakeholders
,log.notificationMethod as notificationMethod
,date_format(log.timeNotified ,'HH:mm') as timeNotified
,cast(log.dateNotified as date) as dateNotified
,env_req.regulationOrConditionNotCompliedWith as regulationOrConditionNotCompliedWith
,env_req.regulatoryNoticeYesNoPotentially as regulatoryNoticeYesNoPotentially
,reg_notice.regulatoryNoticeType as regulatoryNoticeType
,file.fileName as fileName
,bps.cause as cause
,bps.incidentClass as incidentClass
,bps.potentialPublicHealthImpact as potentialPublicHealthImpact
,NULL as treatmentType 
FROM cleansed.swirl_incident inc
INNER JOIN cleansed.swirl_incident_bypass_and_partial_treatment bps on inc.id = bps.incident_FK 
LEFT JOIN cleansed.swirl_stakeholder_notification stnf on bps.id = stnf.bypassIncident_FK
LEFT JOIN cleansed.swirl_licence_noncompliance licnc on inc.id = licnc.incident_FK 
LEFT JOIN cleansed.swirl_notification_log log on inc.id = log.incident_FK  
LEFT JOIN ( SELECT env.incident_FK as incident_FK
                  ,req.id as environmentRequirementsNotMetId
                  ,req.regulationOrConditionNotCompliedWith
                  ,req.regulatoryNoticeYesNoPotentially
            FROM cleansed.swirl_incident_environment env
            INNER JOIN 
            cleansed.swirl_incident_environment_requirements_not_met req
            ON env.id = req.environmentIncident_FK
          ) env_req ON inc.id = env_req.incident_FK
LEFT JOIN cleansed.swirl_regulatory_notice_received reg_notice on env_req.environmentRequirementsNotMetId = reg_notice.environmentRequirementsNotMet_FK
LEFT JOIN ( SELECT invest.incidentAsSourceOfInvestigation_FK as incidentAsSourceOfInvestigation_FK
                   ,file.investigation_FK as investigation_FK
                   ,file.fileName as fileName
            FROM cleansed.swirl_investigation invest
            INNER JOIN
            cleansed.swirl_file file 
            ON invest.id = file.investigation_FK
          ) file ON inc.id = file.incidentAsSourceOfInvestigation_FK
--Left join cleansed.swirl_treatment_plan treat_plan on 

""")
