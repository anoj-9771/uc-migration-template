# Databricks notebook source
 spark.sql("""
CREATE OR REPLACE VIEW cleansed.vw_swirl_ref_lookup AS 
SELECT DISTINCT 
   mli.lookupItemId as lookupItemId,
   li.lookupId as lookupId,
    mli.componentEntityId as incidentId, 
    l.lookupName as lookupName,
    li.description as description
FROM cleansed.swirl_multi_lookup_items mli
INNER JOIN cleansed.swirl_lookup_items li ON mli.lookupItemId = li.lookupitemid
INNER JOIN cleansed.swirl_lookups l ON li.lookupid = l.lookupId
WHERE mli.lookupItemId <> '0'
""")

# COMMAND ----------

 spark.sql("""
CREATE OR REPLACE VIEW cleansed.vw_swirl_ref_nfcomponents AS 
SELECT DISTINCT nfc1.componentName as headTable,
nfc2.componentName as childTable,
rel.twoDescription as headDescription,
rel.oneDescription as childDescription,
cl.fromId as headId,
cl.toId as childId 
FROM cleansed.swirl_nf_components nfc1
INNER JOIN cleansed.swirl_relationships rel ON nfc1.componentId = rel.compidOne
INNER JOIN cleansed.swirl_nf_components nfc2 ON nfc2.componentId = rel.compidTwo
INNER JOIN cleansed.swirl_componentlinks cl ON cl.relationshipId = rel.relationshipId
""")


# COMMAND ----------

#Curated view SWIRL Network
spark.sql("""
CREATE OR REPLACE VIEW curated.viewswirlnetworkincident AS
SELECT DISTINCT 
 inc.incidentNumber as incidentNumber
, cast(inc.incidentDate as date) as incidentDate
,inc.incidentStatus as incidentStatus
,inc.reasonForRecycledCancelledStatus as incRecycledOrCancelledStatus
,Net.incidentTypeStatus as incidentTypeStatus
,Net.ifIncidentStatusIsCancelledPleaseProvideReasonWhy as netCancelledReason
,inc.incidentShortDescription as incidentShortDescription
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
,waterwaysAffected.waterSource as waterways
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
LEFT JOIN ( SELECT DISTINCT ref.headId,ref.childId, w.waterSource 
            FROM cleansed.vw_swirl_ref_nfcomponents ref
            INNER JOIN cleansed.swirl_water_source w
            ON ref.childId = w.id 
            WHERE UPPER(ref.headTable) = UPPER('Incident - Network') 
            AND UPPER(ref.childDescription) = UPPER('Waterways Affected')
          ) waterwaysAffected ON Net.id = waterwaysAffected.headId
WHERE inc.incidentNumber IS NOT NULL 
""")

# COMMAND ----------

#Curated view SWIRL licence noncompliance
spark.sql("""
CREATE OR REPLACE VIEW curated.viewswirllicencenoncompliance AS
SELECT DISTINCT 
 cast(inc.reportedDate as date) as reportedDate
,inc.incidentNumber as incidentNumber
, cast(inc.incidentDate as date) as incidentDate
,date_format(inc.incidentTime ,'HH:mm') as incidentTime
,inc.incidentShortDescription
,Maxio.workOrderNumber
,inc.incidentOwnerOrganisation as organisation
,concat_ws(',',per.Lastname,per.Firstname,per.userName) as owner
,inc.incidentStatus as incidentStatus
,lic.licenceTitle as licenceName
,Maxio.networkDeliverySystemReceivingWaters as maximoDeliverySystem
,Net.doesTheIncidentResultInLicenceNonCompliance as isItNonCompliance
,liccl.title as licenceTitle
,liccl.description
,licnc.causeOfNonCompliance as causeOfNonCompliance
,licnc.commentsToExternalStakeholders as commentsToExternalStakeholders
,licnc.mitigationActionsActionsTakenToLessenTheEffect as mitigationActionsActionsTakenToLessenTheEffect
,licnc.preventionActions as preventionActions
,lic.systemLicenceNumber as licenceNumber
,lic.licenceType as licenceType
,concat_ws(',',lic.systemLicenceNumber,lic.licenceType,lic.licenceTitle) as licenceDetails 
FROM cleansed.swirl_incident inc
INNER JOIN cleansed.swirl_licence_noncompliance licnc on inc.id = licnc.incident_FK 
LEFT JOIN cleansed.swirl_licence lic on lic.id = licnc.licence_FK  
LEFT JOIN cleansed.swirl_maximo_work_order_details Maxio on inc.id = Maxio.incident_FK  
LEFT JOIN cleansed.swirl_incident_network Net on inc.id = Net.incident_FK 
LEFT JOIN cleansed.swirl_licence_clause liccl  on liccl.id = licnc.newLicenceClause_FK
LEFT JOIN cleansed.swirl_person per on inc.incidentProcessor_FK = per.id 
WHERE inc.incidentNumber IS NOT NULL
""")

# COMMAND ----------

#Curated view SWIRL open action report -personal
spark.sql("""
CREATE OR REPLACE VIEW curated.viewswirlopenactionpersonal AS
SELECT DISTINCT Dept.businessArea AS businessArea ,
       Dept.subDivision AS subDivision ,
       act.actionNumber AS actionNumber ,
       inc.incidentNumber AS incidentNumber ,
       cast(act.dateRaised AS date) AS dateRaised ,
       cast(act.dueDate AS date) AS dueDate ,
       act.priority AS actionPriority ,
       Imp.consequenceCategory AS consequenceCategory ,
       act.actionStatus as actionStatus , 
       inc.incidentShortDescription AS incidentShortDescription ,
       act.actionDescription AS actionDescription ,
       inc.initialActionTaken AS actionTaken ,
       act.completionDate AS dateClosed ,
       concat_ws(',',per.Lastname,per.Firstname,per.userName) AS actionsAsAssignedToPerson ,
       Imp.consequenceRating AS consequenceRatingWord ,
       Imp.consequenceRatingInteger AS consequenceRating ,
       Invest.typeOfInvestigation AS typeOfInvestigation ,
       Invest.investigationFindings AS investigationFindings ,
       Invest.investigationNumber AS investigationNumber ,
       act.actionTitle AS actionTitle ,
       Lesson.description AS lessonsLearnedDescription ,
       act.completionCategory AS completionCategory ,
       eventType.incidentEventType as incidentEventType
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
LEFT JOIN (SELECT DISTINCT incidentId 
                           ,array_join(collect_set(description), ',')  as incidentEventType 
           FROM cleansed.vw_swirl_ref_lookup
           WHERE UPPER(lookupName) = UPPER('Incident Event Type')
           AND lookupItemId <> '0' 
           AND description IS NOT NULL
           GROUP BY incidentId
          ) eventType ON inc.id = eventType.incidentId
WHERE inc.incidentNumber IS NOT NULL 
AND EXISTS (SELECT 1 
            FROM cleansed.vw_swirl_ref_lookup as inner_tbl
            WHERE UPPER(lookupName) = UPPER('Incident Event Type')
            AND lookupItemId <> '0'
            AND UPPER(description) IN ('PERSONNEL', 'HEALTH AND SAFETY')
            AND inner_tbl.incidentId = inc.id)
""")

# COMMAND ----------

#Curated view SWIRL open action report -non personal
spark.sql("""
CREATE OR REPLACE VIEW curated.viewswirlopenactionnonpersonal AS
SELECT DISTINCT Dept.businessArea AS businessArea ,
       Dept.subDivision AS subDivision ,
       act.actionNumber AS actionNumber ,
       inc.incidentNumber AS incidentNumber ,
       cast(act.dateRaised AS date) AS dateRaised ,
       cast(act.dueDate AS date) AS dueDate ,
       act.priority AS actionPriority ,
       Imp.consequenceCategory AS consequenceCategory ,
       act.actionStatus as actionStatus , 
       inc.incidentShortDescription AS incidentShortDescription ,
       act.actionDescription AS actionDescription ,
       inc.initialActionTaken AS actionTaken ,
       act.completionDate AS dateClosed ,
       concat_ws(',',per.Lastname,per.Firstname,per.userName) AS actionsAsAssignedToPerson ,
       Imp.consequenceRating AS consequenceRatingWord ,
       Imp.consequenceRatingInteger AS consequenceRating ,
       Invest.typeOfInvestigation AS typeOfInvestigation ,
       Invest.investigationFindings AS investigationFindings ,
       Invest.investigationNumber AS investigationNumber ,
       act.actionTitle AS actionTitle ,
       Lesson.description AS lessonsLearnedDescription ,
       act.completionCategory AS completionCategory ,
       eventType.incidentEventType as incidentEventType
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
LEFT JOIN (SELECT DISTINCT incidentId 
                           ,array_join(collect_set(description), ',')  as incidentEventType 
           FROM cleansed.vw_swirl_ref_lookup
           WHERE UPPER(lookupName) = UPPER('Incident Event Type')
           AND lookupItemId <> '0' 
           AND description IS NOT NULL
           GROUP BY incidentId
          ) eventType ON inc.id = eventType.incidentId
WHERE inc.incidentNumber IS NOT NULL 
AND NOT EXISTS (SELECT 1 
            FROM cleansed.vw_swirl_ref_lookup as inner_tbl
            WHERE UPPER(lookupName) = UPPER('Incident Event Type')
            AND lookupItemId <> '0'
            AND UPPER(description) IN ('PERSONNEL', 'HEALTH AND SAFETY')
            AND inner_tbl.incidentId = inc.id)
""")

# COMMAND ----------

#Curated view SWIRL open incident report-personal
spark.sql("""
CREATE OR REPLACE VIEW curated.viewswirlopenincidentpersonal AS
SELECT DISTINCT inc.incidentNumber AS incidentNumber ,
     inc.incidentStatus AS incidentStatus ,
     eventType.incidentEventType as incidentEventType,
     --inj.severityOfTheInjury as severity,
     cast(inc.incidentDate as date) as incidentDate,
     cast(inc.reportedDate as date) as reportedDate,
     inc.incidentShortDescription AS incidentShortDescription ,
     inc.summary as incidentSummary ,
     loc.location as incidentLocation ,
     inc.incidentLocation as locationOther , 
     per.sydneyWaterDepartmentId as incidentOwnerResponsible ,   
     concat_ws(',',per.Lastname,per.Firstname,per.userName) AS owner ,
     concat_ws(',',ent.Lastname,ent.Firstname,ent.userName) AS enteredBy ,
     inc.incidentOwnerOrganisation as incidentOwnerOrganisation , 
     inc.itemsToCompletePriorToIncidentClose as itemsToCompletePriorToIncidentClose , 
     per.sydneyWaterBusinessUnit as businessUnit ,
     imp.consequenceCategory as consequenceCategory, 
     CASE WHEN UPPER(inc.incidentStatus) = 'OPEN' THEN datediff(CAST(current_date() as DATE),CAST(inc.incidentDate as date)) 
          ELSE 0
     END as overdueGreaterThan45Days
FROM cleansed.swirl_incident inc 
LEFT JOIN cleansed.swirl_person per ON per.id = inc.incidentProcessor_FK
LEFT JOIN cleansed.swirl_person ent ON ent.id = inc.enteredBy_FK
LEFT JOIN cleansed.swirl_location loc ON loc.id = inc.location_FK
LEFT JOIN 
 (SELECT DISTINCT incident_FK,
                   consequenceCategory 
  FROM cleansed.swirl_incident_impact) imp ON imp.incident_FK = inc.id
--LEFT JOIN 
--(SELECT DISTINCT incident_FK,severityOfTheInjury 
-- FROM cleansed.swirl_injury) inj ON inj.incident_FK = inc.id 
LEFT JOIN (SELECT DISTINCT incidentId 
                           ,array_join(collect_set(description), ',')  as incidentEventType 
           FROM cleansed.vw_swirl_ref_lookup
           WHERE UPPER(lookupName) = UPPER('Incident Event Type')
           AND lookupItemId <> '0' 
           AND description IS NOT NULL
           GROUP BY incidentId
          ) eventType ON inc.id = eventType.incidentId
WHERE inc.incidentNumber IS NOT NULL 
AND EXISTS (SELECT 1 
            FROM cleansed.vw_swirl_ref_lookup as inner_tbl
            WHERE UPPER(lookupName) = UPPER('Incident Event Type')
            AND lookupItemId <> '0'
            AND UPPER(description) IN ('PERSONNEL', 'HEALTH AND SAFETY')
            AND inner_tbl.incidentId = inc.id)
""")

# COMMAND ----------

#Curated view SWIRL open incident report-non personal
spark.sql("""
CREATE OR REPLACE VIEW curated.viewswirlopenincidentnonpersonal AS
SELECT DISTINCT inc.incidentNumber AS incidentNumber ,
     inc.incidentStatus AS incidentStatus ,
     eventType.incidentEventType as incidentEventType,
     --inj.severityOfTheInjury as severity,
     cast(inc.incidentDate as date) as incidentDate,
     cast(inc.reportedDate as date) as reportedDate,
     inc.incidentShortDescription AS incidentShortDescription ,
     inc.summary as incidentSummary ,
     loc.location as incidentLocation ,
     inc.incidentLocation as locationOther , 
     per.sydneyWaterDepartmentId as incidentOwnerResponsible ,   
     concat_ws(',',per.Lastname,per.Firstname,per.userName) AS owner ,
     concat_ws(',',ent.Lastname,ent.Firstname,ent.userName) AS enteredBy ,
     inc.incidentOwnerOrganisation as incidentOwnerOrganisation , 
     inc.itemsToCompletePriorToIncidentClose as itemsToCompletePriorToIncidentClose , 
     per.sydneyWaterBusinessUnit as businessUnit ,
     imp.consequenceCategory as consequenceCategory,
     CASE WHEN UPPER(inc.incidentStatus) = 'OPEN' THEN datediff(CAST(current_date() as DATE),CAST(inc.incidentDate as date)) 
          ELSE 0
     END as overdueGreaterThan45Days
FROM cleansed.swirl_incident inc 
LEFT JOIN cleansed.swirl_person per ON per.id = inc.incidentProcessor_FK
LEFT JOIN cleansed.swirl_person ent ON ent.id = inc.enteredBy_FK
LEFT JOIN cleansed.swirl_location loc ON loc.id = inc.location_FK
LEFT JOIN 
 (SELECT DISTINCT incident_FK,
                   consequenceCategory 
  FROM cleansed.swirl_incident_impact) imp ON imp.incident_FK = inc.id
--LEFT JOIN 
--(SELECT DISTINCT incident_FK,severityOfTheInjury 
-- FROM cleansed.swirl_injury) inj ON inj.incident_FK = inc.id 
LEFT JOIN (SELECT DISTINCT incidentId 
                           ,array_join(collect_set(description), ',')  as incidentEventType 
           FROM cleansed.vw_swirl_ref_lookup
           WHERE UPPER(lookupName) = UPPER('Incident Event Type')
           AND lookupItemId <> '0' 
           AND description IS NOT NULL
           GROUP BY incidentId
          ) eventType ON inc.id = eventType.incidentId
WHERE inc.incidentNumber IS NOT NULL 
AND NOT EXISTS (SELECT 1 
            FROM cleansed.vw_swirl_ref_lookup as inner_tbl
            WHERE UPPER(lookupName) = UPPER('Incident Event Type')
            AND lookupItemId <> '0'
            AND UPPER(description) IN ('PERSONNEL', 'HEALTH AND SAFETY')
            AND inner_tbl.incidentId = inc.id)
""")

# COMMAND ----------

#Curated view SWIRL bypass and reportable incidents
#spark.sql("""
#CREATE OR REPLACE VIEW curated.viewswirlbypassandreportableincidents AS
#SELECT DISTINCT 
# inc.incidentNumber
#,NULL as incidentEventType
#,inc.incidentShortDescription as incidentShortDescription
#,bps.doesTheIncidentResultInLicenceNonCompliance as isItNonCompliance
#, cast(inc.incidentDate as date) as incidentDate
#,cast(stnf.dateNotificationReported as date) as dateNotificationReported
#,date_format(inc.incidentTime ,'HH:mm') as incidentTime
#,date_format(stnf.timeNotificationReported ,'HH:mm') as timeNotificationReported
#,stnf.externalAgencyRegulator as externalAgencyRegulator
#,stnf.externalStakeholderContactChannel as externalStakeholderContactChannel
#,stnf.externalStakeholderReferenceNumber as externalStakeholderReferenceNumber
#,stnf.notificationNumber as notificationNumber
#,stnf.notificationDetails as notificationDetails
#,stnf.notificationType as notificationType
#,bps.sendEmailNotificationToDepartmentOfHealthCode as sendEmailNotificationToDepartmentOfHealthCode
#,licnc.commentsToExternalStakeholders as commentsToExternalStakeholders
#,log.notificationMethod as notificationMethod
#,date_format(log.timeNotified ,'HH:mm') as timeNotified
#,cast(log.dateNotified as date) as dateNotified
#,env_req.regulationOrConditionNotCompliedWith as regulationOrConditionNotCompliedWith
#,env_req.regulatoryNoticeYesNoPotentially as regulatoryNoticeYesNoPotentially
#,reg_notice.regulatoryNoticeType as regulatoryNoticeType
#,file.fileName as fileName
#,bps.cause as cause
#,bps.incidentClass as incidentClass
#,bps.potentialPublicHealthImpact as potentialPublicHealthImpact
#,NULL as treatmentType 
#FROM cleansed.swirl_incident inc
#INNER JOIN cleansed.swirl_incident_bypass_and_partial_treatment bps on inc.id = bps.incident_FK 
#LEFT JOIN cleansed.swirl_stakeholder_notification stnf on bps.id = stnf.bypassIncident_FK
#LEFT JOIN cleansed.swirl_licence_noncompliance licnc on inc.id = licnc.incident_FK 
#LEFT JOIN cleansed.swirl_notification_log log on inc.id = log.incident_FK  
#LEFT JOIN ( SELECT env.incident_FK as incident_FK
#                  ,req.id as environmentRequirementsNotMetId
#                  ,req.regulationOrConditionNotCompliedWith
#                  ,req.regulatoryNoticeYesNoPotentially
#            FROM cleansed.swirl_incident_environment env
#            INNER JOIN 
#            cleansed.swirl_incident_environment_requirements_not_met req
#            ON env.id = req.environmentIncident_FK
#          ) env_req ON inc.id = env_req.incident_FK
#LEFT JOIN cleansed.swirl_regulatory_notice_received reg_notice on env_req.environmentRequirementsNotMetId = reg_notice.environmentRequirementsNotMet_FK
#LEFT JOIN ( SELECT invest.incidentAsSourceOfInvestigation_FK as incidentAsSourceOfInvestigation_FK
#                   ,file.investigation_FK as investigation_FK
#                   ,file.fileName as fileName
#            FROM cleansed.swirl_investigation invest
#            INNER JOIN
#            cleansed.swirl_file file 
#            ON invest.id = file.investigation_FK
#          ) file ON inc.id = file.incidentAsSourceOfInvestigation_FK
#--Left join cleansed.swirl_treatment_plan treat_plan on 
#WHERE inc.incidentNumber IS NOT NULL
#AND EXISTS (SELECT 1 
#            FROM cleansed.vw_swirl_ref_lookup as inner_tbl
#            WHERE UPPER(lookupName) = UPPER('Incident Event Type')
#            AND lookupItemId <> '0'
#            AND UPPER(description) IN ('PERSONNEL', 'HEALTH AND SAFETY')
#            AND inner_tbl.incidentId = inc.id)
#""")

# COMMAND ----------

#All incidents and events -personal
spark.sql("""
CREATE OR REPLACE VIEW curated.viewswirlallincidentsandeventspersonal AS 
SELECT DISTINCT inc.id,
inc.incidentNumber
,cast(incidentDate as date) as incidentDate
,cast(reportedDate as date) as recordedDate
,cast(closeIncidentDate as date) as dateClosed
,incidentStatus
,incidentCategory
,incidentShortDescription
,inc.consequenceRating
,inc.incidentOwnerOrganisation as incidentOwnerOrganisation
,Dep.organisationalUnit as businessArea
,per.sydneyWaterBusinessUnit as businessLocation
,incidentLocation
,concat_ws(',',per.Lastname,per.Firstname,per.userName) as incidentOwner
,concat_ws(',',report.Lastname,report.Firstname,report.userName) as reportedBy
,Imp.isTheIncidentImpact as actualPotential
,Imp.consequenceRating as consequenceRatingWord
,inc.riskRating
,Imp.consequenceCategory as consequenceCategory
,rootcause.rootCause
,rootcause.rootCauseDescription
,Lesson.description as lessonLearntDescription
,Imp.consequenceRatingInteger as consequenceRatingInteger
,Bypass.doesTheIncidentResultInLicenceNonCompliance as bypassNonCompliance
,treatmentType.treatmentType as bypassTreatmentType
,Bypass.volumeBypassed as bypassVolumeBypassed
,Bypass.volumeTreated as bypassVolumeTreated
,pod.name as bypassPointOfDischarge
,ReceivingWaterway.name as bypassReceivingWaterway
,inc.isAnInvestigationRequired as investigationRequired
,bypass.weather as weather
FROM cleansed.swirl_incident inc
LEFT JOIN cleansed.swirl_person per on inc.incidentProcessor_FK = per.id
LEFT JOIN cleansed.swirl_person report on inc.reportedBy_FK = report.id
LEFT JOIN cleansed.swirl_action act ON act.incidentAsSourceOfAction_FK = inc.id
LEFT JOIN cleansed.swirl_root_cause_analysis rootcause ON rootcause.incident_FK = inc.id
LEFT JOIN cleansed.swirl_incident_bypass_and_partial_treatment bypass ON bypass.incident_FK = Inc.id
LEFT JOIN
(SELECT DISTINCT incidentId ,
                 array_join(collect_set(description), ',')  as treatmentType 
 FROM cleansed.vw_swirl_ref_lookup
 WHERE UPPER(lookupName) = UPPER('Treatment Type')
 AND lookupItemId <> '0' 
 AND description IS NOT NULL
 GROUP BY incidentId 
) treatmentType on treatmentType.incidentId = inc.id
LEFT JOIN cleansed.swirl_pre_treatment ReceivingWaterway on ReceivingWaterway.id = bypass.receivingWaterway_FK
LEFT JOIN cleansed.swirl_pre_treatment pod on pod.id = bypass.pointOfDischarge_FK
LEFT JOIN (SELECT DISTINCT incident_FK, consequenceCategory,consequenceRating,consequenceRatingInteger,isTheIncidentImpact FROM cleansed.swirl_incident_impact) Imp ON Imp.incident_FK= inc.id
LEFT JOIN (SELECT DISTINCT incidentAsSourceOfLessonsLearned_FK,description FROM cleansed.swirl_lessons_learned) Lesson ON Lesson.incidentAsSourceOfLessonsLearned_FK = Inc.id
LEFT JOIN cleansed.swirl_department Dep on Dep.id = inc.organisationalUnitPrimaryResponsibility_FK
LEFT JOIN (SELECT DISTINCT incidentId 
                           ,array_join(collect_set(description), ',')  as incidentEventType 
           FROM cleansed.vw_swirl_ref_lookup
           WHERE UPPER(lookupName) = UPPER('Incident Event Type')
           AND lookupItemId <> '0' 
           AND description IS NOT NULL
           GROUP BY incidentId
          ) eventType ON inc.id = eventType.incidentId
WHERE inc.incidentNumber IS NOT NULL 
AND EXISTS (SELECT 1 
            FROM cleansed.vw_swirl_ref_lookup as inner_tbl
            WHERE UPPER(lookupName) = UPPER('Incident Event Type')
            AND lookupItemId <> '0'
            AND UPPER(description) IN ('PERSONNEL', 'HEALTH AND SAFETY')
            AND inner_tbl.incidentId = inc.id)
 """)

# COMMAND ----------

#All incidents and events -non personal
spark.sql("""
CREATE OR REPLACE VIEW curated.viewswirlallincidentsandeventsnonpersonal AS 
SELECT DISTINCT inc.id,
inc.incidentNumber
,cast(incidentDate as date) as incidentDate
,cast(reportedDate as date) as recordedDate
,cast(closeIncidentDate as date) as dateClosed
,incidentStatus
,incidentCategory
,incidentShortDescription
,inc.consequenceRating
,inc.incidentOwnerOrganisation as incidentOwnerOrganisation
,Dep.organisationalUnit as businessArea
,per.sydneyWaterBusinessUnit as businessLocation
,incidentLocation
,concat_ws(',',per.Lastname,per.Firstname,per.userName) as incidentOwner
,concat_ws(',',report.Lastname,report.Firstname,report.userName) as reportedBy
,Imp.isTheIncidentImpact as actualPotential
,Imp.consequenceRating as consequenceRatingWord
,inc.riskRating
,Imp.consequenceCategory as consequenceCategory
,rootcause.rootCause
,rootcause.rootCauseDescription
,Lesson.description as lessonLearntDescription
,Imp.consequenceRatingInteger as consequenceRatingInteger
,Bypass.doesTheIncidentResultInLicenceNonCompliance as bypassNonCompliance
,treatmentType.treatmentType as bypassTreatmentType
,Bypass.volumeBypassed as bypassVolumeBypassed
,Bypass.volumeTreated as bypassVolumeTreated
,pod.name as bypassPointOfDischarge
,ReceivingWaterway.name as bypassReceivingWaterway
,inc.isAnInvestigationRequired as investigationRequired
,bypass.weather as weather
FROM cleansed.swirl_incident inc
LEFT JOIN cleansed.swirl_person per on inc.incidentProcessor_FK = per.id
LEFT JOIN cleansed.swirl_person report on inc.reportedBy_FK = report.id
LEFT JOIN cleansed.swirl_action act ON act.incidentAsSourceOfAction_FK = inc.id
LEFT JOIN cleansed.swirl_root_cause_analysis rootcause ON rootcause.incident_FK = inc.id
LEFT JOIN cleansed.swirl_incident_bypass_and_partial_treatment bypass ON bypass.incident_FK = Inc.id
LEFT JOIN
(SELECT DISTINCT incidentId ,
                 array_join(collect_set(description), ',')  as treatmentType 
 FROM cleansed.vw_swirl_ref_lookup
 WHERE UPPER(lookupName) = UPPER('Treatment Type')
 AND lookupItemId <> '0' 
 AND description IS NOT NULL 
 GROUP BY incidentId
) treatmentType on treatmentType.incidentId = inc.id
LEFT JOIN cleansed.swirl_pre_treatment ReceivingWaterway on ReceivingWaterway.id = bypass.receivingWaterway_FK
LEFT JOIN cleansed.swirl_pre_treatment pod on pod.id = bypass.pointOfDischarge_FK
LEFT JOIN (SELECT DISTINCT incident_FK, consequenceCategory,consequenceRating,consequenceRatingInteger,isTheIncidentImpact FROM cleansed.swirl_incident_impact) Imp ON Imp.incident_FK= inc.id
LEFT JOIN (SELECT DISTINCT incidentAsSourceOfLessonsLearned_FK,description FROM cleansed.swirl_lessons_learned) Lesson ON Lesson.incidentAsSourceOfLessonsLearned_FK = Inc.id
LEFT JOIN cleansed.swirl_department Dep on Dep.id = inc.organisationalUnitPrimaryResponsibility_FK
LEFT JOIN (SELECT DISTINCT incidentId 
                           ,array_join(collect_set(description), ',')  as incidentEventType 
           FROM cleansed.vw_swirl_ref_lookup
           WHERE UPPER(lookupName) = UPPER('Incident Event Type')
           AND lookupItemId <> '0' 
           AND description IS NOT NULL
           GROUP BY incidentId
          ) eventType ON inc.id = eventType.incidentId
WHERE inc.incidentNumber IS NOT NULL 
AND NOT EXISTS (SELECT 1 
            FROM cleansed.vw_swirl_ref_lookup as inner_tbl
            WHERE UPPER(lookupName) = UPPER('Incident Event Type')
            AND lookupItemId <> '0'
            AND UPPER(description) IN ('PERSONNEL', 'HEALTH AND SAFETY')
            AND inner_tbl.incidentId = inc.id)
""")
