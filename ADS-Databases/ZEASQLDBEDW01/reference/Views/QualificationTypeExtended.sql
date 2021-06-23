Create View reference.[QualificationTypeExtended]
as
select 
  qt.QualificationTypeID
  , QualificationTypeCode
  , QualificationTypeName
  , QualificationTypeText
  , Active
  , aq.AvetmissQualificationID
  , aq.AvetmissQualificationName
  , qg.QualificationGroupID
  , qg.QualificationGroupName
  , qg.QualificationGroupDescription
  , qg.QualificationGroupShort
  , bqg.BPRQualificationGroupID
  , bqg.BPRQualificationGroupName
  , bqg.BPRQualificationGroupDescription
  , BPRLevelID
  , (case when BPRLevelID = 1 then 'Lower Level Qualification'
		 when BPRLevelID = 2 then 'Higher Level Qualification'
		 else NULL
	end) AS BPRQualificationLevel
  , AQFGroupID
  , (case when AQFGroupID = 1 then 'AQF'
		 when AQFGroupID = 2 then 'Non AQF'
		 else NULL
	end) AS AQFGroup
  , qtm.AQFLevelID
  , [Order]
from [reference].qualificationtype qt
left join reference.avetmissqualification aq on aq.avetmissqualificationID = qt.avetmissqualificationid and aq._RecordDeleted = 0 and aq._RecordCurrent = 1
left join reference.qualificationtypemapping qtm on qt.qualificationtypeid = qtm.qualificationtypeid and qtm._RecordDeleted = 0 and qtm._RecordCurrent = 1
left join reference.qualificationgroup qg on qg.QualificationGroupID = qtm.QualificationGroupID and qg._RecordDeleted = 0 and qg._RecordCurrent = 1
left join reference.bprqualificationgroup bqg on bqg.bprqualificationgroupid = qtm.bprqualifcationgroupid and bqg._RecordDeleted = 0 and bqg._RecordCurrent = 1
where qt._RecordDeleted = 0
and qt._RecordCurrent = 1