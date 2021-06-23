create View Reference.[FieldOfEducationIscExtended]
as


select 
foei.FieldOfEducationID
, foe.FieldOfEducationName
, isc.IndustrySkillsCouncilID
, isc.IndustrySkillsCouncilName
from reference.FieldOfEducationIsc foei
left join reference.FieldOfEducation foe on foe.FieldOfEducationID = foei.FieldOfEducationID and foe._RecordCurrent =1 and foe._RecordDeleted = 0
left join reference.IndustrySkillsCouncil isc on isc.IndustrySkillsCouncilID = foei.IndustrySkillsCouncilID and isc._RecordCurrent = 1 and isc._RecordDeleted = 0
where foei._RecordCurrent = 1
and foei._RecordDeleted = 0