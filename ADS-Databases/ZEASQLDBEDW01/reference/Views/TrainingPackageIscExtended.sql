CREATE View Reference.[TrainingPackageIscExtended]
as
select 
TrainingPackageSubstring
, isc. IndustrySkillsCouncilID
, isc.IndustrySkillsCouncilName
from reference.TrainingPackageIsc tpi
join reference.IndustrySkillsCouncil isc on isc.IndustrySkillsCouncilID = tpi.IndustrySkillsCouncilID
Where 	tpi._RecordDeleted = 0 
and		tpi._RecordCurrent = 1
and		isc._RecordDeleted = 0 
and		isc._RecordCurrent = 1