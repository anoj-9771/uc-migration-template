

CREATE   View [reference].[AvetmissCourseSkillsPointExtended]
as

select 
AvetmissCourseCode
, ac.SkillsPointID
, SkillsPointCode
, SkillsPointName
, SkillsPointDescription
from reference.AvetmissCourseSkillsPoint ac
left join reference.SkillsPoint sp on ac.SkillsPointID = sp.SkillsPointID and sp._RecordDeleted = 0 and sp._RecordCurrent = 1

where ac._RecordDeleted = 0
and ac._RecordCurrent = 1