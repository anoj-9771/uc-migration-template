

CREATE   View [reference].[CourseSkillsPointExtended]
as

select  csp.NationalCourseCode
, csp.SkillsPointID
, SkillsPointCode
, SkillsPointName
, SkillsPointDescription
from [reference].[CourseSkillsPoint] csp
left join reference.SkillsPoint sp on sp.SkillsPointID = csp.SkillsPointID  and sp._RecordDeleted = 0 and sp._RecordCurrent = 1
Where 	csp._RecordDeleted = 0 
and		csp._RecordCurrent = 1