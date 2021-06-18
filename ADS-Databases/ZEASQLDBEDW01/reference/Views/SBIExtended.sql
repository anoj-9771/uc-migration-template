
Create View Reference.[SBIExtended]
as
select 
SBISubCategoryID
, SBISubCategory
, sc.SBICategoryID
, sc.SBICategoryName
, sg.SBIGroupID
, sg.SBIGroupName
from reference.SbiSubCategory ssc
join reference.SbiCategory sc on sc.SBICategoryID = ssc.SBICategoryID
join reference.SbiGroup sg on sg.SBIGroupID = sc.SBIGroupID


where ssc._RecordCurrent = 1
and ssc._RecordDeleted = 0
and sc._RecordCurrent = 1
and sc._RecordDeleted = 0
and sg._RecordCurrent = 1
and sg._RecordDeleted = 0