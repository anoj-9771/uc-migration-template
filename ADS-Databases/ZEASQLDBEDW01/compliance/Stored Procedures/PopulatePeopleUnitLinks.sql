
--exec compliance.PopulatePeopleUnitLinks

create procedure [compliance].[PopulatePeopleUnitLinks]
as

	if object_id('tempdb..#pul') is not null drop table #pul

	;with UNIT_COURSE_LINK as
	(
		select PU.ID AS grpchildid, PU.ID AS CHILD_ID, PUL.PARENT_ID, 1 as level
		from edw.OneEBS_EBS_0165_PEOPLE_UNITS PU
			left join edw.OneEBS_EBS_0165_PEOPLE_UNIT_LINKS PUL on PU.ID = PUL.CHILD_ID 
				and PUL._RecordCurrent = 1
				and PUL._RecordDeleted = 0
		where PUL.PARENT_ID is null
		and PU._RecordCurrent = 1
		and PU._RecordDeleted = 0
		union all
		select Parent.grpchildid, Child.CHILD_ID, Child.PARENT_ID, Parent.level + 1
		from UNIT_COURSE_LINK Parent
			join edw.OneEBS_EBS_0165_PEOPLE_UNIT_LINKS Child on Child.PARENT_ID = Parent.CHILD_ID
		where Child._RecordCurrent = 1
		and Child._RecordDeleted = 0
	)
	SELECT grpchildid AS ParentId,CHILD_ID AS ChildId
	into #pul
	FROM UNIT_COURSE_LINK

	truncate table compliance.AvetmissPeopleUnitLinks
	insert into compliance.AvetmissPeopleUnitLinks (UnitEnrolmentId, CourseEnrolmentId)
	select ChildId, ParentId
	from #pul

	if object_id('tempdb..#pul') is not null drop table #pul