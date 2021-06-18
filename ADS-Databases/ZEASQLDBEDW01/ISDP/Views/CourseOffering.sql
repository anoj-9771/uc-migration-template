
CREATE   view     [ISDP].[CourseOffering]  
as
Select 
		  c.coursecode as ProductCode
		 , c.CourseName AS ProductName
		 ,c.AvetmissCourseCode as CourseCode
		 ,CourseStatus as ProductStatus
		 ,Coalesce(ccm.CurrentCourseCode,c.AvetmissCourseCode) as CurrentCourseCode
		 ,ccm.VersionOnSkillsList
		 ,c.NationalCourseCode
		, l.LocationCode
		,l.LocationName
		,l.RegionName
		,co.CourseOfferingID
		,co.OfferingStartDate
		,co.OfferingEndDate
		,sp.SkillsPointCode
		,sp.SkillsPointName
		, c.CourseName
		,qt.QualificationTypeCode	
		,qt.QualificationTypeName
		,co.OfferingStatusDescription
		,case when substring(c.AvetmissCourseCode,1,2) = 'HE' then 'Yes - Higher Ed' else 'No - Higher Ed' end as HigherEd
		,am.AttendanceModeCode
		,am.AttendanceModeName
		,am.AttendanceModeDescription
		,dm.DeliveryModeCode
		,dm.DeliveryModeName
		,dm.DeliveryModeDescription
		,qg.QualificationGroupName
		,qg.QualificationGroupDescription
		,qg.QualificationGroupShort
		,OT.OfferingTypeCode 
		,OT.[OfferingTypeName]
		,OT.[OfferingTypeDescription] 
		,CASE 
		 WHEN RIGHT(co.Duration,1) = 'D' and SUBSTRING(co.Duration,1, len(co.Duration)-1) = '1' then Concat (SUBSTRING(co.Duration,1, len(co.Duration)-1) , ' Day')
		 WHEN RIGHT(co.Duration,1) = 'D' then Concat (SUBSTRING(co.Duration,1, len(co.Duration)-1) , ' Days')
		 WHEN RIGHT(co.Duration,1) = 'W' and SUBSTRING(co.Duration,1, len(co.Duration)-1) = '1' then Concat (SUBSTRING(co.Duration,1, len(co.Duration)-1) , ' Week')
		 WHEN RIGHT(co.Duration,1) = 'W' then Concat (SUBSTRING(co.Duration,1, len(co.Duration)-1) , ' Weeks')
		 WHEN RIGHT(co.Duration,1) = 'M' and SUBSTRING(co.Duration,1, len(co.Duration)-1) = '1' then Concat (SUBSTRING(co.Duration,1, len(co.Duration)-1) , ' Month')
		 WHEN RIGHT(co.Duration,1) = 'M' then Concat (SUBSTRING(co.Duration,1, len(co.Duration)-1) , ' Months')
		 WHEN RIGHT(co.Duration,1) = 'Y' and SUBSTRING(co.Duration,1, len(co.Duration)-1) = '1' then Concat (SUBSTRING(co.Duration,1, len(co.Duration)-1) , ' Year')
		 WHEN RIGHT(co.Duration,1) = 'Y' then Concat (SUBSTRING(co.Duration,1, len(co.Duration)-1) , ' Years')
		 WHEN RIGHT(co.Duration,1) = 'S' and SUBSTRING(co.Duration,1, len(co.Duration)-1) = '1' then Concat (SUBSTRING(co.Duration,1, len(co.Duration)-1) , ' Semester')
		 WHEN RIGHT(co.Duration,1) = 'S' then Concat (SUBSTRING(co.Duration,1, len(co.Duration)-1) , ' Semesters')
		 Else co.Duration end as Duration
		,co.AnytimeEnrolment
		,l.locationName as OfferingEnrolmentLocation
		,co.OfferingDeliveryLocation
		,co.TeachingSectionCode
		,co.TeachingSectionDescription
		,co.CourseFundingSourceCode
		,co.CourseFundingSourceDescription
		,co.PublishToWebsites
		,co.status as OfferingStatus

from compliance.AvetmissCourseOffering co
     Join compliance.AvetmissCourse c on c.CourseCode = co.CourseCode and c._RecordCurrent = 1
left join compliance.AvetmissLocation l on l.LocationCode = co.OfferingEnrolmentLocation and l._RecordCurrent =1

--New Way Skills Point
Left Join reference.CourseSkillsPoint csp on Coalesce(c.NationalCourseCode, c.CourseCode) = csp.NationalCourseCode and csp._RecordCurrent = 1
left Join reference.SkillsPoint sp on csp.SkillsPointID = sp.SkillsPointID and sp._RecordCurrent = 1

--Old Way Skills Point
--Left Join reference.AvetmissCourseSkillsPoint acsp on acsp.AvetmissCourseCode = c.AvetmissCourseCode and acsp._RecordCurrent = 1
--left Join reference.SkillsPoint spa on acsp.SkillsPointID = spa.SkillsPointID and spa._RecordCurrent = 1

left join reference.QualificationType qt on c.QualificationTypeCode = qt.QualificationTypeID and qt._RecordCurrent = 1
left join reference.QualificationTypeMapping qtm on qt.QualificationTypeID = QTM.QualificationTypeID and qtm._RecordCurrent = 1
left join reference.QualificationGroup qg on qg.QualificationGroupID = qtm.QualificationGroupID and qg._RecordCurrent = 1
left join reference.AttendanceMode am on am.AttendanceModeCode = co.AttendanceModeCode and am._RecordCurrent = 1
left join reference.DeliveryMode dm on co.OfferingDeliveryMode = dm.DeliveryModeCode and dm._RecordCurrent = 1
left join reference.OfferingType ot on ot.OfferingTypeCode = co.offeringType and  ot._RecordCurrent = 1
left join [compliance].[AvetmissCurrentCourseMapping] ccm on ccm.OldCourseCode = c.AvetmissCourseCode


Where co._RecordCurrent = 1
--AND OfferingEndDate >= '2018-01-01'
AND OfferingStartDate >= '2020-07-01'
and co.CourseOfferingID is not null
and coursestatus  in ('CURRENT','SUPERSEDED')