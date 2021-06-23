




CREATE  view [compliance].[vwAvetmissUnitCourseEnrolment]

as

		SELECT distinct 
concat( ce.ReportingYear, ce.InstituteId , ce.DeliveryLocationCode, ce.StudentId, ce.AvetmissCourseCode) as ID_CPU
, ce.ID_UIO as ID_UIO
,NULL AS ID_FAC
, ce.ID_NCC as ID_NCC
,ce.InstituteId AS INSTITUTE
, CONVERT(VARCHAR(10), ce.reportingDate, 112) as REPORT_DATE--,rp.ReportingPeriodKey AS REPORT_DATE
,RIGHT(ce.ReportingYear,4) AS ENROLMENT_YEAR
, CONVERT(VARCHAR(10), ce.reportingDate, 112)  AS 'EXTRACT_DATE'
,CASE 
			WHEN ce.TafeDigital = 1 AND (ce.OTEN = 0 or ce.OTEN is null) AND ce.TOL = 1
				THEN ce.DeliveryLocationCode + '-TOL'
			WHEN ce.TafeDigital = 1 AND (ce.OTEN = 0 or ce.OTEN is null) AND ce.TNW = 1
				THEN ce.DeliveryLocationCode + '-TNW'
			WHEN ce.TafeDigital = 1 AND (ce.OTEN = 0 or ce.OTEN is null) AND ce.[OPEN] = 1
				THEN ce.DeliveryLocationCode + '-OPEN'
			ELSE ce.DeliveryLocationCode
		END AS DELIVERY_LOC 
,ce.AGE as'Age'
,ce.AgeGroup as 'Age Group'
,co.OfferingStartDate as 'Start Date'
,DAY(co.OfferingStartDate) as 'Start Date Day'
,DATENAME(MONTH, co.OfferingStartDate) as 'Start Date Month'
,YEAR(co.OfferingStartDate) as 'Start Date Year'
,MONTH(co.OfferingStartDate) as SORT_SD
,co.OfferingEndDate as 'End Date'
,DAY(co.OfferingEndDate) as 'End Date Day'
,DATENAME(MONTH, co.OfferingEndDate) as 'End Date Month'
,YEAR(co.OfferingEndDate) as 'End Date Year'
,MONTH(co.OfferingEndDate) as SORT_ED
,ce.CourseAwardDate as 'Date Awarded'
,DAY(ce.CourseAwardDate) as 'Date Awarded Day'
,DATENAME(MONTH, ce.CourseAwardDate) as 'Date Awarded Month'
,YEAR(ce.CourseAwardDate) as 'Date Awarded Year'
,MONTH(ce.CourseAwardDate) AS SORT_DA
,ce.CourseProgressDate as 'Progress Date'
,DAY(ce.CourseProgressDate) as 'Progress Date Day'
,DATENAME(MONTH, ce.CourseProgressDate) as 'Progress Date Month'
,YEAR(ce.CourseProgressDate) as 'Progress Date Year'
,MONTH(ce.CourseProgressDate) AS SORT_PD
,CASE 
	WHEN ANZSCO IS NOT NULL THEN CONCAT(ANZSCO, ' ', Replace(anz.OccupationName, ' nfd', ''))
	ELSE NULL
END as ANZSCO
,case 
	when ce.APPRTRAINEE = 'A' then 'Apprentice'
	when ce.APPRTRAINEE = 'T' then 'Trainee' 
	else 'Neither'
end as 'Apprentice or Trainee'
,awd_group.QualificationGroupDescription as 'Award Group'
,Case when awd.QualificationTypeName = 'Course' then 'Accredited Short Course' else awd.QualificationTypeName end as 'Award Name'
,cast(qtm.[Order] as VARCHAR) AS AWARD_NAME_SORT			
,ce.CourseAwardDescription AS 'Award Decription'
,CASE 
	WHEN ce.CommencementFlag = 'Commencing' THEN 'Commencing'
	ELSE 'Continuing'
END as 'Commencement Flag'
,ce.CommencementSemester as 'Commencement Semester'
,ce.CommencementYear as 'Commencement Year'
,ce.CommitmentIdentifier as 'Commitment ID'
,CASE 
	WHEN RTRIM(LTRIM(ce.CommitmentIdentifier)) IS NOT NULL THEN 'Yes - CID Provided'
	ELSE 'No - CID Blank'
END as 'CID Provided'

		,CASE
			WHEN ce.AVETCOUNT = 1 THEN 'Included'
			WHEN ce.AVETCOUNT is null AND ce.ENRFLAG = 1 THEN 'Included'
			ELSE 'Exlcuded'
		END as 'Count AVETMISS'
,CASE
	WHEN ce.ValidFlag = 1 THEN 'Valid'
	ELSE 'Invalid'
END as 'Count Valid Course Enrolments'
,ce.CourseCaloccCode  as 'Course Calocc Code'
,ce.AvetmissCourseCode as 'Course Code'

			,null as 'National Course Code'  -- note the cube has this as an integer so I can't load it with the real value
,c.CourseName as 'Course Name' --
,CASE 
	WHEN ce.FOE4 IS NOT NULL THEN CONCAT(ce.FOE4, ' ', FOE.FieldOfEducationDescription)
	ELSE NULL
END AS 'Field of Education'
,CASE 
	WHEN ce.AVFUND IS NOT NULL OR ce.AVFUND <> 0 THEN CONCAT(cast(ce.AVFUND as varchar(10)), ' ', fund_avet.AvetmissFundName)
	ELSE NULL
END AS'Course Funding Source - AVETMISS'
,CASE 
	WHEN ce.CourseFundingSourceCode IS NOT NULL THEN CONCAT(ce.CourseFundingSourceCode, ' ', fund_tafe.FundingSourceDescription)
	ELSE NULL
END AS 'Course Funding Source - TAFE'
,CASE 
	WHEN ce.CommitmentIdentifier IS NOT NULL THEN 'Yes'
	ELSE 'No'
END AS 'In eReporting'
 ,compliance.propercase( predom_dm.DeliveryModeDescription) as 'Course Delivery Mode'
,compliance.propercase(Unit_dm.DeliveryModeDescription) as 'Unit Delivery Mode'
,offering.OfferingTypeDescription  as 'Course Offering Type'
,ce.CourseProgressCode as 'Course Progress Code'
,ce.CourseProgressStatus as 'Course Progress Status'
,CourseProgReason.ProgressReasonDescription as 'Course Progress Reason'
,ce.CourseOfferingFunctionalUnit as 'Course Teaching Section Code' 
,case 
		when SUBSTRING (ce.EnrolmentTeachingSectionName, 4,1) = '-'  and Len(ce.EnrolmentTeachingSectionName) <> 4
		then SUBSTRING (ce.EnrolmentTeachingSectionName, 5,999) 
	else  ce.EnrolmentTeachingSectionName 
  end as 'Course Teaching Section'
,amu.[MinimumRequiredCoreUOCS] as 'Minimum Core Units' --
,amu.MinimumRequiredUOCS as 'Minimum UOC'
,Compliance.propercase(stsfund.TSNSWFundDescription) AS 'STS Fund'
,Compliance.propercase( stsfundgrp.TSNSWFundGroupDescription) as  'STS Fund Group'
, ce.SubsidyStatus as 'Smart and Skilled Status'
,ce.TrainingContractID AS 'Training Contract ID'
,CASE
	WHEN ce.ShellEnrolmentFlag = 1 THEN 'Yes - Shell Enrol'
	ELSE 'No - Shell Enrol'
END AS 'Shell Enrol'
,ce.SponsorCode AS 'Course Sponsor Org Code'
,ProgramStream.ProgramStreamDescription AS 'Program Stream'
,Upper(ce.ISC) AS 'Industry Skills Council'
,CASE 
WHEN ce.AvetmissFlag = 'Y' THEN 'Include in AVETMISS'
WHEN ce.AvetmissFlag = 'N' THEN 'Do not Include in AVETMISS'
ELSE null
END as 'AVETMISS Flag'
,ce.Decile AS 'SEIFA - Decile'
,ce.Percentile AS 'SEIFA - Percentile'
,c.trainingPackageCode as 'Training Package Code'
,c.TrainingPackageName as 'Training Package Name'
,c.CourseCategoryName as 'Unit Category'
,c.[RecommendedUsage] as 'Recommended Usage'
,[CourseStatus] as 'Product Status'
,[AttendanceModeDescription]   as 'Course Offering Attendance Code'
,offerEnrLoc.LocationName   as 'Course Offering Enrolment Location'
,co.[CourseOfferingCode]  as 'Course Offering Code'
,[OfferingDeliveryLocation]   as 'Course Offering Delivery Location' 
,[OfferingDeliveryModeDescription]  as 'Course Offering Delivery Mode' 
,OfferingStatus.OfferingStatusCode  as 'Course Offering Status'
	from		compliance.[AvetmissCourseEnrolment] ce 
  left join	[compliance].[AvetmissCourseOffering]  co  on co.CourseOfferingID = ce.CourseOfferingID 
		AND		co._RecordCurrent = 1
		AND		co._RecordDeleted = 0
  left	join	[compliance].[AvetmissCourse]  c   on co.[CourseCode]  = c.[CourseCode] 
		AND		C._RecordCurrent = 1
		AND		C._RecordDeleted = 0
		left join reference.Occupation anz on ce.ANZSCO = anz.OccupationCode 
				AND		ANZ._RecordCurrent = 1
				AND		ANZ._RecordDeleted = 0
		left join reference.QualificationType awd on ce.AWARD = awd.QualificationTypeID
				AND		awd._RecordCurrent = 1
				AND		awd._RecordDeleted = 0
		left join reference.QualificationTypeMapping qtm on awd.QualificationTypeID = qtm.QualificationTypeID
				AND		qtm._RecordCurrent = 1
				AND		qtm._RecordDeleted = 0
		left join reference.QualificationGroup awd_group on qtm.QualificationGroupID = awd_group.QualificationGroupID
				AND		awd_group._RecordCurrent = 1
				AND		awd_group._RecordDeleted = 0
		left join reference.FieldOfEducation foe on ce.FOE4 = foe.FieldOfEducationID
				AND		foe._RecordCurrent = 1
				AND		foe._RecordDeleted = 0
		left join reference.AvetmissFund fund_avet on ce.AVFUND = fund_avet.AvetmissFundID
				AND		fund_avet._RecordCurrent = 1
				AND		fund_avet._RecordDeleted = 0
		left join reference.FundingSource fund_tafe on ce.CourseFundingSourceCode = fund_tafe.FundingSourceCode
				AND		fund_tafe._RecordCurrent = 1
				AND		fund_tafe._RecordDeleted = 0
		left join reference.OfferingType offering on ce.CourseEnrolmentOfferingType = offering.OfferingTypeCode
				AND		offering._RecordCurrent = 1
				AND		offering._RecordDeleted = 0
		left join Reference.TsnswFund stsfund on TRIM(ce.TSNSWFundCode) = trim( stsfund.TSNSWFundCode )
				AND		stsfund._RecordCurrent = 1
				AND		stsfund._RecordDeleted = 0
		left join Reference.TsnswFundGroup stsfundgrp on ce.TSNSWFundGroupCode = stsfundgrp.TSNSWFundGroupCode
				AND		stsfundgrp._RecordCurrent = 1
				AND		stsfundgrp._RecordDeleted = 0
		left join Reference.DeliveryMode predom_dm on ce.UnitsDeliveryMode = predom_dm.DeliveryModeCode
				AND		predom_dm._RecordCurrent = 1
				AND		predom_dm._RecordDeleted = 0	   
	    left join Reference.DeliveryMode Unit_dm on ce.UnitsDeliveryMode = Unit_dm.DeliveryModeCode
				AND		Unit_dm._RecordCurrent = 1
				AND		Unit_dm._RecordDeleted = 0		
		left join reference.SkillsPoint skills on ce.SKILLSPOINT = skills.SkillsPointID
				AND		skills._RecordCurrent = 1
				AND		skills._RecordDeleted = 0
		left join reference.WelfareStatus wel on ce.WelfareStatus = wel.WelfareStatusID
				AND		wel._RecordCurrent = 1
				AND		wel._RecordDeleted = 0
		left join reference.StudyReason stdr on ce.StudyReason = stdr.StudyReasonID
				AND		stdr._RecordCurrent = 1
				AND		stdr._RecordDeleted = 0
		left join reference.FundingSourceVetFeeHelpFund fg_code on ce.CourseFundingSourceCode = fg_code.FundingSourceCode
				AND		fg_code._RecordCurrent = 1
				AND		fg_code._RecordDeleted = 0
		left join reference.VetFeeHelpFund fg_code_name on fg_code.VETFeeHelpFundID = fg_code_name.VETFeeHelpFundID
				AND		fg_code_name._RecordCurrent = 1
				AND		fg_code_name._RecordDeleted = 0
		LEFT JOIN reference.FundingSourceBpr BPR_MAP_CODE ON CE.CourseFundingSourceCode = BPR_MAP_CODE.FundingSourceCode
				AND		BPR_MAP_CODE._RecordCurrent = 1
				AND		BPR_MAP_CODE._RecordDeleted = 0
		LEFT JOIN reference.BprSubCategory BPR_SUBCAT ON BPR_MAP_CODE.BPRSubCategoryID = BPR_SUBCAT.BPRSubCategoryID
				AND		BPR_SUBCAT._RecordCurrent = 1
				AND		BPR_SUBCAT._RecordDeleted = 0
		left join [edw].[SAP_CostCentreSkillsTeam] stm on ce.CourseOfferingFunctionalUnit = stm.CostCentreCode
				AND		stm._RecordCurrent = 1
				AND		stm._RecordDeleted = 0
		LEFT JOIN reference.ProgramStream  ON ProgramStream.[ProgramStreamID]  = ce.ProgramStream
				AND		ProgramStream._RecordCurrent = 1
				AND		ProgramStream._RecordDeleted = 0
  left	join	[reference].[AvetmissMinimumUnits]  amu   on amu.AvetmissCourseCode = ce.AvetmissCourseCode
		AND		amu._RecordCurrent = 1
		AND		amu._RecordDeleted = 0
  left	join	[reference].[ProgressReason]  CourseProgReason   on CourseProgReason.ProgressReasonCode = ce.CourseProgressReason
		AND		CourseProgReason._RecordCurrent = 1
		AND		CourseProgReason._RecordDeleted = 0
  left	join	compliance.AvetmissLocation  offerEnrLoc   on offerEnrLoc.LocationCode = co.[OfferingEnrolmentLocation]	
		AND		offerEnrLoc._RecordCurrent = 1
		AND		offerEnrLoc._RecordDeleted = 0
  left	join	reference.OfferingStatus  OfferingStatus   on OfferingStatus.OfferingStatusDescription = co.OfferingStatusDescription	
		AND		OfferingStatus._RecordCurrent = 1
		AND		OfferingStatus._RecordDeleted = 0
where 1=1
AND LEFT (CE.REPORTINGYEAR, 2) = 'CY'	
AND		CE._RecordCurrent = 1
AND		CE._RecordDeleted = 0