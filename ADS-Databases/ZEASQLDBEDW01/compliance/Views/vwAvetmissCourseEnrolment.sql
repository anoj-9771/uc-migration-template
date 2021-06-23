


CREATE      view [compliance].[vwAvetmissCourseEnrolment]
as
	
	select 
		ce.ID_PC, 
		ce.ID_UIO,
		ce.ID_CPU, 
		NULL as ID_FAC,
		ce.ID_NCC,
		rp.ReportingPeriodKey AS REPORT_DATE, 
		convert(varchar(10), ce.ReportingDate, 112) AS EXTRACT_DATE,
		right(rp.SpecialReportingYear,4) AS ENROLMENT_YEAR,
		ce.InstituteId AS INSTITUTE,
		CASE 
			WHEN ce.TafeDigital = 1 AND (ce.OTEN = 0 or ce.OTEN is null) AND ce.TOL = 1
				THEN ce.DeliveryLocationCode + '-TOL'
			WHEN ce.TafeDigital = 1 AND (ce.OTEN = 0 or ce.OTEN is null) AND ce.TNW = 1
				THEN ce.DeliveryLocationCode + '-TNW'
			WHEN ce.TafeDigital = 1 AND (ce.OTEN = 0 or ce.OTEN is null) AND ce.[OPEN] = 1
				THEN ce.DeliveryLocationCode + '-OPEN'
			ELSE ce.DeliveryLocationCode
		END AS 'Delivery Location', 
		ce.CourseEnrolmentStartDate as 'Start Date',
		DAY(ce.CourseEnrolmentStartDate) AS 'Start Date Day', 
		DATENAME(MONTH, ce.CourseEnrolmentStartDate) AS 'Start Date Month',
		YEAR(ce.CourseEnrolmentStartDate) AS 'Start Date Year',
		MONTH(ce.CourseEnrolmentStartDate) AS SORT_SD,
		ce.CourseEnrolmentEndDate as 'End Date',
		DAY(ce.CourseEnrolmentEndDate) AS 'End Date Day', 
		DATENAME(MONTH, ce.CourseEnrolmentEndDate) AS 'End Date Month',
		YEAR(ce.CourseEnrolmentEndDate) AS 'End Date Year',
		MONTH(ce.CourseEnrolmentEndDate) AS SORT_ED,
		ce.CourseAwardDate AS 'Date Awarded',
		DAY(ce.CourseAwardDate) AS 'Date Awarded Day', 
		DATENAME(MONTH, ce.CourseAwardDate) AS 'Date Awarded Month',
		YEAR(ce.CourseAwardDate) AS 'Date Awarded Year',
		MONTH(ce.CourseAwardDate) AS SORT_DA,
		ce.Age as 'Age',
		ce.AgeGroup as 'Age Group',
		case when ce.Age < 20 then '0-19'
			when ce.Age > 19 and ce.Age < 25 then '20-24'
			when ce.Age > 24 and ce.Age < 35 then '25-34'
			when ce.Age > 34 and ce.Age < 46 then '35-45'
			else '45 Plus'
		end as 'Simple Age Group',
		CASE 
			WHEN ANZSCO IS NOT NULL THEN CONCAT(ANZSCO, ' ', case when right(anz.OccupationName, 4) in (' nfd', ' nec') then reverse(substring(reverse(anz.OccupationName), 5, len(anz.OccupationName))) else anz.OccupationName end)
			ELSE NULL
		END as ANZSCO,
		case 
			when ce.APPRTRAINEE = 'A' then 'Apprentice'
			when ce.APPRTRAINEE = 'T' then 'Trainee' 
			else 'Neither'
		end as 'Apprentice or Trainee', 
		coalesce(ce.ARIA11B, '_NOT KNOWN') as ARIA11B,
		awd_group.QualificationGroupDescription as 'Award Group', 
		CASE 
			WHEN ce.HIGHED = 'Yes - Higher Ed' OR qtm.BPRLevelID = '2' THEN 'Higher Level Qualifications'
			else 'Lower Level Qualifications'
		END as 'AWARD BPR',
		--	coalesce(awd_group.QualificationGroupShort, '09. Not mapped to a Qual') as 'Award BPR Name',
		coalesce (bpr_qual_group.BPRQualificationGroupDescription, '09. Not Mapped to a Qualification Level') as 'Award BPR Name',
	awd.QualificationTypeName as 'Award Name',
		cast(qtm.[Order] as int) AS AWARD_NAME_SORT,

		ce.CourseAwardDescription AS 'Award Description',
		ce.CourseAwardStatus as 'Award Status1',
		ce.CourseCaloccCode as 'Calocc Code', 
		CASE 
			WHEN ce.CommencementFlag = 'Commencing' THEN 'Commencing'
			ELSE 'Continuing'
		END as 'Commencement Flag', 
		CASE
		WHEN left (coalesce ( ce.CommencementSemester, '9999'),4 ) < 2013
		Then '_OTHER'
		ELSE  ce.CommencementSemester
		END as 'Commencement Semester', 
		ce.CommencementYear as 'Commencement Year', 
		ce.CommitmentIdentifier as 'Commitment ID',
		CASE
			WHEN ce.CommitmentIdentifier IS NOT NULL THEN 'CID Provided'
			ELSE 'CID not Provided'
		END AS 'Commitment ID Provided',
		CASE
			WHEN ce.GRAD = 1 THEN 'Yes - Completed'
			ELSE 'No - Not Completed'
		END as 'Completion Flag', 
		ce.YEAR_COMPLETE as 'Completion Year',
		ce.COREFND as 'Core Non Core', 
		CASE
			WHEN ce.AVETCOUNT = 1 THEN 'Included'
			WHEN ce.AVETCOUNT is null AND ce.ENRFLAG = 1 THEN 'Included'
			ELSE 'Exlcuded'
		END as 'Count AVETMISS', 
		CASE
			WHEN ce.AvetmissFlag = 'Y' THEN 'Include'
			ELSE 'Exlude'
		END as 'Include in AVETMISS Flag',
		CASE
			WHEN ce.ValidFlag = 1 THEN 'Valid'
			ELSE 'Invalid'
		END as 'Count Valid Enrolments', 
		ce.AvetmissCourseCode as 'Course Code',
		--COALESCE(UPPER(ce.CSENM), 'MISSING COURSE CODE') as 'Course Name',
		CASE
			WHEN ISNULL(ce.CSENM, '') = '' then 'MISSING COURSE CODE'
			else UPPER(ce.CSENM)
		END as 'Course Name',
		CASE 
			WHEN ce.FOE4 IS NOT NULL THEN CONCAT(ce.FOE4, ' ', FOE.FieldOfEducationDescription)
			ELSE NULL
		END AS 'Field of Education',
		CASE 
			WHEN ce.AVFUND IS NOT NULL OR ce.AVFUND <> 0 THEN CONCAT(cast(ce.AVFUND as varchar(10)), ' ', fund_avet.AvetmissFundName)
			ELSE NULL
		END AS 'Funding Source - AVETMISS',
		CASE 
			WHEN ce.CourseFundingSourceCode IS NOT NULL THEN CONCAT(ce.CourseFundingSourceCode, ' ', fund_tafe.FundingSourceDescription)
			ELSE NULL
		END AS 'Funding Source - TAFE',
		CASE 
			WHEN ce.FundingVFH IS NOT NULL THEN ce.FundingVFH
			ELSE NULL
		END AS 'Funding Category - VFH',
		ce.HIGHED AS 'Higher Education', 
		ce.HQUALN AS 'Highest Previous Qualification', 
		Coalesce(ce.SECED, 'NOT STATED') AS 'Highest School Level',
		/*
		CASE 
			WHEN sts.COMMITMENT_ID IS NOT NULL THEN 'Yes'
			ELSE 'No'
		END AS 'In eReporting',
		*/
		'' AS 'In eReporting',
		upper(coalesce(ce.ISC, '___Other')) AS 'Industry Skills Council',
		ce.LGANAM AS 'LGA Name', 
		--NULL AS NATIONAL_COURSE_CODE,
		offering.OfferingTypeDescription AS 'Offering Type',
		ce.CourseProgressCode AS 'Progress Code', 
		ce.CourseProgressDate AS 'Progress Date',
		DAY(ce.CourseProgressDate) AS 'Progress Date Day', 
		DATENAME(MONTH, ce.CourseProgressDate) AS 'Progress Date Month',
		YEAR(ce.CourseProgressDate) AS 'Progress Date Year',
		MONTH(ce.CourseProgressDate) AS SORT_PD,
--		ce.CourseProgressReason AS 'Progress Reason',
		pr.ProgressReasonDescription AS 'Progress Reason',
		ce.CourseProgressStatus AS 'Progress Status',
		ce.RIND11 AS 'Regional Remote Flag',
		CASE
			WHEN ce.ShellEnrolmentFlag = 1 THEN 'Yes - Shell Enrol'
			ELSE 'No - Shell Enrol'
		END AS 'Shell Enrolment', 
		'' AS 'eReporting Region',
		ce.SponsorCode AS 'Sponsor Code',
		ce.UnitCount AS 'Total Units', 
		ce.FailGradeUnitCount AS 'Total Units Fail',
		ce.PassGradeUnitCount AS 'Total Units Pass', 
		ce.WithdrawnUnitCount AS 'Total Units Withdrawn',
		ce.TrainingContractID AS 'Training Contract ID',
		CASE
			WHEN ce.TSNSWFundCode IS NOT NULL THEN CONCAT(ce.TSNSWFundCode, ' ', stsfund.TSNSWFundDescription)
			ELSE NULL
		END AS 'TSNSW Fund',
		CASE
			WHEN ce.TSNSWFundGroupCode IS NOT NULL THEN CONCAT(ce.TSNSWFundGroupCode, ' ', stsfundgrp.TSNSWFundGroupDescription)
			ELSE NULL
		END AS 'TSNSW Fund Group',
		CASE 
			WHEN ce.ValidUnitExistsFlag = 1 THEN 'Valid Units'
			WHEN right(ce.ReportingYear, 4) IN ('2014', '2015') THEN NULL
			ELSE 'Invalid Units'
		END AS 'Finalised Unit Flag',
		CASE 
			WHEN ce.ValidUnitCount = 0 THEN NULL
			ELSE ce.ValidUnitCount
		END AS 'Finalised Unit Total',
		CASE 
			WHEN ce.AvetmissCourseCode LIKE '%-999999' THEN 'Unit Only Enrolment'
			ELSE 'Course Enrolment'
		END AS 'Unit Only Flag',
		ce.WaiverCodes AS 'Waiver Codes',
		CASE ce.WhoToPay 
		WHEN 'CONTRACT' THEN 'Sponsor Commercial'
		WHEN 'LEARNER' THEN 'Learner to Pay'
		WHEN 'MKTINGPTNR' THEN 'Sponsor Marketing'
		WHEN 'TPPSAP' THEN 'Sponsor (TPP) to pay'
		ELSE  ce.WhoToPay 
			END AS 'Who To Pay',
		ce.CourseOfferingFunctionalUnit AS 'Enrolment Teaching Section Code',
		ce.EnrolmentTeachingSectionName AS 'Enrolment Teaching Section Name',
		SUBSTRING(ce.CourseOfferingFunctionalUnit,1,4) as 'Enrolment Teaching Unit',
		ce.SA_STATE AS 'SA State',
		ce.SA4_NAME AS 'SA4',
		ce.SA2_NAME AS 'SA2', 
		--CASE 
	
		--	WHEN predom_dm.DeliveryModeDescription = 'UNIT - ELECTRONIC DELIVERY' THEN 'Unit - Electronic Delivery'
		--	WHEN predom_dm.DeliveryModeDescription = 'COURSE - MIXED MODE' THEN 'Course - Mixed Mode'
		--	WHEN predom_dm.DeliveryModeDescription = 'UNIT - ONLINE DELIVERY' THEN 'Unit - Online Delivery'
		--	WHEN predom_dm.DeliveryModeDescription = 'COURSE - ONLINE/CORRESPONDENCE' THEN 'Course - Online/Correspondence'
		--	WHEN predom_dm.DeliveryModeDescription = 'UNIT - COLLEGE/CAMPUS CLASS' THEN 'Unit - College/Campus Class'
		--	WHEN predom_dm.DeliveryModeDescription = 'UNIT - ON THE JOB DISTANCE' THEN 'Unit - On The Job Distance'
		--	WHEN predom_dm.DeliveryModeDescription = 'UNIT - COMBINATION OF ALL' THEN 'Unit - Combination Of All'
		--	WHEN predom_dm.DeliveryModeDescription = 'UNIT - ON THE JOB ONLINE' THEN 'Unit - On The Job Online'
		--	WHEN predom_dm.DeliveryModeDescription = 'UNIT - SIMULATED WORKPLACE' THEN 'Unit - Simulated Workplace'
		--	WHEN predom_dm.DeliveryModeDescription = 'COURSE - CLASSROOM' THEN 'Course - Classroom'
		--	WHEN predom_dm.DeliveryModeDescription = 'UNIT - DISTANCE EDUCATION' THEN 'Unit - Distance Education'
		--	WHEN predom_dm.DeliveryModeDescription = 'MIXED MODE' THEN 'Mixed Mode'
		--	WHEN predom_dm.DeliveryModeDescription = 'UNIT - CLASS SELF-PACED' THEN 'Unit - Class Self-Paced'
		--	WHEN predom_dm.DeliveryModeDescription = 'UNIT - CLASS ONLINE' THEN 'Unit - Class Online'
		--	WHEN predom_dm.DeliveryModeDescription = 'UNIT - ON THE JOB TRAINING' THEN 'Unit - On The Job Training'
		--	WHEN predom_dm.DeliveryModeDescription = 'COURSE - WORK BASED' THEN 'Course - Work Based'
		--	WHEN predom_dm.DeliveryModeDescription = 'UNIT - BLENDED DELIVERY' THEN 'Unit - Blended Delivery'

		
	 --ELSE predom_dm.DeliveryModeDescription end as 'Predominant Delivery Mode',
	 compliance.ProperCase (predom_dm.DeliveryModeDescription) as 'Predominant Delivery Mode',


		CASE 
			WHEN ce.HasLongTermUnemploymentEvidence = 'Y' THEN 'Yes - Long Term Unemployed'
			ELSE 'No - Long Term Unemployed'
		END AS 'Long Term Unemployed',
		wel.WelfareStatusDescription as 'Welfare Status',
		COALESCE (skills.SkillsPointDescription, 'Other') as 'SkillsPoints Industry',
		Case when stdr.StudyReasonID is not null then concat(stdr.StudyReasonID,'. ', stdr.StudyReasonDescription) else null end as 'Study Reason', 
		CASE 
			WHEN ce.CourseFundingSourceCode IN ('001J', '001T') THEN 'Yes - Fully Subsidised' 
			ELSE 'No - Fully Subsidised'
		END AS 'Fully Subsidised',
		CASE 
			WHEN ce.CTOnly = 1 THEN 'Yes - Credit Transfer Only' 
			ELSE NULL 
		END AS 'Credit Transfer Only',
		CASE 
			WHEN isnull(ce.ElectronicDeliveryUnitCount, 0) + isnull(ce.BlendedDeliveryUnitCount, 0) > 0 THEN 'Yes - Online Units' 
			ELSE 'No - Online Units' 
		END AS 'Online Units',
		

		-- MEASURES 
		ce.TotalHours as ASH_COUNT,
		CASE
			WHEN ce.AVETCOUNT = 1 THEN 1
			WHEN ce.AVETCOUNT is null AND ce.ENRFLAG = 1 THEN 1
			ELSE NULL
		END as AVETMISS_COUNT, 
		CASE 
			WHEN ce.CommencementFlag = 'Commencing' THEN 1
			ELSE NULL
		END as COMMENCEMENT_COUNT, 
		CASE
			WHEN ce.GRAD = 1 THEN 1
			ELSE NULL
		END as COMPLETION_COUNT, 
		CASE 
			WHEN ce.CommencementFlag = 'Continuing' THEN 1
			ELSE null
		END as CONTINUING_COUNT, 
		ce.ContinuingHours as CONTINUING_ASH, 
		1 as ENROLMENT_COUNT, 
		CASE
			WHEN ce.ValidFlag = 1 THEN 1
			ELSE 0
		END as VALID_COUNT,
		ce.RPLHours AS RPL_ASH_COUNT,
		ce.TrainingHours AS TRAINING_ASH_COUNT,
		CASE 
			WHEN ce.TransitionFlag = 'Y' THEN 1
			ELSE 0
		END AS TRANSITION_COUNT, 

		-- EBS FEES 
		ce.ActualFee AS FEE_ACTUAL, 
		ce.FeeAdjustment AS FEE_ADJUSTMENT,
		ce.FeeInstalment AS FEE_INSTALMENT, 
		ce.OriginalFee AS FEE_ORIGINAL, 
		ce.FeeOutstanding AS FEE_OUTSTANDING, 
		ce.FeePaid AS FEE_PAID,

		-- EREPORTING PAYMENTS 
		NULL AS LOC_LOADING_PAYMENT, 
		NULL AS NEEDS_LOADING_PAYMENT, 
		NULL AS STAGE_PAYMENT, 

		-- SMART & SKILLED MEASURES
		ce.SubsidyStatus as 'eReporting Status',


		EPS.ProgramStreamDescription AS 'eReporting Program Stream',
		ce.LocationLoadingAmount AS LOC_LOADING,
		ce.NeedsLoadingAmount AS NEED_LOADING, 
		ce.StandardSubsidyAmount AS STANDARD_SUBSIDY, 
		NULL AS SUBSIDY_ADJUSTMENT,
		ce.SubsidisedAmount AS SUBSIDY_AMOUNT,

		-- SEIFA
		ce.Decile AS 'SEIFA - Decile', 
		ce.Percentile AS 'SEIFA - Percentile',

		-- REGIONS 
		CASE 
			WHEN ce.TafeDigital =  1 AND (ce.OTEN = 1 OR ce.TOL = 1 OR ce.TNW = 1 OR ce.[OPEN] = 1) THEN 'TAFE Digital'
			WHEN ce.DeliveryLocationCode = 'OTE' THEN 'TAFE Digital'
			ELSE 'Region'
		END AS 'TAFE Digital Flag',
		ce.Region AS 'Region',

		-- TAFE DIGITAL 
		CASE 
			WHEN rp.ReportingDate < '2020-01-01' and ce.TOL = 1 THEN 'TAFE Online' --Confirmed this with Jeremy that TOL does not apply from 1-jan-2020
			WHEN ce.TNW = 1 THEN 'TAFE Now'
			WHEN ce.OTEN = 1 THEN 'OTEN'
			WHEN ce.[OPEN] = 1 THEN 'Open College'
			WHEN ce.DeliveryLocationCode = 'OTE' THEN 'OTEN'
			ELSE 'Region'
		END AS 'TAFE Digital Area',
		CASE 
			WHEN ce.TOL = 1 THEN 4
			WHEN ce.TNW = 1 THEN 3
			WHEN ce.OTEN = 1 THEN 2
			WHEN ce.[OPEN] = 1 THEN 1
			WHEN ce.DeliveryLocationCode = 'OTE' THEN 2
			ELSE 5
		END AS SORT_TDA,


		-- TRANSITION
		ce.CourseEnrolmentIdTransitionFrom AS 'Transitioned from ID',
		ce.CourseCodeTransitionFrom AS 'Transitioned from Course Code',
		ce.CourseCaloccCodeTransitionFrom AS 'Transitioned from Calocc Code',
		ce.CourseEnrolmentStartDateTransitionFrom as 'Original Start Date', 
		DAY(ce.CourseEnrolmentStartDateTransitionFrom) AS 'Original Start Date Day', 
		DATENAME(MONTH, ce.CourseEnrolmentStartDateTransitionFrom) AS 'Original Start Date Month',
		YEAR(ce.CourseEnrolmentStartDateTransitionFrom) AS 'Original Start Date Year',
		MONTH(ce.CourseEnrolmentStartDateTransitionFrom) AS SORT_OSD,
		ce.TransitionFlag as 'Transition Flag',

		-- VET FEE HELP
		CASE 
			WHEN ce.VFHCourse = 'Y' THEN 'Yes - VFH Course'
			ELSE 'No - VFH Course'
		END AS 'Is VET FEE HELP',
		CASE 
			WHEN ce.FeeHelpFlag = 'Y' THEN 'Yes - FH Course'
			ELSE 'No - FH Course'
		END AS 'Is FEE HELP',
		CASE 
			WHEN ce.VSLFlag = 'Y' THEN 'Yes - VSL Course'
			ELSE 'No - VSL Course'
		END AS 'Is VSL',

		-- EMPLOYMENT INFORMATION
		CASE 
			WHEN ce.WorksInNSW = 'Y' THEN 'Yes - Works in NSW' 
			WHEN ce.WorksInNSW = 'N' THEN 'No - Works in NSW' 
			ELSE NULL
		END AS 'Works in NSW',
		ce.OrganisationTypeDescription AS 'Employer Organisation Type',
		ce.EmployerPostcode as 'Employer Postcode',
		ce.EmployerSuburb as 'Employer Suburb',

		-- SBI CATEGORIES 
		ce.SBIGroup as 'SBI Group',
		ce.SBICategory as 'SBI Category',
		ce.SBISubCategory as 'SBI Sub Category',
		
		-- short codes for MD Dashboards
		awd_group.QualificationGroupShort as 'Award Group Code',

		--Jasons new funding category mapping
		fg_code_name.VETFeeHelpFundName as 'Funding Group Code',
		coalesce(cast(BPR_MAP_CODE.BPRSubCategoryID as varchar(10)),'Unmapped') AS 'BPR Mapping Code',
		coalesce(BPR_CAT.BPRCategoryName,'Unmapped') AS 'BPR Category',
		coalesce(BPR_SUBCAT.BPRSubCategoryName,'Unmapped') AS 'BPR Sub Category',

		coalesce(stm.[CostCentreName],'Others') as 'SKills Team Cost Centre Name',
		coalesce(stm.[CostCentreDescription],'Others') as 'SKills Team Cost Centre with Name',
		coalesce(stm.[RegioniPlan],'Others') as 'SKills Team Region - iPlan',
		coalesce(stm.[HeadTeachers],'Others') as 'SKills Team Head Teacher',
		coalesce(stm.[TeachingSection],'Others') as 'SKills Team Teaching Section',
		coalesce(stm.[ActivityLevel3],'Others') as 'SKills Team Activity Level 3',
		coalesce(stm.[SkillsTeam],'Others') as 'Skills Team',
		AQF.aqfGroupID as 'AQF Code',
		Case AQF.aqfGroupID when '1' then 'AQF' when '2' then 'Non AQF' END as 'AQF Description',
		ce.ProgramStartDate as 'Program Start Date',
		DAY(ce.ProgramStartDate) AS 'Program Start Date Day', 
		DATENAME(MONTH, ce.ProgramStartDate) AS 'Program Start Date Month',
		YEAR(ce.ProgramStartDate) AS 'Program Start Date Year',
		MONTH(ce.ProgramStartDate) AS SORT_PSD,
		ce.ReportingYear
	from compliance.AvetmissCourseEnrolment ce  
		join compliance.AvetmissReportingPeriod  rp on ce.ReportingYear = rp.ReportingYear and ce.ReportingDate = rp.ReportingDate

		--left join STS_DATA sts on ce.ID_CPU = sts.ID_CPU 
		--not required
		--left join UNIT_INSTANCE_OCCURRENCES uio on ce.ID_UIO = uio.ID_UIO
		--not required
		
		--left join REFERENCE_DATA anz on ce.ANZSCO = anz.value and anz.DOMAIN = 'ANZSCO'
		left join reference.Occupation anz on ce.ANZSCO = anz.OccupationCode 
			and anz._RecordCurrent = 1 and anz._RecordDeleted = 0

		--left join REFERENCE_DATA aptr on ce.apprtrainee = aptr.value and aptr.DOMAIN = 'APPRENTICE TRAINEE'
		--not required
		
		--left join REFERENCE_DATA awd on ce.AWARD = awd.value and awd.DOMAIN = 'AWARD CATEGORY'
		--left join REFERENCE_DATA awd_group on ce.AWARD = awd_group.value and awd_group.DOMAIN = 'AWARD GROUP'
		--left join REFERENCE_DATA awd_bpr on ce.AWARD = awd_bpr.value and awd_bpr.DOMAIN = 'AWARD_BPR'
		left join reference.QualificationType awd on ce.AWARD = awd.QualificationTypeID
			and awd._RecordCurrent = 1 and awd._RecordDeleted = 0
		left join reference.QualificationTypeMapping qtm on awd.QualificationTypeID = qtm.QualificationTypeID
			and qtm._RecordCurrent = 1 and qtm._RecordDeleted = 0
		left join reference.QualificationGroup awd_group on qtm.QualificationGroupID = awd_group.QualificationGroupID
			and awd_group._RecordCurrent = 1 and awd_group._RecordDeleted = 0
		Left Join reference.BprQualificationGroup bpr_qual_group on qtm.BPRQualifcationGroupID = bpr_qual_group.BPRQualificationGroupID
			and bpr_qual_group._RecordCurrent = 1 and bpr_qual_group._RecordDeleted = 0

		left join reference.QualificationTypeMapping aqf on awd.QualificationTypeID = aqf.QualificationTypeID
			and aqf._RecordCurrent = 1 and aqf._RecordDeleted = 0
		--left join REFERENCE_DATA foe on ce.FOE4 = foe.value and foe.DOMAIN = 'FOE'
		left join reference.FieldOfEducation foe on ce.FOE4 = foe.FieldOfEducationID
			and foe._RecordCurrent = 1 and foe._RecordDeleted = 0

		--left join REFERENCE_DATA fund_avet on ce.AVFUND = fund_avet.value and fund_avet.DOMAIN = 'FUNDING AVETMISS'
		left join reference.AvetmissFund fund_avet on ce.AVFUND = fund_avet.AvetmissFundID
			and fund_avet._RecordCurrent = 1 and fund_avet._RecordDeleted = 0

		--left join REFERENCE_DATA fund_tafe on ce.NZ_FUNDING = fund_tafe.value and fund_tafe.DOMAIN = 'FUNDING TAFE'
		left join reference.FundingSource fund_tafe on ce.CourseFundingSourceCode = fund_tafe.FundingSourceCode
			and fund_tafe._RecordCurrent = 1 and fund_tafe._RecordDeleted = 0

		--left join REFERENCE_DATA offering on ce.OFFERING_TYPE = offering.value and offering.DOMAIN = 'OFFERING TYPE'
		left join reference.OfferingType offering on ce.CourseEnrolmentOfferingType = offering.OfferingTypeCode
			and offering._RecordCurrent = 1 and offering._RecordDeleted = 0

		--left join REFERENCE_DATA sponsor on ce.SPONSOR_ORG_CODE = sponsor.VALUE and sponsor.DOMAIN = 'ORGANISATION SPONSOR'
		--not required

		--left join REFERENCE_DATA stsfund on ce.STSFUND = stsfund.VALUE and stsfund.DOMAIN = 'STS FUND'
		left join Reference.TsnswFund stsfund on ce.CourseFundingSourceCode = stsfund.TSNSWFundCode 
			and stsfund._RecordCurrent = 1 and stsfund._RecordDeleted = 0

		--left join REFERENCE_DATA stsfundgrp on ce.STSFUNDGROUP = stsfundgrp.VALUE and stsfundgrp.DOMAIN = 'STS FUND GROUP'
		left join Reference.TsnswFundGroup stsfundgrp on ce.TSNSWFundGroupCode = stsfundgrp.TSNSWFundGroupCode
			and stsfundgrp._RecordCurrent = 1 and stsfundgrp._RecordDeleted = 0

		--left join REFERENCE_DATA awdorder on ce.AWARD = awdorder.VALUE and awdorder.DOMAIN = 'AWARD ORDER'
		-- taken from QualificationTypeMapping

		--left join REFERENCE_DATA predom_dm on ce.DELIVERY_MODE_UNIT = predom_dm.VALUE and predom_dm.DOMAIN = 'DELIVERY MODE'
		left join Reference.DeliveryMode predom_dm on ce.UnitsDeliveryMode = predom_dm.DeliveryModeCode
			and predom_dm._RecordCurrent = 1 and predom_dm._RecordDeleted = 0
		
		--left join REFERENCE_DATA skills on ce.SKILLSPOINT = skills.VALUE and skills.DOMAIN = 'SKILLSPOINTS'
		left join reference.SkillsPoint skills on ce.SKILLSPOINT = skills.SkillsPointID
			and skills._RecordCurrent = 1 and skills._RecordDeleted = 0

		--left join REFERENCE_DATA wel on ce.WELFARE_STATUS = wel.VALUE and wel.DOMAIN = 'WELFARE STATUS'
		left join reference.WelfareStatus wel on ce.WelfareStatus = wel.WelfareStatusID
			and wel._RecordCurrent = 1 and wel._RecordDeleted = 0

		--left join REFERENCE_DATA stdr on ce.STUDY_REASON = stdr.VALUE and stdr.DOMAIN = 'STUDY REASON'
		left join reference.StudyReason stdr on ce.StudyReason = stdr.StudyReasonID
			and stdr._RecordCurrent = 1 and stdr._RecordDeleted = 0

		--used for short codes for MD Dashboards
		--left join REFERENCE_DATA awd_code on awd_group.description = awd_code.VALUE and awd_code.DOMAIN = 'AWARD_GROUP_CODE'
		--LEFT JOIN REFERENCE_DATA awd_bpr_group on awd_bpr_group.VALUE=awd.DESCRIPTION and awd_bpr_group.domain='AWARD_GROUP_CODE'
		--replaced with reference.QualificationGroup
		
		--left join REFERENCE_DATA fg_code on ce.NZ_FUNDING = fg_code.VALUE and fg_code.DOMAIN = 'FUNDING_VFH_CODE1'
		left join reference.FundingSourceVetFeeHelpFund fg_code on ce.CourseFundingSourceCode = fg_code.FundingSourceCode
			and fg_code._RecordCurrent = 1 and fg_code._RecordDeleted = 0
		left join reference.VetFeeHelpFund fg_code_name on fg_code.VETFeeHelpFundID = fg_code_name.VETFeeHelpFundID
			and fg_code_name._RecordCurrent = 1 and fg_code_name._RecordDeleted = 0

	    --LEFT JOIN REFERENCE_DATA BPR_MAP_CODE ON CE.NZ_FUNDING=BPR_MAP_CODE.VALUE AND BPR_MAP_CODE.DOMAIN='BPR_MAP_CODE'
		LEFT JOIN reference.FundingSourceBpr BPR_MAP_CODE ON CE.CourseFundingSourceCode = BPR_MAP_CODE.FundingSourceCode
			and BPR_MAP_CODE._RecordCurrent = 1 and BPR_MAP_CODE._RecordDeleted = 0
		--LEFT JOIN REFERENCE_DATA BPR_SUBCAT ON CE.NZ_FUNDING=BPR_SUBCAT.VALUE AND BPR_SUBCAT.DOMAIN='BPR_SUBCAT'
		LEFT JOIN reference.BprSubCategory BPR_SUBCAT ON BPR_MAP_CODE.BPRSubCategoryID = BPR_SUBCAT.BPRSubCategoryID
			and BPR_SUBCAT._RecordCurrent = 1 and BPR_SUBCAT._RecordDeleted = 0
		--LEFT JOIN REFERENCE_DATA BPR_CAT ON CE.NZ_FUNDING=BPR_CAT.VALUE AND BPR_CAT.DOMAIN='BPR_CAT'
		LEFT JOIN reference.BprCategory BPR_CAT ON BPR_SUBCAT.BPRCategoryID = BPR_CAT.BPRCategoryID
			and BPR_CAT._RecordCurrent = 1 and BPR_CAT._RecordDeleted = 0
		
		--left join skills_team_mapping stm on ce.functional_unit = stm.cost_centre
		left join [edw].[SAP_CostCentreSkillsTeam] stm on ce.CourseOfferingFunctionalUnit = stm.CostCentreCode
			and stm._RecordCurrent = 1 and stm._RecordDeleted = 0

		Left Join [reference].[ProgramStream] eps on eps.ProgramStreamID = ce.ProgramStream 
			and eps._RecordCurrent = 1 and eps._RecordDeleted = 0

		Left Join [reference].[ProgressReason] PR on  pr.ProgressReasonCode = ce.CourseProgressReason
			and PR._RecordCurrent = 1 and PR._RecordDeleted = 0

	where ce._RecordCurrent = 1
	and ce._RecordDeleted = 0
	--and ce.AvetmissCourseCode not in ('168-LSAB','168-LSD','168-LSFS','168-LSG' ,'161-N1000','161-N2000','161-N2001','161-N2002')