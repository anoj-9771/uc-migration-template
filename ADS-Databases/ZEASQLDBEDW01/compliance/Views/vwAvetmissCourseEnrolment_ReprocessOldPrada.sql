







CREATE View [compliance].[vwAvetmissCourseEnrolment_ReprocessOldPrada]
as
select 
		cast(ce.ID_PC as varchar(50)) as ID_PC, 
		cast(ce.ID_UIO as varchar(50)) as ID_UIO,
		cast(ce.ID_CPU as varchar(50)) as ID_CPU, 
		NULL as ID_FAC,
		CASE 
		WHEN FY=1 then SUBSTRING(ce.id_NCC,5,20)
		else  SUBSTRING(ce.id_NCC,3,20)
		end as ID_NCC,
		ce.REPORT_DATE, 
		ce.EXTRACT_DATE,
		ce.ENROLMENT_YEAR,
		ce.INSTITUTE,
		CASE 
			WHEN ce.TAFE_DIGITAL = 1 AND (ce.OTEN = 0 or ce.OTEN is null) AND ce.TOL = 1
				THEN ce.delivery_loc + '-TOL'
			WHEN ce.TAFE_DIGITAL = 1 AND (ce.OTEN = 0 or ce.OTEN is null) AND ce.TNW = 1
				THEN ce.delivery_loc + '-TNW'
			WHEN ce.TAFE_DIGITAL = 1 AND (ce.OTEN = 0 or ce.OTEN is null) AND ce.[OPEN] = 1
				THEN ce.delivery_loc + '-OPEN'
			ELSE delivery_loc
		END AS 'Delivery Location', 
		ce.DATE_OFFER_START as 'Start Date',
		DAY(ce.DATE_OFFER_START) AS 'Start Date Day', 
		DATENAME(MONTH, ce.DATE_OFFER_START) AS 'Start Date Month',
		YEAR(ce.DATE_OFFER_START) AS 'Start Date Year',
		MONTH(ce.DATE_OFFER_START) AS SORT_SD,
		ce.DATE_OFFER_END as 'End Date',
		DAY(ce.DATE_OFFER_END) AS 'End Date Day', 
		DATENAME(MONTH, ce.DATE_OFFER_END) AS 'End Date Month',
		YEAR(ce.DATE_OFFER_END) AS 'End Date Year',
		MONTH(ce.DATE_OFFER_END) AS SORT_ED,
		ce.DATE_AWARDED AS 'Date Awarded',
		DAY(ce.DATE_AWARDED) AS 'Date Awarded Day', 
		DATENAME(MONTH, ce.DATE_AWARDED) AS 'Date Awarded Month',
		YEAR(ce.DATE_AWARDED) AS 'Date Awarded Year',
		MONTH(ce.DATE_AWARDED) AS SORT_DA,
		ce.AGE as 'Age',
		ce.AGEGRP as 'Age Group',
		case when ce.Age < 20 then '0-19'
			when ce.Age > 19 and ce.Age < 25 then '20-24'
			when ce.Age > 24 and ce.Age < 35 then '25-34'
			when ce.Age > 34 and ce.Age < 46 then '35-45'
			else '45 Plus'
		end as 'Simple Age Group',
		CASE 
			WHEN ce.ANZSCO IS NOT NULL THEN CONCAT(ce.ANZSCO, ' ', anz.OccupationName)
			ELSE NULL
		END as ANZSCO,
		case 
			when ce.APPRTRAINEE = 'A' then 'Apprentice'
			when ce.APPRTRAINEE = 'T' then 'Trainee' 
			else 'Neither'
		end as 'Apprentice or Trainee', 
		ce.ARIA11B,
		awd_group.QualificationGroupDescription as 'Award Group', 
		CASE 
			WHEN ce.HIGHED = 1 OR qtm.BPRLevelID = '2' THEN 'Higher Level Qualifications'
			else 'Lower Level Qualifications'
		END as 'AWARD BPR',

--		coalesce(awd_group.QualificationGroupShort, '09. Not mapped to a Qual') as 'Award BPR Name',
	coalesce (bpr_qual_group.BPRQualificationGroupDescription, '09. Not Mapped to a Qualification Level') as 'Award BPR Name',
		awd.QualificationTypeName as 'Award Name',
		cast(qtm.[Order] as int) AS AWARD_NAME_SORT, ----*********

		ce.AWARD_DESCRIPTION AS 'Award Description',
		ce.AWARD_STATUS as 'Award Status1',
		ce.CALOCC_CODE as 'Calocc Code', 
		CASE 
			WHEN ce.COMMENCE_FLAG = 'Y' THEN 'Commencing'
			WHEN ce.COMMENCE_FLAG = 'N' THEN 'Continuing'
			ELSE null
		END as 'Commencement Flag', 
		DATE_COMMENCE_SEMESTER as 'Commencement Semester', 
		year_commence as 'Commencement Year', 
		COMMITMENT_IDENTIFIER as 'Commitment ID',
		CASE
			WHEN COMMITMENT_IDENTIFIER IS NOT NULL THEN 'CID Provided'
			ELSE 'CID not Provided'
		END AS 'Commitment ID Provided',
		CASE
			WHEN GRAD = 1 THEN 'Yes - Completed'
			ELSE 'No - Not Completed'
		END as 'Completion Flag', 
		year_complete as 'Completion Year',
		CASE
			WHEN COREFND = '1' THEN 'Core'
			ELSE 'Non Core'
		END as 'Core Non Core', 
		CASE
			WHEN avetcount = 1 THEN 'Included'
			WHEN avetcount is null AND (ENRFLAG = 1 and ENROLCOUNT = 1) THEN 'Included'
			ELSE 'Exlcuded'
		END as 'Count AVETMISS', 
		CASE
			WHEN ce.AVETMISS_FLAG = 'Y' THEN 'Include'
			ELSE 'Exlude'
		END as 'Include in AVETMISS Flag',
		CASE
			WHEN ENROLCOUNT = 1 THEN 'Valid'
			ELSE 'Invalid'
		END as 'Count Valid Enrolments', 
		course_code as 'Course Code',
		CSENM as 'Course Name',
		CASE 
			WHEN ce.FOE4 IS NOT NULL THEN CONCAT(ce.FOE4, ' ', FOE.FieldOfEducationDescription)
			ELSE NULL
		END AS 'Field of Education',
		CASE 
			WHEN ce.AVFUND IS NOT NULL OR ce.AVFUND <> 0 THEN CONCAT(cast(ce.AVFUND as varchar(10)), ' ', fund_avet.AvetmissFundName)
			ELSE NULL
		END AS 'Funding Source - AVETMISS',
		CASE 
			WHEN ce.NZ_FUNDING IS NOT NULL THEN CONCAT(ce.NZ_FUNDING, ' ', fund_tafe.FundingSourceDescription)
			ELSE NULL
		END AS 'Funding Source - TAFE',
		CASE 
			WHEN ce.FUNDING_VFH IS NOT NULL THEN ce.FUNDING_VFH
			ELSE NULL
		END AS 'Funding Category - VFH',
		CASE 
			WHEN HIGHED = 1 THEN 'Yes - Higher Ed'
			ELSE 'No - Higher Ed'
		END AS 'Higher Education', 
		HQUALN AS 'Highest Previous Qualification', 
		SECED AS 'Highest School Level',
		/*
		CASE 
			WHEN sts.COMMITMENT_ID IS NOT NULL THEN 'Yes'
			ELSE 'No'
		END AS 'In eReporting',
		*/
		'' AS 'In eReporting',
		ce.ISC AS 'Industry Skills Council',
		ce.LGANAM AS 'LGA Name', 
		--NULL AS NATIONAL_COURSE_CODE,
		offering.OfferingTypeDescription AS 'Offering Type',
		PROGRESS_CODE AS 'Progress Code', 
		PROGRESS_DATE AS 'Progress Date',
		DAY(PROGRESS_DATE) AS 'Progress Date Day', 
		DATENAME(MONTH, PROGRESS_DATE) AS 'Progress Date Month',
		YEAR(PROGRESS_DATE) AS 'Progress Date Year',
		MONTH(PROGRESS_DATE) AS SORT_PD,
		PROGRESS_REASON AS 'Progress Reason',
		PROGRESS_STATUS AS 'Progress Status',
		RIND11 AS 'Regional Remote Flag',
		CASE
			WHEN SHELL_ENROL = 'Y' THEN 'Yes - Shell Enrol'
			ELSE 'No - Shell Enrol'
		END AS 'Shell Enrolment', 
		'' AS 'eReporting Region',
		CASE
			WHEN SPONSOR_ORG_CODE IS NOT NULL THEN CONCAT(SUBSTRING(SPONSOR_ORG_CODE, 4, 10), ' ', sponsor.DESCRIPTION)
			WHEN sponsor.DESCRIPTION IS NULL THEN NULL
			ELSE NULL
		END AS 'Sponsor Code',
		UNITTOT AS 'Total Units', 
		FAILTOT AS 'Total Units Fail',
		PASSTOT AS 'Total Units Pass', 
		WDRAWTOT AS 'Total Units Withdrawn',
		TRAINING_CONTRACT_IDENTIFIER AS 'Training Contract ID',
		'' AS 'TSNSW Fund',
		'' AS 'TSNSW Fund Group',
		CASE 
			WHEN validunitflag = 1 THEN 'Valid Units'
			WHEN REPORT_DATE IN (2014, 2015) THEN NULL
			ELSE 'Invalid Units'
		END AS 'Finalised Unit Flag',
		CASE 
			WHEN validtot = 0 THEN NULL
			ELSE validtot
		END AS 'Finalised Unit Total',
		CASE 
			WHEN COURSE_CODE LIKE '%-999999' THEN 'Unit Only Enrolment'
			ELSE 'Course Enrolment'
		END AS 'Unit Only Flag',
		WAIVER_CODES AS 'Waiver Codes',
		WHO_TO_PAY AS 'Who To Pay',
		FUNCTIONAL_UNIT AS 'Enrolment Teaching Section Code',
		uio.FES_FULL_NAME AS 'Enrolment Teaching Section Name',
		SUBSTRING(ce.functional_unit,1,4) as 'Enrolment Teaching Unit',
		SA_STATE AS 'SA State',
		SA4_NAME AS 'SA4',
		SA2_NAME AS 'SA2', 
		predom_dm.DeliveryModeDescription as 'Predominant Delivery Mode',
		CASE 
			WHEN ce.LTU_EVIDENCE = 'Y' THEN 'Yes - Long Term Unemployed'
			ELSE 'No - Long Term Unemployed'
		END AS 'Long Term Unemployed',
		wel.WelfareStatusDescription as 'Welfare Status',
		skills.SkillsPointDescription as 'SkillsPoints Industry',
		stdr.StudyReasonDescription as 'Study Reason', 
		CASE 
			WHEN ce.NZ_FUNDING IN ('001J', '001T') THEN 'Yes - Fully Subsidised' 
			ELSE 'No - Fully Subsidised'
		END AS 'Fully Subsidised',
		CASE 
			WHEN ce.CTONLY = '1' THEN 'Yes - Credit Transfer Only' 
			ELSE NULL 
		END AS 'Credit Transfer Only',
		CASE 
			WHEN ce.ONLMTOTUNIT > 0 AND ce.ONLMTOTUNIT is not null THEN 'Yes - Online Units' 
			ELSE 'No - Online Units' 
		END AS 'Online Units',
		
		
		-- MEASURES 
		CASE 
			WHEN totalhrs = 0 THEN NULL 
			ELSE totalhrs
		END as ASH_COUNT,
		CASE
			WHEN avetcount = 1 THEN 1
			WHEN avetcount is null AND (ENRFLAG = 1 and ENROLCOUNT = 1) THEN 1
			ELSE NULL
		END as AVETMISS_COUNT, 
		CASE 
			WHEN COMMENCE_FLAG = 'Y' THEN 1
			ELSE NULL
		END as COMMENCEMENT_COUNT, 
		CASE
			WHEN GRAD = 1 THEN 1
			ELSE NULL
		END as COMPLETION_COUNT, 
		CASE 
			WHEN COMMENCE_FLAG = 'N' THEN 1
			ELSE null
		END as CONTINUING_COUNT, 
		CASE 
			WHEN out71hrs = 0 THEN NULL
			ELSE out71hrs
		END as CONTINUING_ASH, 
		1 as ENROLMENT_COUNT, 
		CASE
			WHEN ENROLCOUNT = 1 THEN 1
			ELSE NULL
		END as VALID_COUNT,
		CASE 
			WHEN rplhrs = 0 THEN NULL 
			ELSE rplhrs
		END AS RPL_ASH_COUNT,
		CASE 
			WHEN trnghrs = 0 THEN NULL
			ELSE trnghrs
		END AS TRAINING_ASH_COUNT,
		CASE 
			WHEN TRANCOUNT = 0 THEN NULL 
			ELSE TRANCOUNT 
		END AS TRANSITION_COUNT, 

		-- EBS FEES 
		FEE_ACTUAL AS FEE_ACTUAL, 
		FEE_ADJUSTMENT AS FEE_ADJUSTMENT,
		FEE_INSTALMENT AS FEE_INSTALMENT, 
		FEE_ORIGINAL AS FEE_ORIGINAL, 
		FEE_OUTSTANDING AS FEE_OUTSTANDING, 
		FEE_PAID AS FEE_PAID,

		-- EREPORTING PAYMENTS 
		NULL AS LOC_LOADING_PAYMENT, 
		NULL AS NEEDS_LOADING_PAYMENT, 
		NULL AS STAGE_PAYMENT, 

		-- SMART & SKILLED MEASURES
		ce.SS_STATUS as 'eReporting Status',
		ce.SS_PROGRAM_STREAM AS 'eReporting Program Stream',
		ce.SS_LOADING_LOC AS LOC_LOADING,
		ce.SS_LOADING_NEEDS AS NEED_LOADING, 
		ce.SS_STANDARD_SUBSIDY_AMOUNT AS STANDARD_SUBSIDY, 
		NULL AS SUBSIDY_ADJUSTMENT,
		ce.SS_SUBSIDY_AMOUNT AS SUBSIDY_AMOUNT,

		-- SEIFA
		DECILE AS 'SEIFA - Decile', 
		PERCENTILE AS 'SEIFA - Percentile',

		-- REGIONS 
		CASE 
			WHEN ce.TAFE_DIGITAL =  1 AND (OTEN = 1 OR TOL = 1 OR TNW = 1 OR [OPEN] = 1) THEN 'TAFE Digital'
			WHEN ce.delivery_loc = 'OTE' THEN 'TAFE Digital'
			ELSE 'Region'
		END AS 'TAFE Digital Flag',
		ce.REGION AS 'Region',

		-- TAFE DIGITAL 
		CASE 
			WHEN ce.TOL = 1 THEN 'TAFE Online'
			WHEN ce.TNW = 1 THEN 'TAFE Now'
			WHEN ce.OTEN = 1 THEN 'OTEN'
			WHEN ce.[OPEN] = 1 THEN 'Open College'
			WHEN delivery_loc = 'OTE' THEN 'OTEN'
			ELSE 'Region'
		END AS 'TAFE Digital Area',
				CASE 
			WHEN ce.TOL = 1 THEN 4
			WHEN ce.TNW = 1 THEN 3
			WHEN ce.OTEN = 1 THEN 2
			WHEN ce.[OPEN] = 1 THEN 1
			WHEN delivery_loc = 'OTE' THEN 2
			ELSE 5
		END AS SORT_TDA,


		-- TRANSITION
		ce.TRANSITIONED_FROM_ID AS 'Transitioned from ID',
		ce.TRANSITIONED_FROM AS 'Transitioned from Course Code',
		ce.TRANSITIONED_FROM_CALOCC AS 'Transitioned from Calocc Code',
		ce.ORIGINAL_START_DATE as 'Original Start Date', 
		DAY(ce.ORIGINAL_START_DATE) AS 'Original Start Date Day', 
		DATENAME(MONTH, ce.ORIGINAL_START_DATE) AS 'Original Start Date Month',
		YEAR(ce.ORIGINAL_START_DATE) AS 'Original Start Date Year',
		MONTH(ce.ORIGINAL_START_DATE) AS SORT_OSD,
		ce.TRANSITION_FLAG as 'Transition Flag',

		-- VET FEE HELP
		CASE 
			WHEN ce.VFH_COURSE = 'Y' THEN 'Yes - VFH Course'
			ELSE 'No - VFH Course'
		END AS 'Is VET FEE HELP',
		CASE 
			WHEN ce.FEE_HELP_FLAG = 'Y' THEN 'Yes - FH Course'
			ELSE 'No - FH Course'
		END AS 'Is FEE HELP',
		CASE 
			WHEN ce.VSL_FLAG = 'Y' THEN 'Yes - VSL Course'
			ELSE 'No - VSL Course'
		END AS 'Is VSL',

		-- EMPLOYMENT INFORMATION
		CASE 
			WHEN ce.WORKS_IN_NSW = 'Y' THEN 'Yes - Works in NSW' 
			WHEN ce.WORKS_IN_NSW = 'N' THEN 'No - Works in NSW' 
			ELSE NULL
		END AS 'Works in NSW',
		CASE 
			WHEN ce.EMP_ORG_TYPE is not NULL then EMP_ORG_TYPE 
			ELSE NULL
		END AS 'Employer Organisation Type',
		ce.EMP_POSTCODE as 'Employer Postcode',
		ce.EMP_ALT_EMPLOYER_SUBURB as 'Employer Suburb',

		-- SBI CATEGORIES 
		ce.SBI_GROUP as 'SBI Group',
		ce.SBI_CATEGORY as 'SBI Category',
		ce.SBI_SUBCATEGORY as 'SBI Sub Category',
		
		-- short codes for MD Dashboards
		awd_group.QualificationGroupDescription as 'Award Group Code',

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
		NULL as 'AQF Code',
		NULL as 'AQF Description',
		NULL as 'Program Start Date',
		NULL as 'Program Start Date Day', 
		NULL as 'Program Start Date Month',
		NULL as 'Program Start Date Year',
		NULL as SORT_PSD

	from  [edw].[Prada_dbo_COURSE_ENROLMENTS] ce--edw.Prada_dbo_COURSE_ENROLMENTS_FINALS ce

		--left join STS_DATA sts on ce.ID_CPU = sts.ID_CPU 
		--not required
		--left join UNIT_INSTANCE_OCCURRENCES uio on ce.ID_UIO = uio.ID_UIO
		/*left join
		(
			select distinct uio.OWNING_ORGANISATION, org.FES_FULL_NAME
			from edw.OneEBS_EBS_0165_UNIT_INSTANCE_OCCURRENCES uio
				join edw.OneEBS_EBS_0165_ORGANISATION_UNITS org on uio.OWNING_ORGANISATION = org.ORGANISATION_CODE
			where uio._RecordCurrent = 1 and uio._RecordDeleted = 0
			and org._RecordCurrent = 1 and org._RecordDeleted = 0
		) uio on ce.functional_unit = uio.OWNING_ORGANISATION
		*/
		left join 
		(
			select distinct TeachingSectionCode as OWNING_ORGANISATION, TeachingSectionDescription as FES_FULL_NAME
			from compliance.AvetmissCourseOffering uio 
			where _RecordCurrent = 1 and _RecordDeleted = 0
		) uio on ce.functional_unit = uio.OWNING_ORGANISATION

		--left join REFERENCE_DATA anz on ce.ANZSCO = anz.value and anz.DOMAIN = 'ANZSCO'
		left join reference.Occupation anz on ce.ANZSCO = anz.OccupationCode 
			and anz._RecordCurrent = 1 and anz._RecordDeleted = 0

		--left join REFERENCE_DATA aptr on ce.apprtrainee = aptr.value and aptr.DOMAIN = 'APPRENTICE TRAINEE'
		--not required
		
		--left join REFERENCE_DATA awd on ce.AWARD = awd.value and awd.DOMAIN = 'AWARD CATEGORY'
		--left join REFERENCE_DATA awd_group on ce.AWARD = awd_group.value and awd_group.DOMAIN = 'AWARD GROUP'
		--left join REFERENCE_DATA awd_bpr on ce.AWARD = awd_bpr.value and awd_bpr.DOMAIN = 'AWARD_BPR'
		left join reference.QualificationType awd on ce.AWARD = Cast (awd.QualificationTypeID as varchar)
			and awd._RecordCurrent = 1 and awd._RecordDeleted = 0
		left join reference.QualificationTypeMapping qtm on Cast (awd.QualificationTypeID as varchar) = Cast (qtm.QualificationTypeID as varchar)
			and qtm._RecordCurrent = 1 and qtm._RecordDeleted = 0
		left join reference.QualificationGroup awd_group on Cast(qtm.QualificationGroupID as varchar) = Cast(awd_group.QualificationGroupID as varchar)
			and awd_group._RecordCurrent = 1 and awd_group._RecordDeleted = 0

		Left Join reference.BprQualificationGroup bpr_qual_group on Cast(qtm.BPRQualifcationGroupID as varchar) = cast (bpr_qual_group.BPRQualificationGroupID as varchar)
			and bpr_qual_group._RecordCurrent = 1 and bpr_qual_group._RecordDeleted = 0




		--left join REFERENCE_DATA foe on ce.FOE4 = foe.value and foe.DOMAIN = 'FOE'
		left join reference.FieldOfEducation foe on ce.FOE4 = foe.FieldOfEducationID
			and foe._RecordCurrent = 1 and foe._RecordDeleted = 0

		--left join REFERENCE_DATA fund_avet on ce.AVFUND = fund_avet.value and fund_avet.DOMAIN = 'FUNDING AVETMISS'
		left join reference.AvetmissFund fund_avet on ce.AVFUND = cast(fund_avet.AvetmissFundID as varchar)
			and fund_avet._RecordCurrent = 1 and fund_avet._RecordDeleted = 0

		--left join REFERENCE_DATA fund_tafe on ce.NZ_FUNDING = fund_tafe.value and fund_tafe.DOMAIN = 'FUNDING TAFE'
		left join reference.FundingSource fund_tafe on ce.NZ_FUNDING = fund_tafe.FundingSourceCode
			and fund_tafe._RecordCurrent = 1 and fund_tafe._RecordDeleted = 0

		--left join REFERENCE_DATA offering on ce.OFFERING_TYPE = offering.value and offering.DOMAIN = 'OFFERING TYPE'
		left join reference.OfferingType offering on ce.OFFERING_TYPE = offering.OfferingTypeCode
			and offering._RecordCurrent = 1 and offering._RecordDeleted = 0

		--left join REFERENCE_DATA sponsor on ce.SPONSOR_ORG_CODE = sponsor.VALUE and sponsor.DOMAIN = 'ORGANISATION SPONSOR'
		left join
		(
			select org.ORGANISATION_CODE, org.FES_FULL_NAME as Description
			from edw.OneEBS_EBS_0165_ORGANISATION_UNITS org
			where org.ORGANISATION_TYPE = 'SPONSOR'
			and org._RecordCurrent = 1 and org._RecordDeleted = 0
		) sponsor on sponsor.ORGANISATION_CODE = ce.SPONSOR_ORG_CODE

		----left join REFERENCE_DATA stsfund on ce.STSFUND = stsfund.VALUE and stsfund.DOMAIN = 'STS FUND'
		--left join Reference.TsnswFund stsfund on ce.STSFUND = stsfund.TSNSWFundCode 
		--	and stsfund._RecordCurrent = 1 and stsfund._RecordDeleted = 0

		----left join REFERENCE_DATA stsfundgrp on ce.STSFUNDGROUP = stsfundgrp.VALUE and stsfundgrp.DOMAIN = 'STS FUND GROUP'
		--left join Reference.TsnswFundGroup stsfundgrp on ce.STSFUNDGROUP = stsfundgrp.TSNSWFundGroupCode
		--	and stsfundgrp._RecordCurrent = 1 and stsfundgrp._RecordDeleted = 0

		--left join REFERENCE_DATA awdorder on ce.AWARD = awdorder.VALUE and awdorder.DOMAIN = 'AWARD ORDER'
		-- taken from QualificationTypeMapping

		--left join REFERENCE_DATA predom_dm on ce.DELIVERY_MODE_UNIT = predom_dm.VALUE and predom_dm.DOMAIN = 'DELIVERY MODE'
		left join Reference.DeliveryMode predom_dm on ce.DELIVERY_MODE_UNIT = predom_dm.DeliveryModeCode
			and predom_dm._RecordCurrent = 1 and predom_dm._RecordDeleted = 0
		
		--left join REFERENCE_DATA skills on ce.SKILLSPOINT = skills.VALUE and skills.DOMAIN = 'SKILLSPOINTS'
		left join reference.SkillsPoint skills on CASE ce.SKILLSPOINT
													WHEN 'AGRI' Then 2
													WHEN 'CADI' Then 3
													WHEN 'CPES' Then 4
													WHEN 'HWCS' Then 6
													WHEN 'IEAC' Then 7
													WHEN 'IMRS' Then 1
													WHEN 'OTHR' Then 99
													WHEN 'SCEC' Then 8
													WHEN 'TABS' Then 5
													WHEN 'TAES' Then 9
													Else 99
													END		= skills.SkillsPointID
			and skills._RecordCurrent = 1 and skills._RecordDeleted = 0

		--left join REFERENCE_DATA wel on ce.WELFARE_STATUS = wel.VALUE and wel.DOMAIN = 'WELFARE STATUS'
		left join reference.WelfareStatus wel on ce.WELFARE_STATUS = wel.WelfareStatusID
			and wel._RecordCurrent = 1 and wel._RecordDeleted = 0

		--left join REFERENCE_DATA stdr on ce.STUDY_REASON = stdr.VALUE and stdr.DOMAIN = 'STUDY REASON'
		left join reference.StudyReason stdr on ce.STUDY_REASON = stdr.StudyReasonID
			and stdr._RecordCurrent = 1 and stdr._RecordDeleted = 0

		--used for short codes for MD Dashboards
		--left join REFERENCE_DATA awd_code on awd_group.description = awd_code.VALUE and awd_code.DOMAIN = 'AWARD_GROUP_CODE'
		--LEFT JOIN REFERENCE_DATA awd_bpr_group on awd_bpr_group.VALUE=awd.DESCRIPTION and awd_bpr_group.domain='AWARD_GROUP_CODE'
		--replaced with reference.QualificationGroup
		
		--left join REFERENCE_DATA fg_code on ce.NZ_FUNDING = fg_code.VALUE and fg_code.DOMAIN = 'FUNDING_VFH_CODE1'
		left join reference.FundingSourceVetFeeHelpFund fg_code on ce.NZ_FUNDING = fg_code.FundingSourceCode
			and fg_code._RecordCurrent = 1 and fg_code._RecordDeleted = 0
		left join reference.VetFeeHelpFund fg_code_name on Cast(fg_code.VETFeeHelpFundID as varchar) = cast (fg_code_name.VETFeeHelpFundID as varchar)
			and fg_code_name._RecordCurrent = 1 and fg_code_name._RecordDeleted = 0

	    --LEFT JOIN REFERENCE_DATA BPR_MAP_CODE ON CE.NZ_FUNDING=BPR_MAP_CODE.VALUE AND BPR_MAP_CODE.DOMAIN='BPR_MAP_CODE'
		LEFT JOIN reference.FundingSourceBpr BPR_MAP_CODE ON CE.NZ_FUNDING = BPR_MAP_CODE.FundingSourceCode
			and BPR_MAP_CODE._RecordCurrent = 1 and BPR_MAP_CODE._RecordDeleted = 0
		--LEFT JOIN REFERENCE_DATA BPR_SUBCAT ON CE.NZ_FUNDING=BPR_SUBCAT.VALUE AND BPR_SUBCAT.DOMAIN='BPR_SUBCAT'
		LEFT JOIN reference.BprSubCategory BPR_SUBCAT ON BPR_MAP_CODE.BPRSubCategoryID = BPR_SUBCAT.BPRSubCategoryID
			and BPR_SUBCAT._RecordCurrent = 1 and BPR_SUBCAT._RecordDeleted = 0
		--LEFT JOIN REFERENCE_DATA BPR_CAT ON CE.NZ_FUNDING=BPR_CAT.VALUE AND BPR_CAT.DOMAIN='BPR_CAT'
		LEFT JOIN reference.BprCategory BPR_CAT ON cast(BPR_SUBCAT.BPRCategoryID as varchar ) = cast (BPR_CAT.BPRCategoryID as varchar)
			and BPR_CAT._RecordCurrent = 1 and BPR_CAT._RecordDeleted = 0
		
		--left join skills_team_mapping stm on ce.functional_unit = stm.cost_centre
		left join [edw].[SAP_CostCentreSkillsTeam] stm on ce.functional_unit = stm.CostCentreCode
			and stm._RecordCurrent = 1 and stm._RecordDeleted = 0