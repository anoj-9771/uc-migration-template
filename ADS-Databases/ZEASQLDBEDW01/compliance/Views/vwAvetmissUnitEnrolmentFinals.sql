
create   view compliance.vwAvetmissUnitEnrolmentFinals as
	select 
		ue.ID_PC, 
		ue.ID_UUIO, 
		ue.ID_CUIO,
		(substring(cast(ue.ENROLMENT_YEAR as varchar(20)),3,2) + cast(ue.INSTITUTE as varchar(3)) + ue.delivery_loc + substring(cast(ue.ID_PC as varchar(25)),6,12) + ue.COURSE10) as ID_CPU, 
		ue.ID_UPU, 
		ue.INSTITUTE, 
		ue.DELIVERY_LOC as LOCATION_CODE,
		ue.ENROLMENT_YEAR AS ENROLMENT_YEAR,
		ue.REPORT_DATE, 
		ue.EXTRACT_DATE, 
		ue.START_DATE,
		DAY(ue.START_DATE) AS 'Start Date Day', 
		DATENAME(MONTH, ue.START_DATE) AS 'Start Date Month',
		YEAR(ue.START_DATE) AS 'Start Date Year',
		MONTH(ue.START_DATE) AS SORT_SD,
		ue.END_DATE,
		DAY(ue.END_DATE) AS 'End Date Day', 
		DATENAME(MONTH, ue.END_DATE) AS 'End Date Month',
		YEAR(ue.END_DATE) AS 'End Date Year',
		MONTH(ue.END_DATE) AS SORT_ED,
		ue.DATE_AWARDED AS DATE_AWARDED,
		DAY(ue.DATE_AWARDED) AS 'Date Awarded Day', 
		DATENAME(MONTH, ue.DATE_AWARDED) AS 'Date Awarded Month',
		YEAR(ue.DATE_AWARDED) AS 'Date Awarded Year',
		MONTH(ue.DATE_AWARDED) AS SORT_DA,
		ue.AWARD_STATUS AS 'Award Status',
		ue.CALOCC_CODE AS 'Calocc Code',
		--comm_prog.DESCRIPTION AS 'Commencing Program Identifier', 
		CASE ue.COMMEN_PROG
			WHEN 3 THEN 'Commencing enrolment in the program'
			WHEN 4 THEN 'Continuing enrolment in the program from a previous year'
			WHEN 8 THEN 'Unit of competency or module enrolment only'
			ELSE ''
		END AS 'Commencing Program Identifier',
		CASE
			WHEN ue.ENROLCOUNT = 1 THEN 'Valid'
			ELSE 'Invalid'
		END AS 'Count Valid Unit Enrolments', 
		/*
		CASE 
			WHEN ue.DELIVERY_MODE IS NOT NULL THEN CONCAT(DELIVERY_MODE, ' ', del_mode.DESCRIPTION)
			ELSE NULL
		END AS 'Delivery Mode - AVETMISS',*/
		del_mode.AvetmissDeliveryModeDescription AS 'Delivery Mode - AVETMISS',
		CASE
			WHEN ue.TAFE_DELIVERY_MODE IS NOT NULL THEN del_tafe.DeliveryModeDescription
			ELSE NULL
		END AS 'Delivery Mode - TAFE', 
		CASE
			WHEN ue.FINALUNIT = 1 THEN 'Final Unit' 
			ELSE 'Not Final Unit'
		END AS 'Final Unit', 
		CASE 
			WHEN ue.FUNDING_NAT IS NOT NULL THEN CONCAT(ue.FUNDING_NAT, ' ', fund_avet.AvetmissFundName)
			ELSE NULL
		END AS 'Funding - AVETMISS',
		CASE 
			WHEN ue.FUNDING_SOURCE IS NOT NULL THEN CONCAT(FUNDING_SOURCE, ' ', fund_tafe.FundingSourceDescription)
			ELSE NULL 
		END AS 'Funding - TAFE', 
		CASE 
			WHEN ue.FUNDING_SPECIFIC IS NOT NULL THEN CONCAT(ue.FUNDING_SPECIFIC, ' ', fund_spec.SpecificFundingName)
			ELSE NULL 
		END AS 'Funding Specific', 
		CASE 
			WHEN ue.NO_ACA_HISTORY IS NOT NULL THEN 'No Academic History'
			ELSE 'Academic History'
		END AS 'No Academic History', 
		CASE 
			WHEN ue.ID_UPU IS NOT NULL THEN 'Unit Enrolment'
			ELSE 'No Unit Enrolment'
		END AS 'No Unit Enrolment', 
		CASE 
			WHEN ue.OUTCOME_NATIONAL IS NOT NULL THEN CONCAT(ue.OUTCOME_NATIONAL, ' ', outcome_nat.AvetmissOutcomeDescription)
			ELSE NULL 
		END AS 'Outcome - AVETMISS', 
		CASE 
			WHEN ue.OUTCOME_ID_TRAINING IS NOT NULL THEN 
				CASE 
					WHEN ISNUMERIC(ue.OUTCOME_ID_TRAINING) = 1 THEN 'HOURS ONLY'
					ELSE CONCAT(ue.OUTCOME_ID_TRAINING, ' ', outcome_tafe.GradeDescription)
				END
		END AS 'Outcome - TAFE',
		ue.PROGRESS_CODE AS 'Progress Code', 
		ue.PROGRESS_STATUS AS 'Progress Status', 
		CASE 
			WHEN ue.PROGRESS_REASON IS NOT NULL THEN prog_reason.ProgressReasonDescription 
			ELSE NULL 
		END AS 'Progress Reason',
		DAY(ue.PROGRESS_DATE) AS 'Progress Date Day', 
		DATENAME(MONTH, ue.PROGRESS_DATE) AS 'Progress Date Month',
		YEAR(ue.PROGRESS_DATE) AS 'Progress Date Year',
		MONTH(ue.PROGRESS_DATE) AS SORT_PD,
		CASE 
			WHEN ue.RECOGNITION_TYPE IS NOT NULL THEN CONCAT(ue.RECOGNITION_TYPE, ' ', recog.RecognitionTypeDescrtiption)
			ELSE NULL
		END AS 'Recognition Type', 
		CASE 
			WHEN ue.SPONSOR_ORG_CODE IS NOT NULL THEN CONCAT(ue.SPONSOR_ORG_CODE, ' ', sponsor.DESCRIPTION)
			ELSE NULL 
		END AS 'Sponsor Org Code',
		CASE 
			WHEN ue.STUDY_REASON IS NOT NULL THEN CONCAT(ue.STUDY_REASON, ' ', study.StudyReasonDescription)
			ELSE NULL
		END AS 'Study Reason', 
		CASE 
			WHEN ue.VALIDUNIT = 1 THEN 'Finalised'
			ELSE 'Not Finalised'
		END AS 'Valid Unit', 
		CASE 
			WHEN ue.VET_IN_SCHOOL_FLAG = 'Y' THEN 'Yes - VET in School'
			ELSE 'No - VET in School'
		END AS 'VET in School Flag', 
		--col.COLLEGE_NAME AS 'Delivery Location - AVETMISS',
		col.LocationName AS 'Delivery Location - AVETMISS',
		ue.CLIENT_ID AS 'Learner Number', 
		ue.SUBJECT_ID AS 'Unit Code', 
		ue.PROGRAM_ID AS 'Course Code', 
		ue.COURSE10 AS 'Course Code - AVETMISS', 
		
		-- Measures
		CASE 
			WHEN ue.COUNTUNIT = 1 THEN 1 
			ELSE NULL 
		END AS UNIT_COUNT, 
		CASE 
			WHEN ue.VALIDUNIT = 1 THEN 1 
			ELSE NULL 
		END AS VALID_UNIT_COUNT, 
		CASE 
			WHEN ue.SCHED_HOURS = 0 THEN NULL 
			ELSE ue.SCHED_HOURS
		END AS SCHED_HOURS_COUNT,
		-- ASH
		CASE 
			WHEN ue.ASH = 0 THEN NULL 
			ELSE ue.ASH
		END AS ASH_COUNT, 
		CASE 
			WHEN ue.ASH71 = 0 THEN NULL 
			ELSE ue.ASH71
		END AS ASH_CONT_COUNT, 
		CASE 
			WHEN ue.FINALASH = 0 THEN NULL 
			ELSE ue.FINALASH
		END AS ASH_FINAL_COUNT, 
		CASE 
			WHEN ue.RPLASH = 0 THEN NULL
			ELSE ue.RPLASH 
		END AS ASH_RPL_COUNT, 
		CASE 
			WHEN ue.TRNGASH = 0 THEN NULL
			ELSE ue.TRNGASH
		END AS ASH_TRAINING_COUNT,
		CASE 
			WHEN ue.SEM1HRS = 0 THEN NULL
			ELSE ue.SEM1HRS
		END AS SEM1HRS_COUNT,
		CASE 
			WHEN ue.SEM2HRS = 0 THEN NULL
			ELSE ue.SEM2HRS
		END AS SEM2HRS_COUNT,
		-- OUTCOMES 
		CASE 
			WHEN ue.OUT70UNIT = 1 THEN 1 
			ELSE NULL
		END AS CONT_UNITS_COUNT, 
		CASE 
			WHEN ue.CTUNIT = 1 THEN 1 
			ELSE NULL 
		END AS CT_UNITS_COUNT, 
		CASE 
			WHEN ue.OUT60UNIT = 1 THEN 1
			ELSE NULL 
		END AS CT_RPL_UNITS_COUNT, 
		CASE 
			WHEN ue.FAILUNIT = 1 THEN 1 
			ELSE NULL 
		END AS FAIL_UNIT_COUNT, 
		CASE 
			WHEN ue.FINALUNIT = 1 THEN 1 
			ELSE NULL 
		END AS FINAL_UNIT_COUNT, 
		CASE 
			WHEN ue.NONASSUNIT = 1 THEN 1 
			ELSE NULL 
		END AS NON_ASS_UNIT_COUNT , 
		CASE 
			WHEN ue.OUT82UNIT = 1 THEN 1 
			ELSE NULL 
		END AS NON_ASS_WDRAW_COUNT, 
		CASE 
			WHEN ue.OUT81UNIT = 1 THEN 1 
			ELSE NULL 
		END AS NON_ASS_S_COMP_COUNT, 
		CASE 
			WHEN ue.PASSUNIT = 1 THEN 1
			ELSE NULL 
		END AS PASS_UNIT_COUNT, 
		CASE
			WHEN ue.OUT51UNIT = 1 THEN 1 
			ELSE NULL
		END AS RPL_GRANT_UNIT_COUNT, 
		CASE 
			WHEN ue.OUT52UNIT = 1 THEN 1 
			ELSE NULL 
		END AS RPL_NOT_GRANT_UNIT_COUNT, 
		CASE 
			WHEN ue.RPLUNIT = 1 THEN 1 
			ELSE NULL 
		END AS RPL_UNIT_COUNT, 
		CASE 
			WHEN ue.OUT61UNIT = 1 THEN 1 
			ELSE NULL 
		END AS SUPERSEDED_UNIT_COUNT, 
		CASE
			WHEN ue.WDRAWUNIT = 1 THEN 1 
			ELSE NULL 
		END AS WDRAW_UNIT_COUNT,
		-- DELIVERY MODES
		CASE
			WHEN ue.ADVD = 1 THEN 1 
			ELSE NULL
		END AS ADVA_COUNT,
		CASE
			WHEN ue.CLAS = 1 THEN 1 
			ELSE NULL
		END AS CLAS_COUNT,
		CASE
			WHEN ue.SELF = 1 THEN 1 
			ELSE NULL
		END AS SELF_COUNT,
		CASE
			WHEN ue.ELEC = 1 THEN 1 
			ELSE NULL
		END AS ELEC_COUNT,
		CASE
			WHEN ue.BLND = 1 THEN 1 
			ELSE NULL
		END AS BLND_COUNT,
		CASE
			WHEN ue.DIST = 1 THEN 1 
			ELSE NULL
		END AS DIST_COUNT,
		CASE
			WHEN ue.ONJB = 1 THEN 1 
			ELSE NULL
		END AS ONJB_COUNT,
		CASE
			WHEN ue.ONJD = 1 THEN 1 
			ELSE NULL
		END AS ONJD_COUNT,
		CASE
			WHEN ue.SIMU = 1 THEN 1 
			ELSE NULL
		END AS SIMU_COUNT,
		CASE
			WHEN ue.MISD = 1 THEN 1 
			ELSE NULL
		END AS MISD_COUNT,
		CASE
			WHEN ue.OTHD = 1 THEN 1 
			ELSE NULL
		END AS OTHD_COUNT,
		CASE
			WHEN ue.DELU = 1 THEN 1 
			ELSE NULL
		END AS DELU_COUNT
	from --[Unit].[UNIT_ENROLMENTS] ue
		edw.Prada_Unit_UNIT_ENROLMENTS_FINALS ue

		--left join COLLEGES col on ue.DELIVERY_LOC = col.LOCATION_CODE
		left join compliance.AvetmissLocation col on ue.DELIVERY_LOC = col.LocationCode 
			and col._RecordCurrent = 1 and col._RecordDeleted = 0
		
		--left join REFERENCE_DATA del_mode on ue.DELIVERY_MODE = del_mode.value and del_mode.DOMAIN = 'DELIVERY MODE AVETMISS'
		left join reference.AvetmissDeliveryMode del_mode on cast(ue.DELIVERY_MODE as varchar(10)) = del_mode.AvetmissDeliveryModeCode
			and del_mode._RecordCurrent = 1 and del_mode._RecordDeleted = 0
		
		--left join REFERENCE_DATA del_tafe on ue.TAFE_DELIVERY_MODE = del_tafe.value and del_tafe.DOMAIN = 'DELIVERY MODE'
		left join reference.DeliveryMode del_tafe on ue.TAFE_DELIVERY_MODE = del_tafe.DeliveryModeCode
			and del_tafe._RecordCurrent = 1 and del_tafe._RecordDeleted = 0

		--left join REFERENCE_DATA fund_avet on ue.FUNDING_NAT = fund_avet.value and fund_avet.DOMAIN = 'FUNDING AVETMISS'
		left join reference.AvetmissFund fund_avet on ue.FUNDING_NAT = fund_avet.AvetmissFundID
			and fund_avet._RecordCurrent = 1 and fund_avet._RecordDeleted = 0

		--left join REFERENCE_DATA fund_tafe on ue.FUNDING_SOURCE = fund_tafe.value and fund_tafe.DOMAIN = 'FUNDING TAFE'
		left join reference.FundingSource fund_tafe on ue.FUNDING_SOURCE = fund_tafe.FundingSourceCode
			and fund_tafe._RecordCurrent = 1 and fund_tafe._RecordDeleted = 0

		--left join REFERENCE_DATA fund_spec on ue.FUNDING_SPECIFIC = fund_spec.value and fund_spec.DOMAIN = 'FUNDING SPECIFIC'
		left join reference.SpecificFunding fund_spec on ue.FUNDING_SPECIFIC = fund_spec.SpecificFundingCodeID
			and fund_spec._RecordCurrent = 1 and fund_spec._RecordDeleted = 0

		--left join REFERENCE_DATA outcome_nat on ue.OUTCOME_NATIONAL = outcome_nat.value and outcome_nat.DOMAIN = 'OUTCOME AVETMISS'
		left join reference.AvetmissOutcome outcome_nat on ue.OUTCOME_NATIONAL = outcome_nat.AvetmissOutcomeID
			and outcome_nat._RecordCurrent = 1 and outcome_nat._RecordDeleted = 0

		--left join REFERENCE_DATA outcome_tafe on ue.OUTCOME_ID_TRAINING = outcome_tafe.value and outcome_tafe.DOMAIN = 'OUTCOME TAFE'
		left join reference.Grades outcome_tafe on ue.OUTCOME_ID_TRAINING = outcome_tafe.GradeCode
			and outcome_tafe._RecordCurrent = 1 and outcome_tafe._RecordDeleted = 0

		--left join REFERENCE_DATA prog_reason on ue.PROGRESS_REASON = prog_reason.value and prog_reason.DOMAIN = 'PROGRESS REASON'
		left join reference.ProgressReason prog_reason on ue.PROGRESS_REASON = prog_reason.ProgressReasonCode
			and prog_reason._RecordCurrent = 1 and prog_reason._RecordDeleted = 0

		--left join REFERENCE_DATA recog on ue.RECOGNITION_TYPE = recog.value and recog.DOMAIN = 'RECOGNITION TYPE'
		left join reference.RecognitionType recog on ue.RECOGNITION_TYPE = recog.RecognitionTypeCode
			and recog._RecordCurrent = 1 and recog._RecordDeleted = 0

		--left join REFERENCE_DATA sponsor on ue.SPONSOR_ORG_CODE = sponsor.VALUE and sponsor.DOMAIN = 'ORGANISATION SPONSOR'
		--the below join needs to be changed to use CIM master entity PartyOrganisation when refactoring it based on CIM
		left join
		(
			select org.ORGANISATION_CODE, org.FES_FULL_NAME as Description
			from edw.OneEBS_EBS_0165_ORGANISATION_UNITS org
			where org.ORGANISATION_TYPE = 'SPONSOR'
			and org._RecordCurrent = 1 and org._RecordDeleted = 0
		) sponsor on sponsor.ORGANISATION_CODE = ue.SPONSOR_ORG_CODE

		--left join REFERENCE_DATA study on ue.STUDY_REASON = study.VALUE and study.DOMAIN = 'STUDY REASON'
		left join reference.StudyReason study on ue.STUDY_REASON = study.StudyReasonID
			and study._RecordCurrent = 1 and study._RecordDeleted = 0

		--left join REFERENCE_DATA comm_prog on ue.COMMEN_PROG = comm_prog.VALUE and comm_prog.DOMAIN = 'COMMENCING PROGRAM'
		--not required