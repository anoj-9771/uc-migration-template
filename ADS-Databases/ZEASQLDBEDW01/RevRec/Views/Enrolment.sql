Create View RevRec.Enrolment
as
	select 
		ce.ID_PC, 
		ce.ID_UIO,
		ce.ID_CPU, 
		ce.ID_NCC,
		rp.ReportingPeriodKey AS REPORT_DATE, 
		convert(varchar(10), ce.ReportingDate, 112) AS EXTRACT_DATE,
		right(rp.SpecialReportingYear,4) AS ENROLMENT_YEAR,

--Total AVETMISS Count
		CASE
			WHEN ce.AVETCOUNT = 1 THEN 1
			WHEN ce.AVETCOUNT is null AND ce.ENRFLAG = 1 THEN 1
			ELSE NULL
		END as AVETMISS_COUNT,
--Calocc Code
		ce.CourseCaloccCode as 'Calocc Code', 
--Commencement Flag
		CASE 
			WHEN ce.CommencementFlag = 'Commencing' THEN 'Commencing'
			ELSE 'Continuing'
		END as 'Commencement Flag', 
--Course Code
		ce.AvetmissCourseCode as 'Course Code',
--Course Name
		CASE
			WHEN ISNULL(ce.CSENM, '') = '' then 'MISSING COURSE CODE'
			else UPPER(ce.CSENM)
		END as 'Course Name',
--End Date
		ce.CourseEnrolmentEndDate as 'End Date',
--Funding Source - TAFE
		CASE 
			WHEN ce.CourseFundingSourceCode IS NOT NULL THEN CONCAT(ce.CourseFundingSourceCode, ' ', fund_tafe.FundingSourceDescription)
			ELSE NULL
		END AS 'Funding Source - TAFE',
--Sponsor Code
		ce.SponsorCode AS 'Sponsor Code',
--Start Date
		ce.CourseEnrolmentStartDate as 'Start Date',
--Waiver Codes
	ce.WaiverCodes AS 'Waiver Codes',
--Who To Pay
		CASE ce.WhoToPay 
		WHEN 'CONTRACT' THEN 'Sponsor Commercial'
		WHEN 'LEARNER' THEN 'Learner to Pay'
		WHEN 'MKTINGPTNR' THEN 'Sponsor Marketing'
		WHEN 'TPPSAP' THEN 'Sponsor (TPP) to pay'
		ELSE  ce.WhoToPay 
			END AS 'Who To Pay',
--Total Enrolment Count
		1 as ENROLMENT_COUNT, 
--Commencement Year
		ce.CommencementYear as 'Commencement Year', 
--Completion Flag
		CASE
			WHEN ce.GRAD = 1 THEN 'Yes - Completed'
			ELSE 'No - Not Completed'
		END as 'Completion Flag', 
--Field of Education
		CASE 
			WHEN ce.FOE4 IS NOT NULL THEN CONCAT(ce.FOE4, ' ', FOE.FieldOfEducationDescription)
			ELSE NULL
		END AS 'Field of Education',
--Funding Source - AVETMISS
		CASE 
			WHEN ce.AVFUND IS NOT NULL OR ce.AVFUND <> 0 THEN CONCAT(cast(ce.AVFUND as varchar(10)), ' ', fund_avet.AvetmissFundName)
			ELSE NULL
		END AS 'Funding Source - AVETMISS',
--Higher Education
		ce.HIGHED AS 'Higher Education', 
--Include in AVETMISS Flag
		CASE
			WHEN ce.AvetmissFlag = 'Y' THEN 'Include'
			ELSE 'Exlude'
		END as 'Include in AVETMISS Flag',
--Progress Code
		ce.CourseProgressCode AS 'Progress Code',
--Progress Date
		ce.CourseProgressDate AS 'Progress Date',
--Progress Reason
		pr.ProgressReasonDescription AS 'Progress Reason',
--Progress Status
		ce.CourseProgressStatus AS 'Progress Status',
--Transition Flag
		ce.TransitionFlag as 'Transition Flag',

		CASE 
			WHEN ce.TafeDigital = 1 AND (ce.OTEN = 0 or ce.OTEN is null) AND ce.TOL = 1
				THEN ce.DeliveryLocationCode + '-TOL'
			WHEN ce.TafeDigital = 1 AND (ce.OTEN = 0 or ce.OTEN is null) AND ce.TNW = 1
				THEN ce.DeliveryLocationCode + '-TNW'
			WHEN ce.TafeDigital = 1 AND (ce.OTEN = 0 or ce.OTEN is null) AND ce.[OPEN] = 1
				THEN ce.DeliveryLocationCode + '-OPEN'
			ELSE ce.DeliveryLocationCode
		END AS 'Delivery Location', 
		CASE
			WHEN ce.ValidFlag = 1 THEN 'Valid'
			ELSE 'Invalid'
		END as 'Count Valid Enrolments'


	
	from compliance.AvetmissCourseEnrolment ce  
		join compliance.AvetmissReportingPeriod  rp on ce.ReportingYear = rp.ReportingYear and ce.ReportingDate = rp.ReportingDate


		left join reference.FieldOfEducation foe on ce.FOE4 = foe.FieldOfEducationID
			and foe._RecordCurrent = 1 and foe._RecordDeleted = 0

		left join reference.AvetmissFund fund_avet on ce.AVFUND = fund_avet.AvetmissFundID
			and fund_avet._RecordCurrent = 1 and fund_avet._RecordDeleted = 0

		left join reference.FundingSource fund_tafe on ce.CourseFundingSourceCode = fund_tafe.FundingSourceCode
			and fund_tafe._RecordCurrent = 1 and fund_tafe._RecordDeleted = 0

		Left Join [reference].[ProgressReason] PR on  pr.ProgressReasonCode = ce.CourseProgressReason
			and PR._RecordCurrent = 1 and PR._RecordDeleted = 0

	where ce._RecordCurrent = 1
	and ce._RecordDeleted = 0