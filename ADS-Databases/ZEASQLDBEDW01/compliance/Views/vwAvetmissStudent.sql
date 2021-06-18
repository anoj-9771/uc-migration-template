--/****** Object:  View [compliance].[vwAvetmissStudent]    Script Date: 11/05/2021 1:11:34 PM ******/
--SET ANSI_NULLS ON
--GO

--SET QUOTED_IDENTIFIER ON
--GO







CREATE      view [compliance].[vwAvetmissStudent] as





WIth    disabilityRank as (
   
						   Select null as avetmissDisabilityTypeSK, PersonCode, ReportingYear, disabilitytype,  _DLCuratedZoneTimeStamp, _RecordStart, _RecordEnd, 0 as _RecordDeleted, 1 as _RecordCurrent,
										 RANK() OVER   
											(PARTITION BY PersonCode, ReportingYear ORDER BY _recordStart desc ) AS Rank  
				
									  from [compliance].[AvetmissDisabilityType]
										where ReportingYear = '2020'
				)
				, DisabilityType as (
									Select * from disabilityRank
									where rank = 1
									Union all
									Select avetmissDisabilityTypeSK, PersonCode, ReportingYear, disabilitytype,  _DLCuratedZoneTimeStamp, _RecordStart, _RecordEnd, _RecordDeleted, _RecordCurrent, 1 as rank 
									From [compliance].[AvetmissDisabilityType]
													where ReportingYear <> '2020'
													and _RecordCurrent = 1
													And _RecordDeleted = 0 
													)

SELECT distinct 
		concat(ce.ReportingYear, ce.InstituteId, s.PersonCode) as ID_PC
		,ce.InstituteId as INSTITUTE
		,ce.StudentId as 'Person Code'
		,case
			when s.CompletedQualification = 'Y' then 'Yes - Completed Qual'
			else 'No - Completed Qual'
		 end AS 'Completed Qualification'
		,s.[CountryOfBirthDescription] AS 'Country of Birth'
		,CASE
			WHEN s.DisabilityFlag = 2 
				OR s.DisabilityNeedHelp = 'Yes - Help Dis'
				OR Coalesce (dt.DisabilityType, '') <> '' THEN 'Yes - Count Dis'
			ELSE 'No - Count Dis'
		 END AS 'Disability Count Flag'
		,Coalesce (dt.DisabilityType, 'Not Known') AS 'Disability Type'
		,CASE 
			WHEN s.EmploymentStatus IS NOT NULL THEN CONCAT(s.EmploymentStatus, ' ', s.EmploymentStatusDescription) 
			ELSE NULL 
			END AS 'Employment Status'
		,Coalesce(s.gender, 'Unknown') AS 'Gender'
		,DisabilityNeedHelp as 'Help Disability'
		,CASE
			WHEN EnglishHelpFlag  = 'Y' THEN 'Yes - Help Eng'
			ELSE 'No - Help Eng'
		 END  AS 'Help English'
		,Coalesce(s.HighestSchoolLevel, 'NOT STATED') AS 'Highest School Level'
		,CASE 
			WHEN ATSICodeDescription IS NOT NULL OR ATSICodeDescription <> '' THEN ATSICodeDescription
			ELSE 'No'
			END AS 'Indigenous'
		,CASE 
			WHEN ATSICodeDescription IS NOT NULL OR ATSICodeDescription <> ''
			THEN s.AboriginalGroupDescription ELSE 'NON INDIGENOUS' END as 'Indigenous Group'
		,[LanguageGroupDescription] AS 'Language Group'
		,[LanguageSpokenDescription]  AS 'Language Spoken'
		,s.[RegistrationNo] AS 'Learner Number'
		,[PostalAddressPostcode] AS 'Postcode Postal'
		,[HomePostCode] AS 'Postcode Residential'
		,[ResidentialCountry] AS 'Residential Country'
		,ResidentialRegion AS 'Residential Region'
		,CASE
			WHEN s.VisaClassNo  IS NOT NULL THEN CONCAT(s.VisaClassNo, ' ', s.VisaClassName)
			ELSE NULL 
			END  AS 'Residential Status'
		,StillAttendSchool AS 'Still Attends School'
		,s.[USIProvided]  AS 'USI Provided'
		,s.[YearSchoolCompleted] AS 'Year School Completed'
		,CASE
			WHEN s.VisaType  is not null THEN 'Yes - Visa Provided'
			ELSE 'No - Visa Blank'
			END  AS 'Visa Provided'
		,[VisaSubClass] as 'Visa Sub Class'
		,[VisaSubClassDescription] as 'Visa Sub Class Description'
		,Case When year(VisaExpiryDate) >= RIGHT(ce.ReportingYear,4) then VisaExpiryDate else null end  as VISA_EXPIRY_DATE
		,Case When year(VisaExpiryDate) >= RIGHT(ce.ReportingYear,4) then DAY(s.VisaExpiryDate) else null end AS 'Visa Expiry Date Day'
		,Case When year(VisaExpiryDate) >= RIGHT(ce.ReportingYear,4) then DATENAME(MONTH,s.VisaExpiryDate) else null end AS 'Visa Expiry Date Month'
		,Case When year(VisaExpiryDate) >= RIGHT(ce.ReportingYear,4) then YEAR(s.VisaExpiryDate) else null end AS 'Visa Expiry Date Year'
		,CASE 
			WHEN USIManuallyVerified = 'Y' THEN 'Yes - USI Verified' 
			ELSE 'No - USI Verified'
			END AS 'USI Manually Verified'
		,Case When year(VisaExpiryDate) >= RIGHT(ce.ReportingYear,4) then MONTH(s.VisaExpiryDate) else null end AS SORT_VED
		,DateUSIVerified as DATE_USI_VERIFIED
		,DAY(DateUSIVerified) AS 'Date USI Verified Day'
		,DATENAME(MONTH, DateUSIVerified) AS 'Date USI Verified Month'
		,YEAR(DateUSIVerified) AS 'Date USI Verified Year'
		,MONTH(DateUSIVerified) AS SORT_DUV
		,Coalesce ([PrimaryLearner], s.RegistrationNo, s.personcode) as PrimaryLearner
	from [compliance].[AvetmissStudent] s
	join [compliance].[AvetmissCourseEnrolment] ce on s.PersonCode = ce.StudentId
	join edw.reference_avetmiss_reporting_year_cut_off_date cutoff on ce.ReportingYear = cutoff.ReportingYear
															and cutoff._RecordCurrent = 1
															and cutoff._RecordDeleted = 0
	left join DisabilityType dt on SUBSTRING (ce.reportingYear, 3, 4)	= dt.ReportingYear and ce.StudentId = dt.PersonCode
		and dt._RecordCurrent = 1 and dt._RecordDeleted = 0
	LEFT JOIN [compliance].[AvetmissPrimaryLearner] PL ON PL.PersonCode = ce.StudentId and cutoff.CubeCutOffReportingDate between PL._RecordStart and PL._RecordEnd

	where 1=1
	and cutoff.CubeCutOffReportingDate between s._RecordStart and s._RecordEnd