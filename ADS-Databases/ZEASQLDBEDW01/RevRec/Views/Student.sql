Create View RevRec.Student
as
Select 
distinct 
--Join Key 
		concat(ce.ReportingYear, ce.InstituteId, s.PersonCode) as ID_PC		
--Learner Number
		,s.[RegistrationNo] AS 'Learner Number'
--Person Code
		,ce.StudentId as 'Person Code'
--Disability Type
		,Coalesce (dt.DisabilityType, 'Not Known') AS 'Disability Type'
--Employment Status
		,CASE 
			WHEN s.EmploymentStatus IS NOT NULL THEN CONCAT(s.EmploymentStatus, ' ', s.EmploymentStatusDescription) 
			ELSE NULL 
			END AS 'Employment Status'
--Indigenous Group
		,CASE 
			WHEN ATSICodeDescription IS NOT NULL OR ATSICodeDescription <> ''
			THEN s.AboriginalGroupDescription ELSE 'NON INDIGENOUS' END as 'Indigenous Group'
--Language Group
		,[LanguageGroupDescription] AS 'Language Group'

--Postcode Residential
		,[HomePostCode] AS 'Postcode Residential'
--Residential Status
		,CASE
			WHEN s.VisaClassNo  IS NOT NULL THEN CONCAT(s.VisaClassNo, ' ', s.VisaClassName)
			ELSE NULL 
			END  AS 'Residential Status'



	from [compliance].[AvetmissStudent] s
	join [compliance].[AvetmissCourseEnrolment] ce on s.PersonCode = ce.StudentId
	join edw.reference_avetmiss_reporting_year_cut_off_date cutoff on ce.ReportingYear = cutoff.ReportingYear
															and cutoff._RecordCurrent = 1
															and cutoff._RecordDeleted = 0
	left join [compliance].[AvetmissDisabilityType] dt on year(ce.ReportingDate) = dt.ReportingYear and ce.StudentId = dt.PersonCode
		and dt._RecordCurrent = 1 and dt._RecordDeleted = 0

	where 1=1
	and cutoff.CubeCutOffReportingDate between s._RecordStart and s._RecordEnd