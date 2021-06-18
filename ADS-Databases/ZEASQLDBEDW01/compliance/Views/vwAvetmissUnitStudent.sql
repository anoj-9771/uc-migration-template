








CREATE    view [compliance].[vwAvetmissUnitStudent] as
	select Distinct

	 CONCAT(ce.ReportingYear, ce.InstituteId, s.PersonCode) AS	'ID_PC'   
,ce.StudentId as 'Person Code'
,s.age as 'Age'
,CASE s.CompletedQualification WHEN 'Y' then 'Yes - Completed Qual' WHEN 'N' THEN 'No - Completed Qual' ELSE NULL END AS 'Completed Qualification'
,s.[CountryOfBirthDescription] AS 'Country of Birth'
,case when dt.DisabilityType is null then 'Not Known' else dt.DisabilityType end AS 'Disability Type'
,CASE 
WHEN s.DisabilityFlag > 0 THEN 'Yes - Count Dis'
ELSE 'No - Count Dis'
END AS 'Disability Count Flag'
,CASE 
		WHEN s.EmploymentStatus IS NOT NULL THEN  s.EmploymentStatusDescription--CONCAT(s.EmploymentStatus, ' ', s.EmploymentStatusDescription) 
		ELSE NULL 
	 END AS 'Employment Status'
,s.EnglishLevelDescription as 'English Level'
,case 
when Left(s.gender, 1) = 'F' then 'Female'
when Left(s.gender, 1) = 'M' then 'Male'
else 'Unknown' 
end AS 'Gender'
,s.HighestSchoolLevel AS 'Highest School Level'
,s.ATSICodeDescription  as 'Indigenous' 
,s.AboriginalGroupDescription as 'Indigenous Group'
,CASE
	WHEN s.InternationalFlag= 'Y' THEN 'Yes - International'
	WHEN s.InternationalFlag = 'N' THEN 'No - International'
	ELSE null
END as 'International Flag' 
,s.LanguageGroupDescription AS 'Language Group'
,s.LanguageSpokenDescription AS 'Language Spoken'
,s.RegistrationNo AS 'Learner Number'
,s.HomePostcodeName AS 'Postcode Residential'
,s.ResidentialCountry AS 'Residential Country'
,ResidentialRegion AS 'Residential Region'
,s.StillAttendSchool AS 'Still Attends School'
,s.USIProvided  AS 'USI Provided'
,CASE 
	WHEN DateUSIVerified is not null THEN 'Yes - USI Verified' 
	ELSE 'No - USI not Verified'
END as 'USI Verified'
,s.[YearSchoolCompleted] AS 'Year School Completed'
,CASE
WHEN s.VisaClassNo  IS NOT NULL THEN CONCAT(s.VisaClassNo, ' ', s.VisaClassName)
ELSE NULL 
END AS 'Residential Status'
,CASE
WHEN s.VisaType  is not null THEN 'Yes - Visa Provided'
ELSE 'No - Visa Blank'
END  AS 'Visa Provided'
,[VisaSubClass] as 'Visa Sub Class'
,[VisaSubClassDescription] as 'Visa Sub Class Description'
,[VisaExpiryDate] as VISA_EXPIRY_DATE
,DAY(s.VisaExpiryDate) AS 'Visa Expiry Date Day'
,DATENAME(MONTH,s.VisaExpiryDate) AS 'Visa Expiry Date Month'
,YEAR(s.VisaExpiryDate) AS 'Visa Expiry Date Year'
,MONTH(s.VisaExpiryDate) AS SORT_VED
,right(ce.ReportingYear, 4) AS 'EXTRACT DATE'
,NULL as PrimaryLearner

from [compliance].[AvetmissStudent] s
join [compliance].[AvetmissCourseEnrolment] ce on s.PersonCode = ce.StudentId 
left join [compliance].[AvetmissDisabilityType] dt on right(ce.ReportingYear, 4) = dt.ReportingYear and ce.StudentId = dt.PersonCode and dt._RecordCurrent = 1    and dt._RecordDeleted = 0
where 1=1
and left(ce.ReportingYear, 2) = 'CY'
and s._RecordCurrent = 1    
and s._RecordDeleted = 0
and ce._RecordCurrent = 1
and ce._RecordDeleted = 0
-->added this to trouble shoot dups
and right(ce.ReportingYear, 4) = (select DATEPART(year, GETDATE())  )