

CREATE    view [compliance].[vwAvetmissCourse] as

	select distinct 
		concat(ce.ReportingYear, ce.InstituteId, ac.CourseCode) as 'ID_UI',
		concat(ce.ReportingYear, ac.AvetmissCourseCode) as 'ID_CSE',
		ce.InstituteId as 'INSTITUTE',
		case 
			when left(ce.ReportingYear, 2) = 'CY' then right(ce.ReportingYear, 4) 
			else concat(right(ce.ReportingYear, 4)-1, right(ce.ReportingYear, 2))
		end  as 'EXTRACT_DATE',
		ac.AnimalUseName AS 'Animal Use',
		CASE 
			WHEN ac.IndustryCode IS NOT NULL THEN CONCAT(ac.IndustryCode, ' ', ac.IndustryName)
			ELSE NULL
		END AS ANZSIC,
		CASE 
			WHEN ac.OccupationCode IS NOT NULL THEN CONCAT(ac.OccupationCode, ' ', ac.OccupationName)
			ELSE NULL
		END AS ANZSCO,
		ac.QualificationTypeName AS 'Award Category',
		CASE 
			WHEN ac.FieldOfEducationId IS NOT NULL THEN CONCAT(ac.FieldOfEducationId, ' ', ac.FieldOfEducationName)
			ELSE NULL
		END AS 'Field of Education',
		ac.IsFEEHelp AS 'Is FEE HELP',
		ac.IsVETFEEHelp AS 'Is VET-FEE HELP',
		ac.IsVocational AS 'Is Vocational',
		ac.NationalCourseCode AS 'National Course Code', 
		ac.NominalHours AS 'Nominal Hours',
		ac.CourseCode AS 'Product Code', 
		ac.CourseName AS 'Product Name', 
		ac.CourseStatus AS 'Product Status', 
		CASE 
			WHEN ac.ResourceAllocationModelCategoryCode IS NOT NULL THEN CONCAT(ac.ResourceAllocationModelCategoryCode, ' ', ac.ResourceAllocationModelCategoryName)
			ELSE NULL
		END AS RAM,
		left(ac.TrainingPackageCode, 5) AS 'Training Package Code', /*PRADA-1658*/
		ac.TrainingPackageName AS 'Training Package Name',
		ac.TrainingPackage3Digit AS 'Training Package 3-Digit',
		ac.TrainingPackage4Digit AS 'Training Package 4-Digit',
		ac.TrainingPackage6Digit AS 'Training Package 6-Digit',
		ac.CourseCategoryName AS 'Unit Category', 
		ac.RecommendedUsage AS 'Recommended Usage',
		ac.ScopeApproved AS 'Scope Approved'
	from compliance.AvetmissCourse ac
		join [compliance].[AvetmissCourseEnrolment] ce on ce.AvetmissCourseCode = ac.AvetmissCourseCode
		join edw.reference_avetmiss_reporting_year_cut_off_date cutoff on ce.ReportingYear = cutoff.ReportingYear 
															and cutoff._RecordCurrent = 1
															and cutoff._RecordDeleted = 0
	where 1=1
	and cutoff.CubeCutOffReportingDate between ac._RecordStart and ac._RecordEnd