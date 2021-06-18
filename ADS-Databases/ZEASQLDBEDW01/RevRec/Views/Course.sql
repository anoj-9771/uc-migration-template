Create View RevRec.Course
as
select distinct 
		concat(ce.ReportingYear, ce.InstituteId, ac.CourseCode) as 'ID_UI',
		concat(ce.ReportingYear, ac.AvetmissCourseCode) as 'ID_CSE',
			case 
			when left(ce.ReportingYear, 2) = 'CY' then right(ce.ReportingYear, 4) 
			else concat(right(ce.ReportingYear, 4)-1, right(ce.ReportingYear, 2))
		end  as 'EXTRACT_DATE',
--Product Code
		ac.CourseCode AS 'Product Code', 
		ac.CourseName AS 'Product Name', 
--Training Package 3-Digit
--Training Package 6-Digit
		ac.TrainingPackage3Digit AS 'Training Package 3-Digit',
		ac.TrainingPackage4Digit AS 'Training Package 4-Digit',
		ac.TrainingPackage6Digit AS 'Training Package 6-Digit',
--National Course Code
		ac.NationalCourseCode AS 'National Course Code'


	from compliance.AvetmissCourse ac
		join [compliance].[AvetmissCourseEnrolment] ce on ce.AvetmissCourseCode = ac.AvetmissCourseCode
		join edw.reference_avetmiss_reporting_year_cut_off_date cutoff on ce.ReportingYear = cutoff.ReportingYear 
															and cutoff._RecordCurrent = 1
															and cutoff._RecordDeleted = 0
	where 1=1
	and cutoff.CubeCutOffReportingDate between ac._RecordStart and ac._RecordEnd