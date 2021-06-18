Create View RevRec.CourseOffering
as
select distinct 
		concat(ce.ReportingYear, ce.InstituteId, co.CourseOfferingId) as 'ID_UIO',
		concat(ce.ReportingYear, ce.InstituteId, co.CourseCode) as 'ID_UI',
--Offering Cost Centre Code
		TeachingSectionCode AS 'Offering Cost Centre Code',
---Enrolment Location
OfferingEnrolmentLocation,
		CASE enrloc.LocationName
			WHEN  'Blue Mountain' THEN 'Blue Mountains'
			WHEN  'B2B / Partner' THEN 'Business to Business/Partnership'
			WHEN  'Coffs Hbr Edc' THEN 'Coffs Harbour Education'
			WHEN  'Flx E Trn Ill' THEN 'Flex E Training'
			WHEN  'Lake Cargelli' THEN 'Lake Cargelligo'
			WHEN  'Lightning Rid' THEN 'Lightning Ridge'
			WHEN  'Macquarie Fie' THEN 'Macquarie Fields'
			WHEN  'Nat Env Ctre' THEN 'National Environmental Centre'
			WHEN  'Nci Open Camp' THEN 'NCI Open Campus'
			WHEN  'New Eng Campu' THEN 'New Eng Campus'
			WHEN  'N Beaches' THEN 'Northern Beaches'
			WHEN 'OTEN' THEN 'OTEN - Distance Education'
			WHEN 'Pt Macquarie' THEN 'Port Macquarie'
			WHEN 'Prim Ind Cent' THEN 'Primary Industries Centre'
			WHEN 'RI Learn Onli' THEN 'Riverina Institute Learn Online'
			WHEN 'Western Conne' THEN 'Western Connect'
			WHEN 'WSI Business' THEN 'Western Sydney Institute Business'
			WHEN 'Wetherill Pk' THEN 'Wetherill Park'
			WHEN 'Wit Access Un' THEN 'WIT Access Unit'
			WHEN 'Wollongong W' THEN 'Wollongong West'
			WHEN 'WSI Internati' THEN 'WSI International'
			WHEN 'Swsi Internat' THEN 'SWSI International'
			else enrloc.LocationName 
		end AS 'Enrolment Location',
		case 
			when left(ce.ReportingYear, 2) = 'CY' then right(ce.ReportingYear, 4) 
			else concat(right(ce.ReportingYear, 4)-1, right(ce.ReportingYear, 2))
		end  as 'EXTRACT_DATE'
	from compliance.AvetmissCourseOffering co
		join compliance.AvetmissCourseEnrolment ce on ce.CourseOfferingId = co.CourseOfferingId
		join edw.reference_avetmiss_reporting_year_cut_off_date cutoff on ce.ReportingYear = cutoff.ReportingYear
															and cutoff._RecordCurrent = 1
															and cutoff._RecordDeleted = 0
		left join compliance.AvetmissLocation enrloc on co.OfferingEnrolmentLocation = enrloc.LocationCode
			and enrloc._RecordCurrent = 1 and enrloc._RecordDeleted = 0
	
	where 1=1
	and cutoff.CubeCutOffReportingDate between co._RecordStart and co._RecordEnd