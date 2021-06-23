

CREATE    view [compliance].[vwAvetmissCurrentCourseMapping]
as
	select distinct
		concat(ce.ReportingYear, ccm.OldCourseCode) AS ID_NCC,
		ccm.CurrentCourseCode AS 'Current Course Code', 
		ccm.CurrentCourseName AS 'Current Course Name',
		CASE 
			WHEN ccm.CurrentCourseOnSL = 'Y' THEN 'Yes'
			WHEN ccm.CurrentCourseOnSL = 'N' THEN 'No'
			ELSE NULL
		END AS 'Current Course on SL',
		ccm.VersionOnSkillsList AS 'Is Superseded', 
		'' AS 'Skills List 2014', 
		CASE
			WHEN ccm.[2015] = 'Y' THEN 'Yes'
			WHEN ccm.[2015] = 'N' THEN 'No'
			ELSE NULL
		END AS 'Skills List 2015', 
		CASE 
			WHEN ccm.[2016] = 'Y' THEN 'Yes'
			WHEN ccm.[2016] = 'N' THEN 'No'
			ELSE NULL
		END AS 'Skills List 2016', 
		CASE 
			WHEN ccm.[2017] = 'Y' THEN 'Yes'
			WHEN ccm.[2017] = 'N' THEN 'No'
			ELSE NULL
		END AS 'Skills List 2017',
		CASE 
			WHEN ccm.[2018] = 'Y' THEN 'Yes'
			WHEN ccm.[2018] = 'N' THEN 'No'
			ELSE NULL
		END AS 'Skills List 2018'
		,CASE 
			WHEN ccm.[2019] = 'Y' THEN 'Yes'
			WHEN ccm.[2019] = 'N' THEN 'No'
			ELSE NULL
		END AS 'Skills List 2019'
		,CASE 
			WHEN ccm.[2020] = 'Y' THEN 'Yes'
			WHEN ccm.[2020] = 'N' THEN 'No'
			ELSE NULL
		END AS 'Skills List 2020'
		,CASE 
			WHEN ccm.[2021] = 'Y' THEN 'Yes'
			WHEN ccm.[2021] = 'N' THEN 'No'
			ELSE NULL
		END AS 'Skills List 2021'
		,CASE 
			WHEN ccm.[2022] = 'Y' THEN 'Yes'
			WHEN ccm.[2022] = 'N' THEN 'No'
			ELSE NULL
		END AS 'Skills List 2022'
		,CASE 
			WHEN ccm.[2023] = 'Y' THEN 'Yes'
			WHEN ccm.[2023] = 'N' THEN 'No'
			ELSE NULL
		END AS 'Skills List 2023'
		,CASE 
			WHEN ccm.[2024] = 'Y' THEN 'Yes'
			WHEN ccm.[2024] = 'N' THEN 'No'
			ELSE NULL
		END AS 'Skills List 2024'
		,CASE 
			WHEN ccm.[2025] = 'Y' THEN 'Yes'
			WHEN ccm.[2025] = 'N' THEN 'No'
			ELSE NULL
		END AS 'Skills List 2025'


	from compliance.AvetmissCurrentCourseMapping ccm

        join [compliance].[AvetmissCourseEnrolment] ce on coalesce(ce.avetmisscoursecode, ce.nationalcoursecode, ce.coursecode) = ccm.oldcoursecode
--		join [compliance].[AvetmissCourseEnrolment] ce on ce.NationalCourseCode = ccm.OldCourseCode
		join edw.reference_avetmiss_reporting_year_cut_off_date cutoff on ce.ReportingYear = cutoff.ReportingYear
															and cutoff._RecordCurrent = 1
															and cutoff._RecordDeleted = 0
	where 1=1
	and cutoff.CubeCutOffReportingDate between ccm._RecordStart and ccm._RecordEnd