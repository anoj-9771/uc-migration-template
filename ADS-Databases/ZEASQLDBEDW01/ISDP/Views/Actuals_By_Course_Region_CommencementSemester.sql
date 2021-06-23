

CREATE     view [ISDP].[Actuals_By_Course_Region_CommencementSemester]
as

With 
cte_ReportingDate as
					(
					select reportingyear, max (reportingdate) as reportingdate
					from [avetmiss].[courseenrolment] 
					where left (reportingyear,1) = 'C'
					and Reportingdate <> '20210307'
					group by reportingyear
					)

,Cte_Union as (
Select  
		
		 COALESCE(ce.NationalCourseCode, ce.AvetmissCourseCode) as CourseCode
		 ,ce.Region
		 ,co.OfferingEnrolmentLocation
		,0 as 'Actuals_2019_S1_Commencements'
		,0 as 'Actuals_2019_S2_Commencements'
		,SUM(Case when ce.ReportingYear = 'CY2020' and ce.CommencementYear = 2020 and  Right (ce.CommencementSemester,1) = 1 then ce.avetcount else 0 end) as 'Actuals_2020_S1_Commencements'
		,SUM(Case when ce.ReportingYear = 'CY2020' and ce.CommencementYear = 2020 and  Right (ce.CommencementSemester,1) = 2 then ce.avetcount else 0 end) as 'Actuals_2020_S2_Commencements'
		,SUM(Case when ce.ReportingYear = 'CY2021' and ce.CommencementYear = 2021 and  Right (ce.CommencementSemester,1) = 1 then ce.avetcount else 0 end) as 'Actuals_2021_S1_Commencements'
		,SUM(Case when ce.ReportingYear = 'CY2021' and ce.CommencementYear = 2021 and  Right (ce.CommencementSemester,1) = 2 then ce.avetcount else 0 end) as 'Actuals_2021_S2_Commencements'
		,SUM(Case when ce.ReportingYear = 'CY2022' and ce.CommencementYear = 2022 and  Right (ce.CommencementSemester,1) = 1 then ce.avetcount else 0 end) as 'Actuals_2022_S1_Commencements'
		,SUM(Case when ce.ReportingYear = 'CY2022' and ce.CommencementYear = 2022 and  Right (ce.CommencementSemester,1) = 2 then ce.avetcount else 0 end) as 'Actuals_2022_S2_Commencements'

from cte_ReportingDate
INNER JOIN  compliance.AvetmissCourseEnrolment ce on cte_ReportingDate.ReportingYear = ce.reportingYear and cte_ReportingDate.ReportingDate = ce.ReportingDate 
inner join Compliance.AvetmissCourseOffering co on ce.CourseOfferingId = co.CourseOfferingID
where ce.courseofferingid is not null
and ce.CommencementYear >= 2020
and ce._RecordCurrent = 1
and ce._RecordDeleted = 0
and co._RecordCurrent = 1
and co._RecordDeleted = 0
GROUP BY 
		  ce.reportingdate 
		 ,COALESCE(ce.NationalCourseCode, ce.AvetmissCourseCode)  
		 ,ce.Region 
		 ,co.OfferingEnrolmentLocation
Union all 
Select *,0,0,0,0,0,0 from [tsPlanning].[ISDP_2019_final_Actual_Commencements_With_Enrolment_Location] b --[ISDP].[2019_final_Actual_Commencements] b 
		 )

Select CourseCode
, Region
, OfferingEnrolmentLocation
, b.LocationName as OfferingEnrolmentLocationName
, b.LocationDescription as OfferingEnrolmentLocationDescription
, Sum(Actuals_2019_S1_Commencements) as Actuals_2019_S1_Commencements 
, Sum(Actuals_2019_S2_Commencements) as Actuals_2019_S2_Commencements 
, Sum(Actuals_2020_S1_Commencements) as Actuals_2020_S1_Commencements 
, Sum(Actuals_2020_S2_Commencements) as Actuals_2020_S2_Commencements 
, Sum(Actuals_2021_S1_Commencements) as Actuals_2021_S1_Commencements 
, Sum(Actuals_2021_S2_Commencements) as Actuals_2021_S2_Commencements 
, Sum(Actuals_2022_S1_Commencements) as Actuals_2022_S1_Commencements 
, Sum(Actuals_2022_S2_Commencements) as Actuals_2022_S2_Commencements 
from Cte_Union a
join  [compliance].[AvetmissLocation] b on a.OfferingEnrolmentLocation = b.LocationCode
where b._RecordCurrent = 1
and b._RecordDeleted = 0
group by CourseCode, Region,  OfferingEnrolmentLocation, b.LocationName, b.LocationDescription