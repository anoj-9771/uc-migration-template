Create View
[reference].[AvetmissReportingYearCutOffDate]
as
Select cast ('CY2019' as nvarchar(6)) as ReportingYear, Cast ('2020-03-01' as date) as CubeCutOffReportingDate	, Cast ('2020-03-01' as date) as AvetmissCutOffReportingDate
union all 
Select 'CY2020',	'2021-02-28',	'2021-02-28'
union all 
Select 'CY2021',	'2022-02-28',	'2022-02-28'
union all 
Select 'FY2020',	'2020-09-27',	'2020-09-27'
union all 
Select 'FY2021',	'2021-09-26',	'2021-09-26'