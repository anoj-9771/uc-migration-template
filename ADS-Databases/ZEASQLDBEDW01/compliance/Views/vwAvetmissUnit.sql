



CREATE VIEW [compliance].[vwAvetmissUnit]--[vwAvetmissUnit]  --[VWUNIT_UNITS] 
AS 


WITH EnrolmentDetailFields AS
  (SELECT  DISTINCT 
			ReportingYear
		   ,InstituteId
		  , AvetmissUnitCode
   FROM [compliance].[AvetmissUnitEnrolment]
   WHERE AvetmissUnitEnrolment._RecordCurrent = 1 
  and AvetmissUnitEnrolment._RecordDeleted = 0
   and  LEFT(ReportingYear, 2) = 'CY' 
   ),

   UnitRank as (Select 
				 RANK() OVER   
					(PARTITION BY ac.UnitCode ORDER BY AC.unitStatus ) AS Rank  
				,unitcode, unitStatus
				from [compliance].[AvetmissUnit] ac
				where _RecordCurrent = 1
				and _RecordDeleted = 0
				)
--,Res as (
SELECT DISTINCT cast(concat(ce.ReportingYear , ce.InstituteId , ce.AvetmissUnitCode) AS varchar(30)) AS 'ID_UUI' ,
				cast(concat(ce.ReportingYear , ce.AvetmissUnitCode) AS varchar(30)) AS 'ID_UNIT',
                ce.InstituteId AS INSTITUTE,
                ac.AnimalUseName AS 'Animal Use',
                CASE
                    WHEN ac.OccupationCode IS NOT NULL THEN CONCAT(ac.OccupationCode, ' ', ac.OccupationName)
                    ELSE NULL
                END AS 'ANZSCO',
                CASE
                    WHEN [IndustryCode] IS NOT NULL THEN CONCAT([IndustryCode], ' ', [IndustryName])
                    ELSE NULL
                END AS 'ANZSIC',
                CASE
                    WHEN ac.FieldOfEducationId IS NOT NULL THEN CONCAT(ac.FieldOfEducationId, ' ', ac.FieldOfEducationName)
                    ELSE NULL
                END AS 'Field of Education',
                ac.IsVocational AS 'Is Vocational',
                CASE
                    WHEN ac.ScopeApproved = 'Y' THEN 'Yes - Scope Approved'
                    WHEN ac.ScopeApproved = 'N' THEN 'No - Scope Approved'
                    ELSE NULL
                END AS 'Scope Approved',
                ac.[NationalCourseCode] AS 'National Course Code',
                ac.[UnitStatus] AS 'Product Status',
                NULL AS 'Program Area' --This is now out of scope See PRADA-1012 for confirmation
,
                CASE
                    WHEN ac.ResourceAllocationModelCategoryCode IS NOT NULL THEN CONCAT(ac.ResourceAllocationModelCategoryCode, ' ', ac.ResourceAllocationModelCategoryName)
                    ELSE NULL
                END AS RAM,
  Coalesce(Replace (ESCNM.ESCNM_Description, '?', ' '), '_UNKNOWN') as Sponsor   --           [Sponsor] AS 'Sponsor' --PRADA-1014 created as a missing field
,
                [TGAUnitName] AS 'TGA Unit Name' --PRADA-1015 bug raised as all records are null
,
                UnitCategory AS 'Unit Category' ,
                UPPER([RecommendedUsage]) AS 'Recommended Usage',
                ac.[UnitCode] AS 'Unit Code',
                [UnitName] AS 'Unit Name',

			Right(ce.ReportingYear,4)  AS 'EXTRACT_DATE'


FROM [compliance].[AvetmissUnit] ac
join UnitRank ur on ur.UnitCode = ac.UnitCode and ur.Rank = 1 and ac.UnitStatus = ur.UnitStatus
JOIN EnrolmentDetailFields ce ON ce.AvetmissUnitCode = ac.UnitCode --and 
left join  [reference].[CommencingProgramIdentifierunitesc] unitesc on unitesc.UNITesc = ac.NationalCourseCode and unitesc._RecordCurrent = 1 and unitesc._RecordDeleted = 0
left join  [reference].[CommencingProgramIdentifierescnm]  escnm on escnm.ESCNM = UNITesc.ESCNM and escnm._RecordCurrent = 1 and escnm._RecordDeleted = 0


WHERE LEFT(ce.ReportingYear, 2) = 'CY' 
and ac._RecordCurrent = 1 
and ac._RecordDeleted = 0
and ((cast(concat(right(ce.ReportingYear, 2) , ce.InstituteId , ce.AvetmissUnitCode) AS varchar(30)) in ('20127NSWTPLG107A', '20165CUAVSS301') and ac.[UnitStatus] = 'CURRENT') or cast(concat(right(ce.ReportingYear, 2) , ce.InstituteId , ce.AvetmissUnitCode) AS varchar(30)) not in ('20127NSWTPLG107A', '20165CUAVSS301'))
and not (cast(concat(right(ce.ReportingYear, 2) , ce.InstituteId , ce.AvetmissUnitCode) AS varchar(30))  in ( '20357NSWTPLG107A','20167CUAVSS301', '20165NSWTPLG107A') and ac.UnitStatus = 'OBSOLETE')