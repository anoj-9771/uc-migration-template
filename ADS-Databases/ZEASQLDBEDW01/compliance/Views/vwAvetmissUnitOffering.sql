
CREATE VIEW [compliance].[vwAvetmissUnitOffering]
AS 

WITH EnrolmentDetailFields AS
  (SELECT DISTINCT ReportingYear ,
                   InstituteId ,
                   UnitOfferingId ,
                   AvetmissUnitCode
   FROM compliance.AvetmissUnitEnrolment
   WHERE LEFT(ReportingYear, 2) = 'CY' )
SELECT CAST (concat(ce.ReportingYear , ce.InstituteId , cast((convert(bigint,Uo.UnitOfferingID)) AS varchar(20))) AS VARCHAR) AS 'ID_UUIO' ,
            cast(concat(ce.ReportingYear, ce.InstituteId , ce.AvetmissUnitCode) AS varchar(30)) AS 'ID_UUI' ,
            ce.InstituteId,
            convert(date, OfferingStartDate) AS 'Start Date',
            DAY(convert(date, OfferingStartDate)) AS 'Start Date Day',
            DATENAME(MONTH, convert(date, OfferingStartDate)) AS 'Start Date Month',
            YEAR(convert(date, OfferingStartDate)) AS 'Start Date Year',
            MONTH(convert(date, OfferingStartDate)) AS SORT_SD,
            convert(date, OfferingEndDate) AS 'End Date',
            DAY(convert(date, OfferingEndDate)) AS 'End Date Day',
            DATENAME(MONTH, convert(date, OfferingEndDate)) AS 'End Date Month',
            YEAR(convert(date, OfferingEndDate)) AS 'End Date Year',
            MONTH(convert(date, OfferingEndDate)) AS SORT_ED,
--            AttendanceModeDescription AS 'Attendance Code',
  
			CASE AttendanceModeDescription  
			WHEN 'Full Time Connected Learning' 
			THEN 'Full Time'
			
			WHEN 'Part Time Connected Learning' 
			THEN 'Part Time' 
			
			WHEN 'Part Time Day Connected Learning' 
			THEN 'Part Time Day' 
			
			WHEN 'Part Time Evening Connected Learning' 
			THEN 'Part Time Evening' 
			
			ELSE AttendanceModeDescription  END AS 'Attendance Code', 
  
            CaloccOccurrenceCode AS 'Calocc Code',
           compliance.ProperCase( uo.DeliveryModeDescription ) AS 'Offering Delivery Mode',
            MAXIMUMHOURS AS 'Maximum Hours',
            OFFERINGCODE AS 'Offering Code',
           CASE When OfferingFundingSource = '7201' then '7201 CSO – Youth Engagement Strategy'
                WHEN OfferingFundingSource IS NOT NULL THEN OfferingFundingSource+ ' ' + FundingSource.FundingSourceDescription
                ELSE NULL
            END AS 'Offering Funding Source',
            OfferingCostCentreCode AS 'Offering Cost Centre Code',

            Case 
			when OfferingCostCentre is not null
			 and LEn(OfferingCostCentre) <> 4
			 and SUBSTRING (OfferingCostCentre,4,1) = '-' 
			then TRIM(SUBSTRING (OfferingCostCentre,5,999)) 

			when OfferingCostCentre is not null and OfferingCostCentre not in ('', ' ')
			Then TRIM(OfferingCostCentre)
			Else Null end  AS 'Offering Cost Centre',
			
			--else TRIM(ISNULL(OfferingCostCentre,'')) end  AS 'Offering Cost Centre',
            left(OfferingCostCentreCode, 4) AS 'Offering Teaching Unit',
            OFFERINGSTATUS AS 'Offering Status',
            OfferingType.OfferingTypeDescription AS 'Offering Type',
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
			WHEN 'Online' Then 'Online Campus'
			WHEN 'Clarence CC' Then 'Grafton'
			else enrloc.LocationName 
		end AS 'Offering Enrolment Location',
            OfferingDeliveryLocation AS 'Offering Delivery Location',
            right(ce.ReportingYear, 4) AS 'EXTRACT_DATE',
			NULL INSTITUTE																															/*FIX ME: */


FROM compliance.AvetmissUnitOffering uo--.

JOIN EnrolmentDetailFields ce ON ce.UnitOfferingId = uo.UnitOfferingId
LEFT JOIN compliance.AvetmissLocation enrloc ON uo.OfferingEnrolmentLocation = enrloc.LocationDescription
AND enrloc._RecordCurrent = 1
AND enrloc._RecordDeleted = 0
LEFT JOIN compliance.AvetmissLocation delloc ON uo.OfferingDeliveryLocation = delloc.LocationCode
AND delloc._RecordCurrent = 1
AND delloc._RecordDeleted = 0
LEFT JOIN reference.FundingSource ON FundingSource.FundingSourceCode = uo.OfferingFundingSource
LEFT JOIN reference.OfferingType ON OfferingType.OfferingTypeCode = uo.OfferingType
WHERE 1=1 
  AND left(ce.ReportingYear, 2) = 'CY'
  and uo._RecordCurrent = 1
  and uo._RecordDeleted = 0