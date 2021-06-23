



CREATE   view [compliance].[vwAvetmissLocation] 
as
	SELECT 
		loc.LocationCode as LOCATION_CODE, 
		right(loc.InstituteID, 3) as 'RTO Code',
		case 
			when loc.LocationName = 'B2B / Partner' then 'Business to Business/Partnership' 
			when loc.LocationName = 'N Beaches' then 'Northern Beaches' 
			when loc.LocationName = 'Prim Ind Cent' then 'Primary Industries Centre' 
			when loc.LocationName = 'RI Learn Onli' then 'Riverina Institute Learn Online' 
			when loc.LocationName = 'Pt Macquarie' then 'Port Macquarie' 
			when loc.LocationName = 'Wollongong W' then 'Wollongong West' 
			when loc.LocationName = 'WSI Business' then 'Western Sydney Institute Business' 
			when loc.LocationName = 'Nat Env Ctre' then 'National Environmental Centre' 
			when loc.LocationName = 'Swsi Internat' then 'SWSI International' 
			when loc.LocationName = 'RIL' then 'Connected Learning Classroom' 
			when loc.LocationName = 'WSI Internati' then 'WSI International' 
			when loc.LocationName = 'Wit Access Un' then 'WIT Access Unit' 
			when loc.LocationName = 'Wetherill Pk' then 'Wetherill Park' 
			when loc.LocationName = 'OTEN' then 'OTEN - Distance Education' 
			when loc.LocationName = 'Online' then 'Online Campus' 
			when loc.LocationName = 'Nci Open Camp' then 'NCI Open Campus' 
			when loc.LocationName = 'Macquarie Fie' then 'Macquarie Fields' 
			when loc.LocationName = 'Lightning Rid' then 'Lightning Ridge' 
			when loc.LocationName = 'Lake Cargelli' then 'Lake Cargelligo' 
			when loc.LocationName = 'Flx E Trn Ill' then 'Flex E Training' 
			when loc.LocationName = 'Coffs Hbr Edc' then 'Coffs Harbour Education' 
			when loc.LocationName = 'Clarence CC' then 'Clarence Correctional Centre' 
			when loc.LocationName = 'YAMBA' then 'Yamba' 
			when loc.LocationName = 'Blue Mountain' then 'Blue Mountains' 
			when loc.LocationName = 'New Eng Campu' then 'New Eng Campus' 
			when loc.LocationName = 'Western Conne' then 'Western Connect' 
			else loc.LocationName 
		end as 'Delivery Location',

--		loc.PostCode as 'Delivery Region',
		Case when loc.LocationCode = 'CCC' then '2460' else coalesce(loc.PostCode ,LocationPostCode) end as 'Delivery Region',

		CASE WHEN loc.InstituteName = 'Open Training & Education Nwork' THEN 'OTEN'
		ELSE loc.InstituteName 
		END as 'RTO Name',
		CASE
			WHEN loc.LocationCode IN ('OTE') THEN loc.RegionName 
			ELSE loc.RegionName + ' Region' 
		END as 'Region',
		CASE 
			WHEN loc.RegionName = 'North' THEN 1
			WHEN loc.RegionName = 'South' THEN 2
			WHEN loc.RegionName = 'Sydney' THEN 3
			WHEN loc.RegionName = 'West' THEN 4
			WHEN loc.RegionName = 'Western Sydney' THEN 5
			WHEN loc.LocationCode IN ('OTE') THEN 6 
			ELSE 9
		END AS REGION_ORDER,
		cast(inst.RTOCode as varchar(25)) as 'RTO Number',
		cast(inst.RTOCode as varchar(25)) as 'Insitute Code',
		loc.LocationCode as 'Location Code'
	FROM compliance.AvetmissLocation loc
		left join reference.Institute inst on right(loc.InstituteID, 3) = cast(inst.InstituteID as varchar(3))
			and inst._RecordCurrent = 1 and inst._RecordDeleted = 0
	WHERE 1=1
	and loc.Active = 'Y'
	and loc._RecordCurrent = 1 and loc._RecordDeleted = 0

	UNION

	SELECT 
		dlc.DELIVERY_LOC_ONLINE as LOCATION_CODE, 
		right(loc.InstituteID, 3) as 'RTO Code',
		Case 
			when loc.LocationName = 'B2B / Partner' then 'Business to Business/Partnership' 
			when loc.LocationName = 'N Beaches' then 'Northern Beaches' 
			when loc.LocationName = 'Prim Ind Cent' then 'Primary Industries Centre' 
			when loc.LocationName = 'RI Learn Onli' then 'Riverina Institute Learn Online' 
			when loc.LocationName = 'Pt Macquarie' then 'Port Macquarie' 
			when loc.LocationName = 'Wollongong W' then 'Wollongong West' 
			when loc.LocationName = 'WSI Business' then 'Western Sydney Institute Business' 
			when loc.LocationName = 'Nat Env Ctre' then 'National Environmental Centre' 
			when loc.LocationName = 'Swsi Internat' then 'SWSI International' 
			when loc.LocationName = 'RIL' then 'Connected Learning Classroom' 
			when loc.LocationName = 'WSI Internati' then 'WSI International' 
			when loc.LocationName = 'Wit Access Un' then 'WIT Access Unit' 
			when loc.LocationName = 'Wetherill Pk' then 'Wetherill Park' 
			when loc.LocationName = 'OTEN' then 'OTEN - Distance Education' 
			when loc.LocationName = 'Online' then 'Online Campus' 
			when loc.LocationName = 'Nci Open Camp' then 'NCI Open Campus' 
			when loc.LocationName = 'Macquarie Fie' then 'Macquarie Fields' 
			when loc.LocationName = 'Lightning Rid' then 'Lightning Ridge' 
			when loc.LocationName = 'Lake Cargelli' then 'Lake Cargelligo' 
			when loc.LocationName = 'Flx E Trn Ill' then 'Flex E Training' 
			when loc.LocationName = 'Coffs Hbr Edc' then 'Coffs Harbour Education' 
			when loc.LocationName = 'Clarence CC' then 'Clarence Correctional Centre' 
			when loc.LocationName = 'YAMBA' then 'Yamba' 
			when loc.LocationName = 'Blue Mountain' then 'Blue Mountains' 
			when loc.LocationName = 'New Eng Campu' then 'New Eng Campus' 
			when loc.LocationName = 'Western Conne' then 'Western Connect' 
			else loc.LocationName 
		end as 'Delivery Location',
--		loc.PostCode as 'Delivery Region',
		Case when loc.LocationCode = 'CCC' then '2460' else coalesce(loc.PostCode ,LocationPostCode) end as 'Delivery Region',
		loc.InstituteName as 'RTO Name',
		CASE
			WHEN loc.LocationCode IN ('OTE') THEN loc.RegionName 
			ELSE 'TAFE Digital'  --Updated case
		END as 'Region',

		6 as REGION_ORDER,
		cast(inst.RTOCode as varchar(25)) as 'RTO Number',
		cast(inst.RTOCode as varchar(25)) as 'Insitute Code',
		loc.LocationCode as 'Location Code'
	FROM compliance.AvetmissLocation loc
		inner join 
		(
			select distinct 
				DeliveryLocationCode, 
				concat(DeliveryLocationCode, 
										case 
											when TOL = 1 then '-TOL'
											when TNW = 1 then '-TNW'
											when [OPEN] = 1 then '-OPEN'
											else NULL
										end) as DELIVERY_LOC_ONLINE
			from compliance.AvetmissCourseEnrolment 
			where left(ReportingYear, 2) = 'CY'
				and cast(right(ReportingYear, 4) as int) >= 2020
				and TafeDigital = 1 
				and OTEN = 0
				and _RecordCurrent = 1 and _RecordDeleted = 0

			union

			Select 'ALB', 'ALB-TOL' UNION 
			Select 'ARM', 'ARM-TOL' UNION 
			Select 'B2B', 'B2B-OPEN' UNION 
			Select 'B2B', 'B2B-TNW' UNION 
			Select 'BAL', 'BAL-TNW' UNION 
			Select 'BKS', 'BKS-TOL' UNION 
			Select 'BLT', 'BLT-TOL' UNION 
			Select 'BMT', 'BMT-TOL' UNION 
			Select 'CAS', 'CAS-TNW' UNION 
			Select 'CBT', 'CBT-TOL' UNION 
			Select 'CHE', 'CHE-TNW' UNION 
			Select 'CMA', 'CMA-TOL' UNION 
			Select 'COF', 'COF-TNW' UNION 
			Select 'COT', 'COT-TOL' UNION 
			Select 'CWR', 'CWR-TOL' UNION 
			Select 'GLA', 'GLA-TNW' UNION 
			Select 'GLB', 'GLB-TOL' UNION 
			Select 'GLE', 'GLE-TOL' UNION 
			Select 'GLI', 'GLI-TOL' UNION 
			Select 'GRA', 'GRA-TNW' UNION 
			Select 'GRA', 'GRA-TOL' UNION 
			Select 'GRI', 'GRI-TOL' UNION 
			Select 'HAM', 'HAM-TOL' UNION 
			Select 'HIL', 'HIL-TOL' UNION 
			Select 'IOL', 'IOL-TOL' UNION 
			Select 'KCL', 'KCL-OPEN' UNION 
			Select 'KCL', 'KCL-TNW' UNION 
			Select 'KEM', 'KEM-TNW' UNION 
			Select 'KUR', 'KUR-TOL' UNION 
			Select 'LIS', 'LIS-TNW' UNION 
			Select 'LVP', 'LVP-TOL' UNION 
			Select 'MAK', 'MAK-TNW' UNION 
			Select 'MFD', 'MFD-TOL' UNION 
			Select 'MIL', 'MIL-TOL' UNION 
			Select 'MRY', 'MRY-TOL' UNION 
			Select 'MUW', 'MUW-TNW' UNION 
			Select 'NCO', 'NCO-OPEN' UNION 
			Select 'NCO', 'NCO-TNW' UNION 
			Select 'NCO', 'NCO-TOL' UNION 
			Select 'NEP', 'NEP-TOL' UNION 
			Select 'NEW', 'NEW-TOL' UNION 
			Select 'NIR', 'NIR-TOL' UNION 
			Select 'NWR', 'NWR-TOL' UNION 
			Select 'OUR', 'OUR-TOL' UNION 
			Select 'PMQ', 'PMQ-TNW' UNION 
			Select 'PMQ', 'PMQ-TOL' UNION 
			Select 'PTH', 'PTH-TOL' UNION 
			Select 'RIO', 'RIO-TOL' UNION 
			Select 'SCO', 'SCO-TOL' UNION 
			Select 'SGR', 'SGR-TOL' UNION 
			Select 'SUT', 'SUT-TOL' UNION 
			Select 'TAR', 'TAR-TNW' UNION 
			Select 'ULT', 'ULT-TOL' UNION 
			Select 'WAL', 'WAL-TOL' UNION 
			Select 'WCH', 'WCH-TNW' UNION 
			Select 'WCN', 'WCN-TOL' UNION 
			Select 'WLB', 'WLB-TNW' UNION 
			Select 'WLG', 'WLG-TOL' UNION 
			Select 'WWG', 'WWG-TOL' UNION 
			Select 'WWY', 'WWY-TOL'  
 
		) dlc on dlc.DeliveryLocationCode = loc.LocationCode and dlc.DELIVERY_LOC_ONLINE is not null
		left join reference.Institute inst on right(loc.InstituteID, 3) = cast(inst.InstituteID as varchar(3))
			and inst._RecordCurrent = 1 and inst._RecordDeleted = 0
	WHERE 1=1
	and loc.Active = 'Y'
	and loc._RecordCurrent = 1 and loc._RecordDeleted = 0