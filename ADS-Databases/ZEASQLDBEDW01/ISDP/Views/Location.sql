


CREATE View [ISDP].[Location]  
AS

SELECT  [AvetmissLocationSK], [LocationID], [LocationCode]

,CASE  [LocationName]

When 'Coffs Hbr Edc' Then 'Coffs Harbour Education'
--When 'Murwillumbah' Then 'Murwillumbah CLC'
When 'Pt Macquarie' Then 'Port Macquarie'
--When 'Scone' Then 'Scone CLC'
--When 'Singleton' Then 'Singleton CLC'
--When 'YAMBA' Then 'Yamba CLC'
When 'YAMBA' Then 'Yamba'
When 'Nat' Then 'National Environment Centre - Thurgoona'
When 'Prim Ind Cent' Then 'Primary Industries Centre'
When 'RI Learn Onli' Then 'Riverina Institute Learn Online'
When 'Wollongong W' Then 'Wollongong West'
When 'Design Centre' Then 'Design Centre Enmore'
When 'Eora Centre' Then 'Eora'
When 'Eora Centre' Then 'Northern Beaches'
When 'St. Leonards' Then 'St Leonards'
When 'Nci Open Camp' Then 'NCI Open'
When 'Coonabarabran' Then 'Coonabarabaran'
When 'Lightning Rid' Then 'Lightning Ridge'
When 'Western Conne' Then 'Western Connect'
When 'Wit Access Un' Then 'WIT Access Unit'
When 'Blue Mountain' Then 'Blue Mountains'
When 'Macquarie Fie' Then 'Macquarie Fields'
When 'Mount Druitt' Then 'Mt Druitt'
When 'WSI Business' Then 'Western Sydney Business'
When 'Wetherill Pk' Then 'Wetherill Park'
When 'Lake Cargelli' Then 'Lake Cargelligo'
Else [LocationName] end as [LocationName]


, [LocationDescription], [LocationSuburb], [LocationPostCode], [AddressLine1], [AddressLine2], [Suburb], [AusState], [PostCode], [Active], [InstituteID], [InstituteCode], [InstituteName], [InstituteDescription], [RTOCode], [InstituteAddressLine1], [InstituteAddressLine2], [InstituteSuburb], [InstituteAusState], [InstitutePostcode], [RegionID], [RegionName], [_DLCuratedZoneTimeStamp], [_RecordStart], [_RecordEnd], [_RecordDeleted], [_RecordCurrent]
  FROM [compliance].[AvetmissLocation]