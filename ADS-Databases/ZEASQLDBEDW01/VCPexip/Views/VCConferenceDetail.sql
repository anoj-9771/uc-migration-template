

CREATE VIEW [VCPexip].[VCConferenceDetail]
AS
SELECT      [resource_uri] AS 'Resource URI',
            CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, [start_time]), DATENAME(TzOffset, SYSDATETIMEOFFSET()))) AS 'Start Time',
            CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, [end_time]), DATENAME(TzOffset, SYSDATETIMEOFFSET()))) AS 'End Time',
		    [duration]/60 AS 'Duration (min)',
            [id] AS ID,
            [instant_message_count] AS 'Instant Message Count',
            [name] AS Name,
            [participant_count] AS 'Participant Count',
            [service_type] AS 'Service Type',
            [tag] AS Tag,
		  Year(CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, [start_time]), DATENAME(TzOffset, SYSDATETIMEOFFSET())))) AS 'Year',
		  Month(CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, [start_time]), DATENAME(TzOffset, SYSDATETIMEOFFSET())))) AS 'Month',
		  Case when [name] like 'Catch all rule%' then 'Catch all rule'
                 when [name] like 'DOE Dialling%' then 'DOE Dialling'
                 when [name] like 'To registered endpoint%' then 'To Registered Endpoints'
                 when [name] like 'To TAFE Skype%' then 'To TAFE Skype'
                 else [name]   end As 'VMR Name',
		  Case when [participant_count] = 0 then '0 Participants'
                 when [participant_count] = 1 then '1 Participant' 
                 when [participant_count] between 2 and 4  then '2-4 Participants'
                 when [participant_count] between 5 and 8  then '5-8 Participants'
			  when [participant_count] between 9 and 12 then '9-12 Participants'
                 else '>12 Participants' end As 'Participant Quantity',
		  Cast(Year([start_time]) As Varchar(4)) + '-' + Cast(FORMAT(Month([start_time]),'00') As Varchar(2))  As 'Start Year Month',
		  CAST(CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, [start_time]), DATENAME(TzOffset, SYSDATETIMEOFFSET()))) AS date) AS 'Start Date',
		  case when [Duration]/60 = 0 then '0 Duration'
                 when [Duration]/60 < 30 then 'Less than 30 min'
			  when [Duration]/60 >= 30 and  [Duration]/60  < 60 then '30 min - 1 hr'
                 when [Duration]/60 >= 60 and  [Duration]/60 < 120 then '1 hr - 2 hrs'
                 when [Duration]/60 >= 120 and  [Duration]/60 < 240 then '2 hrs - 4 hrs'
                 when [Duration]/60 >= 240 then 'Over 4 hrs' end as Durations
     FROM [edw].[VCPexip_conference];