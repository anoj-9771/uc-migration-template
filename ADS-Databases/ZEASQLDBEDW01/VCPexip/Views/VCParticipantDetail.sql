

CREATE VIEW [VCPexip].[VCParticipantDetail]
AS
SELECT      p.[id] as 'VC Participant Detail ID',
			c.[id] as 'VC Conference Detail ID',
            [Conference],
            [conference_name] AS 'Conference Name',
            [Bandwidth],
            [call_direction] AS 'Call Direction',
            UPPER(LEFT(Substring([call_quality],3,LEN([call_quality])),1))+SUBSTRING(Substring([call_quality],3,LEN([call_quality])),2,LEN(Substring([call_quality],3,LEN([call_quality])))) AS 'Call Quality',
            [call_uuid] AS 'Call Uu ID',
		  case when [disconnect_reason] like 'Transaction failed INVITE%' then 'Transaction failed: Invite' 
		  when [disconnect_reason] like 'Transaction failed MESSAGE%' then 'Transaction failed: Message' 
		  when [disconnect_reason] like 'Transaction failed UPDATE%' then 'Transaction failed: Update' 
		  else [disconnect_reason] end As 'Disconnect Reason',
            [display_name] AS 'Display Name',
            p.[duration]/60 AS 'Duration (min)',
            [Encryption] AS Encryption,
            CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, p.[start_time]), DATENAME(TzOffset, SYSDATETIMEOFFSET()))) AS 'Start Time',
            CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, p.[end_time]), DATENAME(TzOffset, SYSDATETIMEOFFSET()))) AS 'End Time',
            [has_media] AS 'Has Media',
            [is_streaming] AS 'Is Streaming',
            [license_count] AS 'License Count',
		  case when [license_type] = 'port' then 'video' else [license_type] end as [License Type],
            [local_alias] AS 'Local Alias',
            [media_node] AS 'Media Node',
            [presentation_id] AS 'Presentation ID',
            [Protocol] AS Protocol,
            [proxy_node] AS 'Proxy Node',
            [remote_address] 'Remote Address',
            [remote_alias] AS 'Remote Alias',
            [remote_port] AS 'Remote Port',
            p.[resource_uri] AS 'Resource URI',
            [Role] AS Role,
            [service_tag] AS 'Service Tag',
            p.[service_type] AS 'Service Type',
            [signalling_node] AS 'Signalling Node',
            [Vendor],
		  Cast(Year(p.[start_time]) As Varchar(4)) + '-' + Cast(FORMAT(Month(p.[start_time]),'00') As Varchar(2))  As 'Start Year Month',
		  Case when [conference_name] like 'Catch all rule%' then 'Catch all rule'
                 when [conference_name] like 'DOE Dialling%' then 'DOE Dialling'
                 when [conference_name] like 'To registered endpoint%' then 'To Registered Endpoints'
                 when [conference_name] like 'To TAFE Skype%' then 'To TAFE Skype'
                 else [conference_name]  end As [VMR Conference Name],
		  CAST(CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, p.[start_time]), DATENAME(TzOffset, SYSDATETIMEOFFSET()))) AS date) AS 'Start Date',
		  Year(CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, p.[start_time]), DATENAME(TzOffset, SYSDATETIMEOFFSET())))) AS 'Year',
		  Month(CONVERT(datetime, SWITCHOFFSET(CONVERT(datetimeoffset, p.[start_time]), DATENAME(TzOffset, SYSDATETIMEOFFSET())))) AS 'Month',
		  case when p.[duration]/60 = 0 then '0 Duration'
                 when p.[duration]/60 < 30 then 'Less than 30 min'
			  when p.[duration]/60 >= 30 and  p.[duration]/60  < 60 then '30 min - 1 hr'
                 when p.[duration]/60 >= 60 and  p.[duration]/60 < 120 then '1 hr - 2 hrs'
                 when p.[duration]/60 >= 120 and  p.[duration]/60 < 240 then '2 hrs - 4 hrs'
                 when p.[duration]/60 >= 240 then 'Over 4 hrs' end as Durations
     FROM [edw].[VCPexip_participant] p
	 inner join [edw].[VCPexip_conference] c on p.conference = c.resource_uri;