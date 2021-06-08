-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/01/2019
-- Description: Basic function to convert CRON like expression to datetime
-- =============================================

CREATE function [CTL].[udf_CronToDatetime](@CRONExp varchar(50))
returns datetime2
as 
begin
	return (
		select cast(concat(Year,'/',Month,'/',DayOfMonth,' ',Hours,':',Minutes,':',Seconds) as datetime2)
		from (
			select 
				--Account for * and numeric seconds
				case when isNumeric(seconds) = 1
					then RIGHT('0'+ISNULL(Seconds,''),2)
					when isNumeric(Seconds) = 0 and Seconds = '*'
					then format([CTL].[udf_getWAustDateTime](getdate ()), 'ss')
					--Add for /
				end Seconds,
				case when isNumeric(Minutes) = 1
					then RIGHT('0'+ISNULL(Minutes,''),2)
					when isNumeric(Minutes) = 0 and Minutes = '*'
					then format([CTL].[udf_getWAustDateTime](getdate ()), 'mm')
					--Add for - /
				end Minutes,
				case when isNumeric(Hours) = 1
					then RIGHT('0'+ISNULL(Hours,''),2)
					when isNumeric(Hours) = 0 and Hours = '*'
					then format([CTL].[udf_getWAustDateTime](getdate ()), 'HH')
					--Add for - /
				end Hours,
				case when isNumeric(DayOfMonth) = 1
					then RIGHT('0'+ISNULL(DayOfMonth,''),2)
					when isNumeric(DayOfMonth) = 0 and DayOfMonth = '*'
					then datepart(dd,[CTL].[udf_getWAustDateTime](getdate ()))
					--Add for , - ? / L W
				end DayOfMonth,
				case when isNumeric(Month) = 1
					then RIGHT('0'+ISNULL(Month,''),2)
					when isNumeric(Month) = 0 and Month = '*'
					then datepart(mm,[CTL].[udf_getWAustDateTime](getdate ()))
					--Add for , - /
				end Month,
				case when isNumeric(DayOfWeek) = 1
					then RIGHT('0'+ISNULL(DayOfWeek,''),2)
					when isNumeric(DayOfWeek) = 0 and DayOfWeek = '*'
					then datepart(dw,[CTL].[udf_getWAustDateTime](getdate ()))
					--Add for , - /
				end DayOfWeek,
				case when isNumeric(Year) = 1
					then RIGHT('0'+ISNULL(Year,''),2)
					when isNumeric(Year) = 0 and (Year = '*' or Year is null) 
					then datepart(yyyy,[CTL].[udf_getWAustDateTime](getdate ()))
					--Add for , - /
				end Year
			from (
				select [1] Seconds , [2] Minutes , [3] Hours , [4] DayOfMonth, [5] Month, [6] DayOfWeek, [7] Year
				from (
						SELECT ItemNumber, Item
						FROM ctl.udf_DelimitedSplit8K(@CronExp, ' ') 
					)a
					PIVOT
				(
					MAX(Item)
					FOR ItemNumber IN
						([1], [2], [3], [4], [5], [6], [7])
				) piv
			)a
		)b
	)
end