-- =============================================
-- Author:      Stephen Lundall
-- Create Date: 31/01/2019
-- Description: Basic function to convert CRON like expression to datetime folder hierarchy
-- update commented out Day of Week
-- =============================================

CREATE function [CTL].[udf_CronToFrequency](@CRONExp varchar(50))
returns varchar(50)
as 
begin
	return (
		select RunFrequency
		from (
			select 
				--Hierarchy for yyyy/
				case when seconds <> '*' 
					and Minutes <> '*' 
					and Hours <> '*' 
					and DayOfMonth <> '*' 
					and Month <> '*' 
					and isnumeric(Year) = 1
					then format([CTL].[udf_CronToDatetime](@CRONExp), '/yyyy/')
				--Hierarchy for yyyy/MM/
				when seconds <> '*'
					and Minutes <> '*'
					and Hours <> '*'
					and DayOfMonth <> '*'
					and isnumeric(Month) = 1
					and year = '*'
					then format([CTL].[udf_CronToDatetime](@CRONExp), '/yyyy/MM/')
				--Hierarchy for yyyy/MM/dd/
				when seconds <> '*'
					and Minutes <> '*'
					and Hours <> '*'
					and isnumeric(DayOfMonth) = 1
					and Month = '*'
					then format([CTL].[udf_CronToDatetime](@CRONExp), '/yyyy/MM/dd/')
				--Hierarchy for yyyy/MM/dd/HH
				when seconds <> '*'
					and Minutes <> '*'
					and isnumeric(Hours) = 1
					and DayOfMonth = '*'
					and Month = '*'
					then format([CTL].[udf_CronToDatetime](@CRONExp), '/yyyy/MM/dd/HH/')
				--Hierarchy for yyyy/MM/dd/HH/mm
				when seconds <> '*'
					and isnumeric(Minutes) = 1
					and Hours = '*'
					and DayOfMonth = '*'
					and Month = '*'
					then format([CTL].[udf_CronToDatetime](@CRONExp), '/yyyy/MM/dd/HH/mm/') 
				--Hierarchy for yyyy/MM/dd/HH/mm/ss
				when isnumeric(seconds) = 1
					and Minutes = '*'
					and Hours = '*'
					and DayOfMonth = '*'
					and Month = '*'
					then format([CTL].[udf_CronToDatetime](@CRONExp), '/yyyy/MM/dd/HH/mm/ss/') 
				end RunFrequency
			from (
				select [1] Seconds , [2] Minutes , [3] Hours , [4] DayOfMonth, [5] Month, [6] /*DayOfWeek, [7] */Year
				from (
						SELECT ItemNumber, Item
						FROM ctl.udf_DelimitedSplit8K(@CronExp, ' ') 
					)a
					PIVOT
				(
					MAX(Item)
					FOR ItemNumber IN
						([1], [2], [3], [4], [5], [6]/*, [7]*/)
				) piv
			)a
		)b
	)
end