CREATE FUNCTION [CTL].[udf_CronToFrequency](@CRONExp varchar(50))
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
					then format([CTL].[udf_CronToDatetime](@CRONExp), 'yyyy/') + 
						format([CTL].[udf_CronToDatetime](@CRONExp), 'MM/')
				--Hierarchy for yyyy/MM/
				when seconds <> '*'
					and Minutes <> '*'
					and Hours <> '*'
					and DayOfMonth <> '*'
					and isnumeric(Month) = 1
					then format([CTL].[udf_CronToDatetime](@CRONExp), 'yyyy/') + 
						format([CTL].[udf_CronToDatetime](@CRONExp), 'MM/')
				--Hierarchy for yyyy/MM/dd/
				when seconds <> '*'
					and Minutes <> '*'
					and Hours <> '*'
					and isnumeric(DayOfMonth) = 1
					and Month = '*'
					then format([CTL].[udf_CronToDatetime](@CRONExp), 'yyyy/') + 
						format([CTL].[udf_CronToDatetime](@CRONExp), 'MM/') +
						format([CTL].[udf_CronToDatetime](@CRONExp), 'dd/')
				--Hierarchy for yyyy/MM/dd/HH
				when seconds <> '*'
					and Minutes <> '*'
					and isnumeric(Hours) = 1
					and DayOfMonth = '*'
					and Month = '*'
					then format([CTL].[udf_CronToDatetime](@CRONExp), 'yyyy/') + 
						format([CTL].[udf_CronToDatetime](@CRONExp), 'MM/') +
						format([CTL].[udf_CronToDatetime](@CRONExp), 'dd/') +
						format([CTL].[udf_CronToDatetime](@CRONExp), 'HH/')
				--Hierarchy for yyyy/MM/dd/HH/mm
				when seconds <> '*'
					and isnumeric(Minutes) = 1
					and Hours = '*'
					and DayOfMonth = '*'
					and Month = '*'
					then format([CTL].[udf_CronToDatetime](@CRONExp), 'yyyy/') + 
						format([CTL].[udf_CronToDatetime](@CRONExp), 'MM/') +
						format([CTL].[udf_CronToDatetime](@CRONExp), 'dd/') +
						format([CTL].[udf_CronToDatetime](@CRONExp), 'HH/') +
						format([CTL].[udf_CronToDatetime](@CRONExp), 'mm/') 
				--Hierarchy for yyyy/MM/dd/HH/mm/ss
				when isnumeric(seconds) = 1
					and Minutes = '*'
					and Hours = '*'
					and DayOfMonth = '*'
					and Month = '*'
					then format([CTL].[udf_CronToDatetime](@CRONExp), 'yyyy/') + 
						format([CTL].[udf_CronToDatetime](@CRONExp), 'MM/') +
						format([CTL].[udf_CronToDatetime](@CRONExp), 'dd/') +
						format([CTL].[udf_CronToDatetime](@CRONExp), 'HH/') +
						format([CTL].[udf_CronToDatetime](@CRONExp), 'mm/') +
						format([CTL].[udf_CronToDatetime](@CRONExp), 'ss/') 
				end RunFrequency
			from (
				select [1] Seconds , [2] Minutes , [3] Hours , [4] DayOfMonth, [5] Month, [6] DayOfWeek, [7] Year
				from (
						SELECT ItemNumber, Item
						FROM [CTL].[udf_DelimitedSplit8K](@CRONExp, ' ') 
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