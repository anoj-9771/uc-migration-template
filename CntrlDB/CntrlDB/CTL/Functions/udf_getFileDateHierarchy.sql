



CREATE   FUNCTION [CTL].[udf_GetFileDateHierarchy] (@Grain varchar(20))
RETURNS varchar(50)
AS
BEGIN

return
(
	select Case @Grain
				WHEN 'Day' then 
					concat(
						'year=',FORMAT([CTL].[udf_GetDateLocalTZ](),N'yyyy'), '/',
						'month=',FORMAT([CTL].[udf_GetDateLocalTZ](),N'MM'), '/',
						'day=',FORMAT([CTL].[udf_GetDateLocalTZ](),N'dd')
					)
				WHEN 'Month' then 
					concat(
						'year=',FORMAT([CTL].[udf_GetDateLocalTZ](),N'yyyy'), '/',
						'month=',FORMAT([CTL].[udf_GetDateLocalTZ](),N'MM'), '/'
					)
				WHEN 'Year' then 
					concat(
						'year=',FORMAT([CTL].[udf_GetDateLocalTZ](),N'yyyy'), '/'
					)
				WHEN 'Hour' then 
					concat(
						'year=',FORMAT([CTL].[udf_GetDateLocalTZ](),N'yyyy'), '/',
						'month=',FORMAT([CTL].[udf_GetDateLocalTZ](),N'MM'), '/',
						'day=',FORMAT([CTL].[udf_GetDateLocalTZ](),N'dd'), '/',
						'hour=',FORMAT([CTL].[udf_GetDateLocalTZ](),N'HH')
					)
				WHEN 'Minute' then 
					concat(
						'year=',FORMAT([CTL].[udf_GetDateLocalTZ](),N'yyyy'), '/',
						'month=',FORMAT([CTL].[udf_GetDateLocalTZ](),N'MM'), '/',
						'day=',FORMAT([CTL].[udf_GetDateLocalTZ](),N'dd'), '/',
						'hour=',FORMAT([CTL].[udf_GetDateLocalTZ](),N'HH'), '/',
						'minute=',FORMAT([CTL].[udf_GetDateLocalTZ](),N'mm')
					)
				WHEN 'Second' then 
					concat(
						'year=',FORMAT([CTL].[udf_GetDateLocalTZ](),N'yyyy'), '/',
						'month=',FORMAT([CTL].[udf_GetDateLocalTZ](),N'MM'), '/',
						'day=',FORMAT([CTL].[udf_GetDateLocalTZ](),N'dd'), '/',
						'hour=',FORMAT([CTL].[udf_GetDateLocalTZ](),N'HH'), '/',
						'minute=',FORMAT([CTL].[udf_GetDateLocalTZ](),N'mm'), '/',
						'second=',FORMAT([CTL].[udf_GetDateLocalTZ](),N'ss')
					)
				ELSE 
					concat(
						'year=',FORMAT([CTL].[udf_GetDateLocalTZ](),N'yyyy'), '/',
						'month=',FORMAT([CTL].[udf_GetDateLocalTZ](),N'MM'), '/',
						'day=',FORMAT([CTL].[udf_GetDateLocalTZ](),N'dd')
					)
		   End
)


END