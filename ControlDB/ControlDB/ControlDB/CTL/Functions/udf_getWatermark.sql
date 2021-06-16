-- =============================================
-- Author:		Stephen Lundall
-- Create date: 16/12/2020
-- Description:	Function converts BatchGrain to next date
-- =============================================

CREATE        FUNCTION [CTL].[udf_getWatermark] (@Grain varchar(20), @Watermark datetime2)
RETURNS datetime
AS
BEGIN

return
(
	select Case @Grain
				WHEN 'Day' then dateadd(day,1,		coalesce(try_cast(@Watermark as datetime2),getdate()))
				WHEN 'Month' then dateadd(month,1,	coalesce(try_cast(@Watermark as datetime2),getdate()))
				WHEN 'Year' then dateadd(year,1,	coalesce(try_cast(@Watermark as datetime2),getdate()))
				WHEN 'Hour' then dateadd(hour,1,	coalesce(try_cast(@Watermark as datetime2),getdate()))
				ELSE @Watermark
		   End
)


END