-- =============================================
-- Author:		Stephen Lundall
-- Create date: 01/10/2019
-- Description:	Convert BatchId to datetime string
-- =============================================

create   FUNCTION [CTL].[udf_BatchIdtoDatetime](@BatchId bigint)
returns varchar(100)
as
begin
     return cast(convert(date, left(cast(@BatchId as varchar), 8)) as varchar) + 'T' + cast(cast(STUFF(STUFF(right(cast(@BatchId as varchar),6),5,0,':'),3,0,':') AS time) as varchar)
end