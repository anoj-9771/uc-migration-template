
create     FUNCTION [CTL].[udf_secondsToTime](@sec int)
returns varchar(15)
as
begin
     DECLARE @time varchar(15)

     SET @time = CONVERT(varchar, DATEADD(ms, @sec * 1000, 0), 114)

     RETURN @time;

end