CREATE FUNCTION [CTL].[udf_GetEAustDateTime](@d as datetime)
returns DATETIME
as
begin
     DECLARE @DT AS datetimeoffset

     SET @DT = CONVERT(datetimeoffset, @d) AT TIME ZONE 'E. Australia Standard Time'

     RETURN CONVERT(datetime, @DT);

end