CREATE FUNCTION [CTL].[udf_GetDateLocalTZ]()
returns DATETIME
as
begin
     DECLARE @DT AS datetimeoffset

     SET @DT = CONVERT(datetimeoffset, GETDATE()) AT TIME ZONE 'AUS Eastern Standard Time'

     RETURN CONVERT(datetime, @DT);

end