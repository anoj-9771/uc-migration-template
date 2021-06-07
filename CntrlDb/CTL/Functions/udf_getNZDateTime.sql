CREATE FUNCTION CTL.udf_GetNZDateTime(@d as datetime)
returns DATETIME
as
begin
     DECLARE @DT AS datetimeoffset

     SET @DT = CONVERT(datetimeoffset, @d) AT TIME ZONE 'New Zealand Standard Time'

     RETURN CONVERT(datetime, @DT);

end