CREATE FUNCTION CTL.udf_GetWAustDateTime(@d as datetime)
returns DATETIME
as
begin
     DECLARE @DT AS datetimeoffset

     SET @DT = CONVERT(datetimeoffset, @d) AT TIME ZONE 'W. Australia Standard Time'

     RETURN CONVERT(datetime, @DT);

end