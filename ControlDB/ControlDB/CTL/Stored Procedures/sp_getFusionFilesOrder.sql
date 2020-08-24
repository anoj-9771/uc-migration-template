create   proc ctl.sp_getFusionFilesOrder 
@DatasetName nvarchar(250),
@json nvarchar(max)
as
select SourceName, 
	BatchId, 
	ExecTime,
	'OracleFusion/' + @DatasetName + [CTL].[udf_getFileDateHierarchy]('day',ExecTIme) TargetBlobPath 
from ( 
	select SourceName,
	cast(right(left(SourceName, len(SourceName) - 11),8) as bigint) BatchId, 
	[CTL].[udf_BatchIdtoDatetime](cast(right(left(SourceName, len(SourceName) - 11),8) + '000000' as bigint)) ExecTIme 
	from openjson(@json)
	with(
	SourceName nvarchar(max) '$.name'
	)
) a
order by BatchId asc