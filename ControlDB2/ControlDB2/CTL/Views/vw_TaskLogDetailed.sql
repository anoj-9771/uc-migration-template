


CREATE   view [CTL].[vw_TaskLogDetailed] as
select TaskLogId, BatchId, tl.TaskId, p2.[Name] as Project, P.[Name] as Process, s.[Name] as Stage, [Status],
tl.InitialLogTime as StartTime,
tl.EndLogTime as EndTime,
Datediff(ss,InitialLogTime,EndLogTime) as Duration,
--cast(json_value([Output], '$.copyDuration') as int) CopyDuration,
coalesce(cast(json_value([Output], '$.errors[0].Code') as int),cast(json_value([Output], '$.ErrorCode') as int)) ErrorCode,
cast(json_value([Output], '$.errors[0].Category') as int)  ErrorCategory,
coalesce(json_value([Output], '$.errors[0].Message'),json_value([Output], '$.Message'))  ErrorMessage,
json_value([Output], '$.errors[0].Data.FailureInitiator')  FailureInitiator,
cast(json_value([Output], '$.executionDetails[0].detailedDurations.queuingDuration') as int) QueueingDuration,
cast(json_value([Output], '$.executionDetails[0].detailedDurations.timeToFirstByte') as int) TimeToFirstByte,
cast(json_value([Output], '$.executionDetails[0].detailedDurations.transferDuration')as int) TransferDuration,
cast(json_value([Output], '$.throughput') as float) Throughput,
cast(json_value([Output], '$.dataRead')         as bigint)    DataRead,
cast(json_value([Output], '$.dataWritten')       as bigint)    DataWritten,
cast(json_value([Output], '$.rowsRead')         as bigint)    RowsRead,
cast(json_value([Output], '$.rowsCopied') as bigint)       RowsWritten,
coalesce(cast(json_value([Output], '$.runPageUrl')      as varchar(500)) ,json_query([Output], '$.executionDetails'))              ExecutionDetails
from ctl.TaskLog as tl
left join ctl.Task as t
on tl.TaskId = t.TaskId
left join ctl.process as p
on t.ProcessId = p.ProcessId
left join ctl.project as p2
on p.ProjectId = p2.ProjectId
left join CTL.Stage as S
on t.StageId = s.StageId