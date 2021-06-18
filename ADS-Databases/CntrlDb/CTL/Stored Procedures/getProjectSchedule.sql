CREATE PROCEDURE [CTL].[GetProjectSchedule]
As

With times as
(
Select p.ProjectName, ps.*, 
	   [CTL].[fn_getWAustDateTime](getdate()) CurrentTime,
       [CTL].[udf_CronToDatetime](ps.CronExpression) ExecutionTime
  From [CTL].[ControlProjects] p
    Join [CTL].[ControlProjectSchedule] ps
      On p.ProjectId = ps.ControlProjectId
 Where p.Enabled = 1
)

Select t.ControlProjectId, t.ProjectName, format(t.ExecutionTime, 'HH:mm:ss')
  From times t
 Where Case When CurrentTime >= ExecutionTime And CurrentTime <= DATEADD(MI, 10, ExecutionTime)
         Then 1
         Else 0
       End = 1