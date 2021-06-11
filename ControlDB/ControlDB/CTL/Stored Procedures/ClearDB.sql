CREATE   PROC [CTL].[ClearDB]
AS

Delete From [CTL].[ControlTaskCommand]

Delete From [CTL].[TaskExecutionLog]

Delete From [CTL].[BatchExecutionLog]

Delete From [CTL].[ControlWatermark]

Delete From [CTL].[ControlTasks]
			
Delete From [CTL].[ControlSource]
			
Delete From [CTL].[ControlTarget]

DELETE FROM CTL.ControlManifest

DBCC CHECKIDENT ('CTL.ControlTarget',Reseed,0)
DBCC CHECKIDENT ('CTL.ControlSource',Reseed,0)
DBCC CHECKIDENT ('CTL.ControlTasks',Reseed,0)
DBCC CHECKIDENT ('CTL.BatchExecutionLog',Reseed,0)
DBCC CHECKIDENT ('CTL.TaskExecutionLog',Reseed,0)
DBCC CHECKIDENT ('CTL.ControlTaskCommand',Reseed,0)
DBCC CHECKIDENT ('CTL.ControlWatermark',Reseed,0)