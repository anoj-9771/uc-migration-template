create   view dbo.vw_ADFActivityRun as
select * from dbo.ADFActivityRun where TimeGenerated >= DATEADD(day, -30, getdate())