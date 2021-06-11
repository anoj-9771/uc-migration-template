CREATE TABLE [CTL].[ControlProjectSchedule] (
    [ControlTaskScheduleId] BIGINT        IDENTITY (1, 1) NOT NULL,
    [ControlProjectId]      BIGINT        NOT NULL,
    [CronExpression]        VARCHAR (250) NULL,
    [ControlStageID]        INT           NULL,
    [TriggerName]           VARCHAR (100) NULL,
    [StageEnabled]          BIT           NULL,
    CONSTRAINT [PK_ControlProjectSchedule] PRIMARY KEY CLUSTERED ([ControlTaskScheduleId] ASC)
);






GO

CREATE UNIQUE INDEX [IX_ControlProjectSchedule_Unique] ON [CTL].[ControlProjectSchedule] ([ControlProjectId], [ControlStageID], [TriggerName])
