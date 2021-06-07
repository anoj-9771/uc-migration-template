CREATE TABLE [CTL].[ControlStages] (
    [ControlStageId] BIGINT        IDENTITY (1, 1) NOT NULL,
    [StageSequence]  INT           NOT NULL,
    [StageName]      VARCHAR (100) NULL,
    CONSTRAINT [PK_ControlStages] PRIMARY KEY CLUSTERED ([ControlStageId] ASC)
);








GO
CREATE UNIQUE NONCLUSTERED INDEX [IX_ControlStages_StageSequence]
    ON [CTL].[ControlStages]([StageSequence] ASC);


GO
CREATE UNIQUE NONCLUSTERED INDEX [IX_ControlStages_StageName]
    ON [CTL].[ControlStages]([StageName] ASC);

