CREATE TABLE [CTL].[ControlTarget] (
    [TargetId]       BIGINT         IDENTITY (1, 1) NOT NULL,
    [TargetName]     VARCHAR (255)  NOT NULL,
    [TargetTypeId]   INT            NOT NULL,
    [TargetLocation] VARCHAR (1000) NOT NULL,
    [TargetEnabled]  BIT            NULL,
    [Compressed]     BIT            NULL,
    [TargetServer]   VARCHAR (255)  NULL,
    CONSTRAINT [PK_ControlTarget] PRIMARY KEY CLUSTERED ([TargetId] ASC),
    CONSTRAINT [FK_ControlTarget_ControlTypes] FOREIGN KEY ([TargetTypeId]) REFERENCES [CTL].[ControlTypes] ([TypeId])
);





