CREATE TABLE [CTL].[ControlTypes] (
    [TypeId]      INT           IDENTITY (1, 1) NOT NULL,
    [ControlType] VARCHAR (255) NULL,
    CONSTRAINT [PK_ControlTypes] PRIMARY KEY CLUSTERED ([TypeId] ASC)
);






GO
CREATE UNIQUE NONCLUSTERED INDEX [IX_ControlTypes_ControlType]
    ON [CTL].[ControlTypes]([ControlType] ASC);

