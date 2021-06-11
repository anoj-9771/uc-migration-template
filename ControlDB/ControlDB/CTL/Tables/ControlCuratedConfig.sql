CREATE TABLE [CTL].[ControlCuratedConfig] (
    [SubjectArea]        VARCHAR (255) NOT NULL,
    [Project]            VARCHAR (255) NOT NULL,
    [DependentTableName] VARCHAR (255) NOT NULL,
    [Valid]              BIT           NOT NULL,
    CONSTRAINT [PK_ControlCuratedConfig] PRIMARY KEY CLUSTERED ([SubjectArea] ASC, [Project] ASC, [DependentTableName] ASC)
);







