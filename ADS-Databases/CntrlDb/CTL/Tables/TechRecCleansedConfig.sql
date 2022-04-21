CREATE TABLE [CTL].[TechRecCleansedConfig](
	[TechRecConfigId] [bigint] IDENTITY(1,1) NOT NULL,
	[TargetObject] [varchar](255) NOT NULL,
	[WhereClause] [varchar](2000) NOT NULL,
	[TechRecDashboardReady] [varchar](1) NOT NULL,
	CONSTRAINT [PK_TechRecCleansedConfig] PRIMARY KEY ([TechRecConfigId]),
	CONSTRAINT [UK_TechRecCleansedConfig] UNIQUE NONCLUSTERED ([TechRecConfigId] ASC, [TargetObject] ASC )
) ON [PRIMARY]
GO
