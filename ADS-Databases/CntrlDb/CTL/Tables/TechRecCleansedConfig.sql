SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [CTL].[TechRecCleansedConfig](
	[TechRecConfigId] [bigint] IDENTITY(1,1) NOT NULL,
	[TargetObject] [varchar](255) NOT NULL,
	[WhereClause] [varchar](2000) NOT NULL,
	[TechRecDashboardReady] [varchar](1) NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [CTL].[TechRecCleansedConfig] ADD  CONSTRAINT [PK_TechRecCleansedConfig] PRIMARY KEY CLUSTERED 
(
	[TechRecConfigId] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
ALTER TABLE [CTL].[TechRecCleansedConfig] ADD  CONSTRAINT [UK_TechRecCleansedConfig] UNIQUE NONCLUSTERED 
(
	[TechRecConfigId] ASC,
	[TargetObject] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO