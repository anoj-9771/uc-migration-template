CREATE TABLE [dbo].[TransformStatus] (
    [ID]           INT           IDENTITY (1, 1) NOT NULL,
    [BatchID]      VARCHAR (64)  NOT NULL,
    [TransformID]  INT           NOT NULL,
    [SpotCount]    BIGINT        NULL,
    [InsertCount]  BIGINT        NULL,
    [UpdateCount]  BIGINT        NULL,
    [CuratedCount] BIGINT        NULL,
    [Status]       NVARCHAR (50) NULL,
    [StartDTS]     DATETIME      NULL,
    [EndDTS]       DATETIME      NULL,
    [CreatedDTS]   DATETIME      NOT NULL
);
GO

ALTER TABLE [dbo].[TransformStatus]
    ADD CONSTRAINT [PK_TransformStatus_1] PRIMARY KEY CLUSTERED ([ID] ASC);
GO

