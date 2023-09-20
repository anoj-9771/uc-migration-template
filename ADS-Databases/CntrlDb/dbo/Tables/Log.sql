CREATE TABLE [dbo].[Log] (
    [ID]                  INT            IDENTITY (1, 1) NOT NULL,
    [ExtractLoadStatusID] INT            NULL,
    [TransformStatusID]   INT            NULL,
    [ActivityType]        NVARCHAR (100) NULL,
    [Message]             NVARCHAR (MAX) NOT NULL,
    [CreatedDTS]          DATETIME       NOT NULL
);
GO

ALTER TABLE [dbo].[Log]
    ADD CONSTRAINT [PK_Log] PRIMARY KEY CLUSTERED ([ID] ASC);
GO

