CREATE TABLE [dbo].[ExtractLoadManifest] (
    [SourceID]                  INT            NOT NULL,
    [SystemCode]                NVARCHAR (50)  NOT NULL,
    [SourceSchema]              NVARCHAR (100) NULL,
    [SourceTableName]           NVARCHAR (100) NULL,
    [SourceQuery]               NVARCHAR (MAX) NULL,
    [SourceFolderPath]          NVARCHAR (200) NULL,
    [SourceFileName]            NVARCHAR (100) NULL,
    [SourceKeyVaultSecret]      NVARCHAR (100) NULL,
    [SourceMetaData]            NVARCHAR (MAX) NULL,
    [SourceHandler]             NVARCHAR (100) NOT NULL,
    [LoadType]                  NVARCHAR (50)  NULL,
    [BusinessKeyColumn]         NVARCHAR (300) NULL,
    [WatermarkColumn]           NVARCHAR (100) NULL,
    [RawHandler]                NVARCHAR (MAX) NULL,
    [RawPath]                   NVARCHAR (MAX) NULL,
    [CleansedHandler]           NVARCHAR (MAX) NULL,
    [CleansedPath]              NVARCHAR (MAX) NULL,
    [DestinationSchema]         NVARCHAR (100) NULL,
    [DestinationTableName]      NVARCHAR (100) NULL,
    [DestinationKeyVaultSecret] NVARCHAR (100) NULL,
    [ExtendedProperties]        NVARCHAR (MAX) NULL,
    [Enabled]                   BIT            NOT NULL,
    [CreatedDTS]                DATETIME       NOT NULL
);
GO

ALTER TABLE [dbo].[ExtractLoadManifest]
    ADD CONSTRAINT [PK_SourceManifest] PRIMARY KEY CLUSTERED ([SourceID] ASC);
GO

