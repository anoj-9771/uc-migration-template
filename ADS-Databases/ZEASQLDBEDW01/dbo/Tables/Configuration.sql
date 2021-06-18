CREATE TABLE [dbo].[Configuration] (
    [ConfigurationId]   INT             NOT NULL,
    [ProjectName]       VARCHAR (50)    NULL,
    [ConfigurationName] VARCHAR (50)    NULL,
    [TextValue]         VARCHAR (50)    NULL,
    [NumericValue]      BIGINT          NULL,
    [DecimalValue]      DECIMAL (18, 4) NULL,
    [DateValue]         DATETIME        NULL,
    CONSTRAINT [PK_AvetmissConfiguration] PRIMARY KEY CLUSTERED ([ConfigurationId] ASC)
);

