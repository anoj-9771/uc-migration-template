CREATE TABLE [edw].[UMD_DimFunding] (
    [CourseFundingSourceCode]        VARCHAR (20)  NOT NULL,
    [CourseFundingSourceName]        VARCHAR (50)  NULL,
    [CourseFundingSourceDescription] VARCHAR (100) NULL,
    [InsertedDate]                   DATETIME      CONSTRAINT [DF_DimFunding_InsertedDate] DEFAULT (getdate()) NULL,
    CONSTRAINT [PK_CourseFundingSourceCode] PRIMARY KEY CLUSTERED ([CourseFundingSourceCode] ASC)
);

