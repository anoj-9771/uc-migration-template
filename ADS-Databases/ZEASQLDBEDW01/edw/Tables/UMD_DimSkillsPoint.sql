CREATE TABLE [edw].[UMD_DimSkillsPoint] (
    [Skillpoint]   VARCHAR (80) NULL,
    [AsAtDate]     DATE         NULL,
    [Year]         INT          NULL,
    [Count]        INT          NULL,
    [InsertedDate] DATETIME     CONSTRAINT [DF_DimSkillsPoint_InsertedDate] DEFAULT (getdate()) NULL
);

