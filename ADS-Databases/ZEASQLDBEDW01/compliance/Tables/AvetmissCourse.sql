CREATE TABLE [compliance].[AvetmissCourse] (
    [AvetmissCourseSK]                    BIGINT         NULL,
    [CourseId]                            BIGINT         NULL,
    [CourseCode]                          NVARCHAR (50)  NULL,
    [CourseName]                          NVARCHAR (100) NULL,
    [CourseStatus]                        NVARCHAR (20)  NULL,
    [AnimalUseCode]                       NVARCHAR (20)  NULL,
    [AnimalUseName]                       NVARCHAR (100) NULL,
    [OccupationCode]                      NVARCHAR (20)  NULL,
    [OccupationName]                      NVARCHAR (100) NULL,
    [IndustryCode]                        NVARCHAR (20)  NULL,
    [IndustryName]                        NVARCHAR (100) NULL,
    [QualificationTypeCode]               NVARCHAR (20)  NULL,
    [QualificationTypeName]               NVARCHAR (100) NULL,
    [FieldOfEducationId]                  NVARCHAR (20)  NULL,
    [FieldOfEducationName]                NVARCHAR (100) NULL,
    [FieldOfEducationDescription]         NVARCHAR (100) NULL,
    [IsFEEHelp]                           NVARCHAR (50)  NULL,
    [IsVETFEEHelp]                        NVARCHAR (50)  NULL,
    [IsVocational]                        NVARCHAR (50)  NULL,
    [NationalCourseCode]                  NVARCHAR (50)  NULL,
    [NominalHours]                        INT            NULL,
    [ResourceAllocationModelCategoryCode] NVARCHAR (20)  NULL,
    [ResourceAllocationModelCategoryName] NVARCHAR (100) NULL,
    [RecommendedUsage]                    NVARCHAR (20)  NULL,
    [ScopeApproved]                       NVARCHAR (1)   NULL,
    [CourseCategory]                      NVARCHAR (50)  NULL,
    [CourseCategoryName]                  NVARCHAR (100) NULL,
    [CourseCodeConverted]                 NVARCHAR (50)  NULL,
    [TrainingPackageCode]                 NVARCHAR (50)  NULL,
    [TrainingPackageName]                 NVARCHAR (100) NULL,
    [TrainingPackage3Digit]               NVARCHAR (3)   NULL,
    [TrainingPackage4Digit]               NVARCHAR (4)   NULL,
    [TrainingPackage6Digit]               NVARCHAR (6)   NULL,
    [UIType]                              NVARCHAR (1)   NULL,
    [AvetmissCourseCode]                  NVARCHAR (50)  NULL,
    [_DLCuratedZoneTimeStamp]             DATETIME2 (7)  NULL,
    [_RecordStart]                        DATETIME2 (7)  NULL,
    [_RecordEnd]                          DATETIME2 (7)  NULL,
    [_RecordDeleted]                      INT            NULL,
    [_RecordCurrent]                      INT            NULL
);




GO
CREATE NONCLUSTERED INDEX [IX_Compliance_AvetmissCourse_AvetmissCourseCode]
    ON [compliance].[AvetmissCourse]([AvetmissCourseCode] ASC);


GO
CREATE CLUSTERED COLUMNSTORE INDEX [CCI_Compliance_AvetmissCourse] ON [compliance].[AvetmissCourse]
WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) 
;

