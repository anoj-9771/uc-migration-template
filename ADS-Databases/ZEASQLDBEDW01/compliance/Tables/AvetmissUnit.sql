CREATE TABLE [compliance].[AvetmissUnit] (
    [AvetmissUnitSK]                      BIGINT         NULL,
    [UnitId]                              BIGINT         NULL,
    [UnitCode]                            NVARCHAR (50)  NULL,
    [UnitName]                            NVARCHAR (100) NULL,
    [UnitStatus]                          NVARCHAR (20)  NULL,
    [AnimalUseCode]                       NVARCHAR (20)  NULL,
    [AnimalUseName]                       NVARCHAR (100) NULL,
    [OccupationCode]                      NVARCHAR (20)  NULL,
    [OccupationName]                      NVARCHAR (100) NULL,
    [IndustryCode]                        NVARCHAR (20)  NULL,
    [IndustryName]                        NVARCHAR (100) NULL,
    [ResourceAllocationModelCategoryCode] NVARCHAR (20)  NULL,
    [ResourceAllocationModelCategoryName] NVARCHAR (100) NULL,
    [FieldOfEducationId]                  NVARCHAR (20)  NULL,
    [FieldOfEducationName]                NVARCHAR (100) NULL,
    [FieldOfEducationDescription]         NVARCHAR (100) NULL,
    [UnitCategory]                        NVARCHAR (50)  NULL,
    [UnitCategoryName]                    NVARCHAR (100) NULL,
    [TGAUnitName]                         NVARCHAR (100) NULL,
    [Sponsor]                             NVARCHAR (100) NULL,
    [ScopeApproved]                       NVARCHAR (1)   NULL,
    [RecommendedUsage]                    NVARCHAR (20)  NULL,
    [ProgramArea]                         NVARCHAR (100) NULL,
    [NationalCourseCode]                  NVARCHAR (50)  NULL,
    [IsVocational]                        NVARCHAR (50)  NULL,
    [NominalHours]                        INT            NULL,
    [UIType]                              NVARCHAR (1)   NULL,
    [_DLCuratedZoneTimeStamp]             DATETIME2 (7)  NULL,
    [_RecordStart]                        DATETIME2 (7)  NULL,
    [_RecordEnd]                          DATETIME2 (7)  NULL,
    [_RecordDeleted]                      INT            NULL,
    [_RecordCurrent]                      INT            NULL
);




GO
CREATE CLUSTERED COLUMNSTORE INDEX [CCI_Compliance_AvetmissUnit] ON [compliance].[AvetmissUnit]
WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) 
;

