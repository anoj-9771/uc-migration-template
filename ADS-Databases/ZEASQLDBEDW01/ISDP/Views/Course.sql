
CREATE View [ISDP].[Course] 
AS
SELECT [AvetmissCourseSK], [CourseId], [CourseCode], [CourseName], [CourseStatus], [AnimalUseCode], [AnimalUseName], [OccupationCode], [OccupationName], [IndustryCode], [IndustryName], [QualificationTypeCode], [QualificationTypeName], [FieldOfEducationId], [FieldOfEducationName], [FieldOfEducationDescription], [IsFEEHelp], [IsVETFEEHelp], [IsVocational], c.[NationalCourseCode], [NominalHours], [ResourceAllocationModelCategoryCode], [ResourceAllocationModelCategoryName], [RecommendedUsage], [ScopeApproved], [CourseCategory], [CourseCategoryName], [CourseCodeConverted], [TrainingPackageCode], [TrainingPackageName], [TrainingPackage3Digit], [TrainingPackage4Digit], [TrainingPackage6Digit], [UIType], [AvetmissCourseCode],sp.SkillsPointCode ,sp.SkillsPointName, c.[_DLCuratedZoneTimeStamp], c.[_RecordStart], c.[_RecordEnd], c.[_RecordDeleted], c.[_RecordCurrent] 
  FROM [compliance].[AvetmissCourse] c
Left Join reference.CourseSkillsPoint csp on Coalesce(c.NationalCourseCode, c.CourseCode) = csp.NationalCourseCode and csp._RecordCurrent = 1
left Join reference.SkillsPoint sp on csp.SkillsPointID = sp.SkillsPointID and sp._RecordCurrent = 1


   where coursestatus  in ('CURRENT','SUPERSEDED')