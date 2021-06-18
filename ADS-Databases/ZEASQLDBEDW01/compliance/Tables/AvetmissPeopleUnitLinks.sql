CREATE TABLE [compliance].[AvetmissPeopleUnitLinks] (
    [UnitEnrolmentId]   BIGINT   NULL,
    [CourseEnrolmentId] BIGINT   NULL,
    [_CreatedDateTime]  DATETIME DEFAULT (CONVERT([datetime],(CONVERT([datetimeoffset],getdate()) AT TIME ZONE 'AUS Eastern Standard Time'))) NULL
);



