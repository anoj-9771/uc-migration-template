CREATE TABLE [compliance].[AvetmissFeeRecord] (
    [FeeRecordCode]    BIGINT   NULL,
    [FirstFeeRecord]   BIGINT   NULL,
    [FinalFeeRecord]   BIGINT   NULL,
    [_CreatedDateTime] DATETIME DEFAULT (CONVERT([datetime],(CONVERT([datetimeoffset],getdate()) AT TIME ZONE 'AUS Eastern Standard Time'))) NULL
);



