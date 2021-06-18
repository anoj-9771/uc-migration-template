CREATE TABLE [compliance].[AvetmissStudentStatus] (
    [AvetmissStudentStatusSK] BIGINT        NOT NULL,
    [PersonCode]              BIGINT        NULL,
    [ReportingYear]           INT           NULL,
    [StudentStatusCode]       NVARCHAR (50) NULL,
    [_DLCuratedZoneTimeStamp] DATETIME2 (7) NULL,
    [_RecordStart]            DATETIME2 (7) NULL,
    [_RecordEnd]              DATETIME2 (7) NULL,
    [_RecordDeleted]          INT           NULL,
    [_RecordCurrent]          INT           NULL
);




GO
CREATE CLUSTERED COLUMNSTORE INDEX [CCI_Compliance_AvetmissStudentStatus] ON [compliance].[AvetmissStudentStatus]
WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) 
;

