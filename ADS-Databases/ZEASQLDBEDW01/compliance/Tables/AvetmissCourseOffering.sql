CREATE TABLE [compliance].[AvetmissCourseOffering] (
    [AvetmissCourseOfferingSK]        BIGINT         NULL,
    [CourseOfferingID]                INT            NULL,
    [CourseOfferingCode]              NVARCHAR (40)  NULL,
    [CourseCaloccCode]                NVARCHAR (10)  NULL,
    [AttendanceModeCode]              NVARCHAR (240) NULL,
    [AttendanceModeDescription]       NVARCHAR (100) NULL,
    [OfferingEnrolmentLocation]       NVARCHAR (10)  NULL,
    [OfferingCode]                    NVARCHAR (240) NULL,
    [OfferingDeliveryLocation]        NVARCHAR (240) NULL,
    [OfferingDeliveryMode]            NVARCHAR (10)  NULL,
    [OfferingDeliveryModeName]        NVARCHAR (100) NULL,
    [OfferingDeliveryModeDescription] NVARCHAR (240) NULL,
    [AvetmissDeliveryModeID]          INT            NULL,
    [AvetmissDeliveryModeName]        NVARCHAR (100) NULL,
    [OfferingStartDate]               DATE           NULL,
    [OfferingEnddate]                 DATE           NULL,
    [Status]                          NVARCHAR (50)  NULL,
    [OfferingStatusDescription]       NVARCHAR (240) NULL,
    [CourseCode]                      NVARCHAR (20)  NULL,
    [TOLinOfferingcode]               NVARCHAR (50)  NULL,
    [TeachingSectionCode]             NVARCHAR (30)  NULL,
    [TeachingSectionDescription]      NVARCHAR (100) NULL,
    [Faculty]                         NVARCHAR (30)  NULL,
    [FacultyDescription]              NVARCHAR (100) NULL,
    [NominalHours]                    INT            NULL,
    [DeliveryHours]                   INT            NULL,
    [PublishToWebsites]               NVARCHAR (1)   NULL,
    [OnlineApplications]              NVARCHAR (1)   NULL,
    [OnlineRegistration]              NVARCHAR (1)   NULL,
    [OnlineEnrolments]                NVARCHAR (1)   NULL,
    [WebStartDate]                    NVARCHAR(20)           NULL,
    [WebEndDate]                      NVARCHAR(20)           NULL,
    [CampusApplications]              NVARCHAR (1)   NULL,
    [CampusEnrolments]                NVARCHAR (1)   NULL,
    [AnytimeEnrolment]                NVARCHAR (1)   NULL,
    [CreatedDate]                     DATETIME2 (7)  NULL,
    [CourseFundingSourceCode]         NVARCHAR (5)   NULL,
    [CourseFundingSourceDescription]  NVARCHAR (100) NULL,
    [Duration]                        NVARCHAR (100) NULL,
    [OfferingType]                    NVARCHAR (100) NULL,
    [UpdatedDate]                     DATETIME2 (7)  NULL,
    [_DLCuratedZoneTimeStamp]         DATETIME2 (7)  NULL,
    [_RecordStart]                    DATETIME2 (7)  NULL,
    [_RecordEnd]                      DATETIME2 (7)  NULL,
    [_RecordDeleted]                  INT            NULL,
    [_RecordCurrent]                  INT            NULL
);




GO
CREATE NONCLUSTERED INDEX [IX_AvetmissCourseOffering_CourseOfferingID]
    ON [compliance].[AvetmissCourseOffering]([CourseOfferingID] ASC);


GO
CREATE NONCLUSTERED INDEX [IX_AvetmissCourseOffering_CourseCode]
    ON [compliance].[AvetmissCourseOffering]([CourseCode] ASC);


GO
CREATE CLUSTERED COLUMNSTORE INDEX [CCI_Compliance_AvetmissCourseOffering] ON [compliance].[AvetmissCourseOffering]
WITH (DROP_EXISTING = OFF, COMPRESSION_DELAY = 0) 
;

