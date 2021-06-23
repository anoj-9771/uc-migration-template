CREATE TABLE [compliance].[AvetmissUnitOffering] (
    [AvetmissUnitOfferingSK]     BIGINT         NOT NULL,
    [UnitOfferingID]             BIGINT         NOT NULL,
    [UnitOfferingCode]           NVARCHAR (64)  NULL,
    [AttendanceCode]             NVARCHAR (240) NULL,
    [AttendanceModeName]         NVARCHAR (32)  NULL,
    [AttendanceModeDescription]  NVARCHAR (256) NULL,
    [CaloccOccurrenceCode]       NVARCHAR (10)  NULL,
    [MaximumHours]               INT            NULL,
    [OfferingCode]               NVARCHAR (240) NULL,
    [OfferingCostCentre]         NVARCHAR (100) NULL,
    [OfferingCostCentreCode]     NVARCHAR (30)  NULL,
    [OfferingDeliveryLocation]   NVARCHAR (240) NULL,
    [OfferingDeliveryMode]       NVARCHAR (10)  NULL,
    [DeliveryModeName]           NVARCHAR (32)  NULL,
    [DeliveryModeDescription]    NVARCHAR (54)  NULL,
    [AvetmissDeliveryModeID]     NVARCHAR (8)   NULL,
    [OfferingEnrolmentLocation]  NVARCHAR (150) NULL,
    [OfferingFundingSource]      NVARCHAR (10)  NULL,
    [OfferingStatus]             NVARCHAR (20)  NULL,
    [OfferingType]               NVARCHAR (10)  NULL,
    [OfferingStartDate]          DATE           NULL,
    [OfferingEndDate]            DATE           NULL,
    [AvetmissDeliveryModeCode]   VARCHAR (3)    NULL,
    [TeachingSectionCode]        NVARCHAR (30)  NULL,
    [TeachingSectionDescription] NVARCHAR (100) NULL,
    [Faculty]                    NVARCHAR (30)  NULL,
    [FacultyDescription]         NVARCHAR (240) NULL,
    [DeliveryHours]              NUMERIC (7)    NULL,
    [PredominantDeliveryMode]    NVARCHAR (40)  NULL,
    [UnitCode]                   NVARCHAR (20)  NULL,
    [_DLCuratedZoneTimeStamp]    DATETIME2 (7)  NULL,
    [_RecordStart]               DATETIME2 (7)  NULL,
    [_RecordEnd]                 DATETIME2 (7)  NULL,
    [_RecordDeleted]             INT            NULL,
    [_RecordCurrent]             INT            NULL
);





GO

GO